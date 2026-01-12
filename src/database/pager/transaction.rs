use std::collections::HashMap;
use std::sync::atomic::Ordering::Relaxed as R;
use std::sync::{Arc, Weak};

use parking_lot::RwLock;
use rustix::fs::fsync;
use tracing::{debug, error, info, instrument, warn};

use super::metapage::*;
use crate::database::BTree;
use crate::database::btree::Tree;
use crate::database::errors::{Error, PagerError, Result, TXError};
use crate::database::pager::freelist::GC;
use crate::database::pager::metapage::*;
use crate::database::pager::mmap::mmap_extend;
use crate::database::pager::{DiskPager, Pager};
use crate::database::tables::{Record, Value};
use crate::database::transactions::keyrange::{KeyRange, Touched};
use crate::database::transactions::kvdb::KVDB;
use crate::database::transactions::tx::{TX, TXKind};
use crate::database::transactions::txdb::{TXDB, TXWrite};
use crate::database::types::PAGE_SIZE;
use crate::database::{btree::ScanMode, tables::Key, types::Pointer};

pub struct TXHistory {
    pub history: HashMap<u64, Vec<Touched>>,
    pub cap: usize,
}

pub trait Transaction {
    fn begin(&self, db: &Arc<KVDB>, kind: TXKind) -> TX;
    fn abort(&self, tx: TX);
    fn commit(&self, tx: TX) -> Result<()>;
}

impl Transaction for DiskPager {
    fn begin(&self, db: &Arc<KVDB>, kind: TXKind) -> TX {
        let _guard = self.lock.lock();

        let version = self.version.load(R);
        let txdb = Arc::new(TXDB::new(db, kind));
        let weak = Arc::downgrade(&txdb);

        self.ongoing.write().push(version);

        TX {
            db: txdb,
            tree: BTree {
                root_ptr: *self.tree.read(),
                pager: weak,
            },
            version,
            kind,
            rollback: metapage_save(self),
            key_range: KeyRange::new(),
        }
    }

    fn abort(&self, tx: TX) {}

    fn commit(&self, tx: TX) -> Result<()> {
        let _guard = self.lock.lock();
        let mut ongoing = self.ongoing.write();

        // is the TX read only?
        if tx.kind == TXKind::Read {
            ongoing.pop(tx.version);
            return Ok(());
        }

        // did the TX write anything?
        if tx.db.tx_buf.as_ref().unwrap().borrow().write_map.len() == 0 {
            ongoing.pop(tx.version);
            return Ok(());
        }

        // was there a new version published in the meantime?
        if self.version.load(R) == tx.version {
            drop(ongoing);
            return self.commit_start(tx);
        }

        // do the writes of the conflicting TX collide with our writes?
        if self.check_conflict(&tx) {
            ongoing.pop(tx.version);
            Err(TXError::CommitError("Write conflict detected".to_string()).into())
        } else {
            // TODO: retry on new version
            drop(ongoing);
            self.commit_start(tx)
        }
    }
}

impl DiskPager {
    fn commit_start(&self, tx: TX) -> Result<()> {
        debug!("tree operations complete, publishing tx");
        let recov_page = &tx.rollback;

        // making sure the meta page is a known good state after a potential write error
        if self.failed.load(R) {
            debug!("failed update detected, restoring meta page...");

            metapage_write(self, recov_page).expect("meta page recovery write error");
            fsync(&self.database).expect("fsync metapage for restoration failed");
            self.failed.store(false, R);
        };

        // in case the file writing fails, we revert back to the old meta page
        if let Err(e) = self.commit_prog(&tx) {
            warn!(%e, "file update failed! Reverting meta page...");

            // save the pager from before the current operation to be rewritten later
            metapage_load(self, recov_page);

            // discard buffer
            self.buf_shared.write().clear();
            self.buf_fl.write().erase();

            self.failed.store(true, R);

            return Err(e);
        }

        self.ongoing.write().pop(tx.version);
        self.history
            .write()
            .history
            .insert(tx.version, tx.key_range.recorded);

        Ok(())
    }

    /// write sequence
    fn commit_prog(&self, tx: &TX) -> Result<()> {
        // updating free list for next update
        self.freelist.write().set_max_seq();

        // flush buffer to disk
        self.commit_write(&tx)?;
        fsync(&self.database)?;

        // write currently loaded metapage to disk
        metapage_write(self, &metapage_save(self))?;
        fsync(&self.database)?;

        Ok(())
    }

    /// helper function: writePages, flushes the buffer
    fn commit_write(&self, tx: &TX) -> Result<()> {
        debug!("writing page...");

        let mut fl_guard = self.freelist.write();
        let tx_buf = tx.db.tx_buf.as_ref().unwrap().borrow();
        let nwrites = tx_buf.write_map.len();
        let npages = self.npages.load(R);

        assert!(npages != 0);
        assert!(nwrites != 0);

        // extend the mmap if needed
        let new_size = (npages as usize + nwrites) * PAGE_SIZE; // amount of pages in bytes
        mmap_extend(self, new_size).map_err(|e| {
            error!(%e, new_size, "Error when extending mmap");
            e
        })?;

        debug!(
            tx_nappend = tx_buf.nappend,
            tx_nwrites = nwrites,
            "pages to be written:"
        );

        // TX buffer write
        let mut bytes_written: usize = 0;
        let mut count = 0;

        for pair in tx_buf.write_map.iter() {
            debug!(
                "writing TX buffer {:<10} at {:<5}",
                pair.1.get_type(),
                pair.0
            );
            assert!(pair.0.get() != 0); // never write to the meta page

            let offset = pair.0.get() * PAGE_SIZE as u64;
            let io_slice = rustix::io::IoSlice::new(&pair.1[..PAGE_SIZE]);

            bytes_written +=
                rustix::io::pwrite(&self.database, &io_slice, offset).map_err(|e| {
                    error!(?e, "page writing error!");
                    PagerError::WriteFileError(e)
                })?;

            count += 1;
        }

        // adding dealloced pages back to the freelist
        for ptr in tx_buf.dealloc_q.iter() {
            fl_guard.append(*ptr, tx.version)?;
        }

        let mut fl_buf = self.buf_fl.write();

        // freelist write
        for pair in fl_buf.to_dirty_iter() {
            debug!(
                "writing pager buffer {:<10} at {:<5}",
                pair.1.get_type(),
                pair.0
            );
            assert!(pair.0.get() != 0); // never write to the meta page

            let offset = pair.0.get() * PAGE_SIZE as u64;
            let io_slice = rustix::io::IoSlice::new(&pair.1[..PAGE_SIZE]);

            bytes_written +=
                rustix::io::pwrite(&self.database, &io_slice, offset).map_err(|e| {
                    error!(?e, "page writing error!");
                    PagerError::WriteFileError(e)
                })?;

            count += 1;
        }

        debug!(bytes_written, "bytes written:");
        if bytes_written != count * PAGE_SIZE {
            return Err(
                PagerError::PageWriteError("wrong amount of bytes written".to_string()).into(),
            );
        };

        // flip over pager
        *self.tree.write() = tx.tree.get_root();
        self.version.store(tx.version + 1, R);
        fl_guard.set_cur_ver(tx.version);
        self.ongoing.write().pop(tx.version);
        self.npages
            .store(npages + fl_buf.nappend + tx_buf.nappend as u64, R);

        // adjust buffer
        fl_buf.nappend = 0;
        fl_buf.clear();

        Ok(())
    }

    /// checks if a TX write conflicts with history write
    fn check_conflict(&self, tx: &TX) -> bool {
        #[cfg(test)]
        {
            if let Ok("debug") = std::env::var("RUSQL_LOG_TX").as_deref() {
                tx.key_range.print();
            }
        }
        // check the history
        let borrow = self.history.read();
        let hist = borrow.history.get(&tx.version);

        if let Some(touched) = hist
            && let true = Touched::conflict(&tx.key_range.recorded[..], &touched[..])
        {
            warn!("write conflict detected");
            return true;
        }
        debug!("no conflict detected");
        false
    }
}
