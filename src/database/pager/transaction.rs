use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use parking_lot::RwLock;
use rustix::fs::fsync;
use tracing::{debug, error, info, instrument, warn};

use super::metapage::*;
use crate::database::BTree;
use crate::database::api::kvdb::KVDB;
use crate::database::api::tx::{TX, TXKind, Touched};
use crate::database::api::txdb::{TXDB, TXWrite};
use crate::database::btree::Tree;
use crate::database::errors::{Error, PagerError, Result};
use crate::database::pager::metapage::*;
use crate::database::pager::mmap::mmap_extend;
use crate::database::pager::{DiskPager, Pager};
use crate::database::tables::{Record, Value};
use crate::database::types::PAGE_SIZE;
use crate::database::{btree::ScanMode, tables::Key, types::Pointer};

// TODO: LRU cache
pub struct TXHistory {
    pub history: HashMap<u64, Touched>,
    pub limit: usize,
}

pub trait Transaction {
    fn begin(&self, db: &Arc<KVDB>, kind: TXKind) -> TX;
    fn abort(&self, tx: TX);
    fn commit(&self, tx: TX) -> Result<()>;
}

impl Transaction for DiskPager {
    fn begin(&self, db: &Arc<KVDB>, kind: TXKind) -> TX {
        let _guard = self.lock.lock();

        let version = *self.version.borrow();
        let txdb = Rc::new(TXDB::new(db, kind));
        let weak = Rc::downgrade(&txdb);
        self.ongoing.borrow_mut().push(version);

        TX {
            db: txdb,
            tree: BTree {
                root_ptr: self.tree.borrow().get_root(),
                pager: weak,
            },
            version,
            kind,
            rollback: metapage_save(self),
            key_range: None,
        }
    }

    fn abort(&self, tx: TX) {
        let _guard = self.lock.lock();
        metapage_load(&self, &tx.rollback);

        let mut buf = self.buffer.borrow_mut();
        buf.nappend = 0;
        buf.hmap.clear();
    }

    fn commit(&self, tx: TX) -> Result<()> {
        let _guard = self.lock.lock();

        self.commit_start(tx)
    }
}

fn check_conflict(a: &Touched, b: &Touched) -> bool {
    todo!()
}

impl DiskPager {
    fn commit_start(&self, tx: TX) -> Result<()> {
        debug!("tree operations complete, publishing tx");
        let recov_page = &tx.rollback;

        // making sure the meta page is a known good state after a potential write error
        if self.failed.get() {
            debug!("failed update detected, restoring meta page...");

            metapage_write(self, recov_page).expect("meta page recovery write error");
            fsync(&self.database).expect("fsync metapage for restoration failed");
            self.failed.set(false);
        };

        // in case the file writing fails, we revert back to the old meta page
        if let Err(e) = self.commit_prog(&tx) {
            warn!(%e, "file update failed! Reverting meta page...");

            // save the pager from before the current operation to be rewritten later
            metapage_load(self, recov_page);

            // discard buffer
            self.buffer.borrow_mut().hmap.clear();
            self.failed.set(true);

            return Err(e);
        }
        Ok(())
    }

    /// write sequence
    fn commit_prog(&self, tx: &TX) -> Result<()> {
        // updating free list for next update
        self.freelist.borrow_mut().set_max_seq();

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

        let tx_buf = tx.db.tx_buf.as_ref().unwrap().borrow();
        let nwrites = tx_buf.write_map.len();
        let npages = self.buffer.borrow().npages;

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
            let nref = pair.1.borrow();

            debug!("writing TX buffer {:<10} at {:<5}", nref.get_type(), pair.0);
            assert!(pair.0.get() != 0); // never write to the meta page

            let offset = pair.0.get() * PAGE_SIZE as u64;
            let io_slice = rustix::io::IoSlice::new(&nref[..PAGE_SIZE]);
            bytes_written +=
                rustix::io::pwrite(&self.database, &io_slice, offset).map_err(|e| {
                    error!(?e, "page writing error!");
                    PagerError::WriteFileError(e)
                })?;
            count += 1;
        }

        // adding deallocedp ages to the freelist
        for ptr in tx_buf.dealloc_q.iter() {
            self.freelist.borrow_mut().append(*ptr, tx.version)?;
        }

        // freelist write
        for pair in self.buffer.borrow_mut().to_dirty_iter() {
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

        // flip over root and version
        self.tree.borrow_mut().set_root(tx.tree.get_root());
        self.version.replace(tx.version);
        self.freelist.borrow_mut().set_cur_ver(tx.version);
        self.ongoing.borrow_mut().pop(tx.version);

        // adjust buffer
        let mut disk_buf = self.buffer.borrow_mut();

        disk_buf.npages += disk_buf.nappend + tx_buf.nappend as u64;
        disk_buf.nappend = 0;
        disk_buf.clear();

        Ok(())
    }
}
