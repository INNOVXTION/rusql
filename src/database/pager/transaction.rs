use std::collections::HashMap;
use std::process::Termination;
use std::sync::atomic::Ordering;
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
use crate::database::transactions::txdb::TXDB;
use crate::database::types::PAGE_SIZE;
use crate::database::{btree::ScanMode, tables::Key, types::Pointer};
use crate::debug_if_env;

pub struct TXHistory {
    pub history: HashMap<u64, Vec<Touched>>,
    pub cap: usize,
}

pub trait Transaction {
    fn begin(&self, db: &Arc<KVDB>, kind: TXKind) -> TX;
    fn abort(&self, tx: TX);
    fn commit(&self, tx: TX) -> Result<CommitStatus>;
}

impl Transaction for DiskPager {
    fn begin(&self, db: &Arc<KVDB>, kind: TXKind) -> TX {
        let version = self.version.load(Ordering::Acquire);
        let txdb = Arc::new(TXDB::new(db, kind));
        let weak = Arc::downgrade(&txdb);
        let root_ptr = *self.tree.read();

        self.ongoing.write().push(version);

        info!("new TX for version: {version}");

        TX {
            db: txdb,
            tree: BTree {
                root_ptr,
                pager: weak,
            },
            version,
            kind,
            rollback: metapage_save(self),
            key_range: KeyRange::new(),
        }
    }

    fn abort(&self, tx: TX) {}

    fn commit(&self, tx: TX) -> Result<CommitStatus> {
        debug!(tx.version, "committing: ");
        if tx.kind == TXKind::Read {
            self.ongoing.write().pop(tx.version);
            return Ok(CommitStatus::Success);
        }

        // did the TX write anything?
        if tx.db.tx_buf.as_ref().unwrap().borrow().write_map.len() == 0 {
            warn!("no write happened");
            self.ongoing.write().pop(tx.version);
            return Err(TXError::EmptyWriteError.into());
        }

        let _guard = self.lock.lock();

        // was there a new version published in the meantime?
        if self.version.load(Ordering::Acquire) == tx.version {
            return self.commit_start(tx).map(|_| CommitStatus::Success);
        }

        if self.check_conflict(&tx) {
            self.ongoing.write().pop(tx.version);
            Err(TXError::CommitError("write conflict detected".to_string()).into())
        } else {
            // retry on new version
            self.ongoing.write().pop(tx.version);
            Ok(CommitStatus::StaleVersion)
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum CommitStatus {
    Success,
    WriteConflict,
    StaleVersion,
    Failed,
}

pub trait Retry {
    fn can_retry(&self) -> bool;
}

impl Retry for Result<CommitStatus> {
    /// checks if the TX can be retried
    fn can_retry(&self) -> bool {
        match self {
            Ok(s) if *s == CommitStatus::StaleVersion => true,
            _ => false,
        }
    }
}

impl DiskPager {
    fn commit_start(&self, tx: TX) -> Result<()> {
        debug!(
            tx_version = tx.version,
            pager_version = self.version.load(Ordering::Acquire),
            "commit accepted, publishing..."
        );

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

        info!("write successful! new version {}", self.version.load(R));
        Ok(())
    }

    /// write sequence
    fn commit_prog(&self, tx: &TX) -> Result<()> {
        // flush buffer to disk
        self.flush_tx(&tx)?;
        self.flush_fl(&tx)?;
        fsync(&self.database)?;

        // write currently loaded metapage to disk
        debug!("writing mp to disk");
        metapage_write(self, &metapage_save(self))?;
        fsync(&self.database)?;

        // updating free list for next update
        self.freelist.write().set_max_seq();

        Ok(())
    }

    /// helper function: writePages, flushes the buffer
    fn flush_tx(&self, tx: &TX) -> Result<()> {
        debug!("flushing TX buffer");

        let tx_buf = tx.db.tx_buf.as_ref().unwrap().borrow();
        let nwrites = tx_buf.write_map.len();
        let npages = self.npages.load(R);

        let pager_version = self.version.load(Ordering::Acquire);

        assert!(npages != 0);
        assert!(nwrites != 0);
        assert_eq!(tx.version, pager_version);

        // extend the mmap if needed
        let new_size = (1 + npages as usize + nwrites) * PAGE_SIZE; // amount of pages in bytes
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
                pair.1.node.get_type(),
                pair.0
            );
            assert!(pair.0.get() != 0, "we cant write to the metapage");

            let offset = pair.0.get() * PAGE_SIZE as u64;
            let io_slice = rustix::io::IoSlice::new(&pair.1.node[..PAGE_SIZE]);

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

        // flipping over pager data
        *self.tree.write() = tx.tree.get_root();
        self.npages
            .store(npages + tx_buf.nappend as u64, Ordering::Release);

        // updating version
        if tx.version != u64::MAX {
            self.version.store(tx.version + 1, Ordering::Release);
        } else {
            // wrap around to version 1
            // this is a naive implementation, banking on the fact that a conflict is highly unlikely
            // its technically possible for a write TX at version 1 to be in progress at this point
            self.version.store(1, Ordering::Release);
        }

        debug!("write done");
        Ok(())
    }

    fn flush_fl(&self, tx: &TX) -> Result<()> {
        debug!("flushing freelist");

        let mut fl_guard = self.freelist.write();
        let tx_buf = tx.db.tx_buf.as_ref().unwrap().borrow();
        let npages = self.npages.load(R);

        let mut count = 0;
        let mut bytes_written = 0;

        // adding dealloced pages back to the freelist
        for ptr in tx_buf.dealloc_map.iter() {
            assert_ne!(ptr.get(), 0, "we cant add the mp to the freelist");
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
            assert!(pair.0.get() != 0, "we cant write to the metapage");

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

        // flipping over data
        fl_guard.set_cur_ver(tx.version);
        self.npages.store(npages + fl_buf.nappend, R);
        fl_buf.erase();

        // // adjust buffer
        // fl_buf.nappend = 0;
        // fl_buf.clear();

        Ok(())
    }

    /// checks if a TX write conflicts with history write
    fn check_conflict(&self, tx: &TX) -> bool {
        debug_if_env!("RUSQL_LOG_TX", {
            tx.key_range.debug_print();
        });

        assert!(!tx.key_range.recorded.is_empty());

        // check the history
        let borrow = self.history.read();

        if let Some(touched) = borrow.history.get(&tx.version)
            && Touched::conflict(&tx.key_range.recorded[..], &touched[..])
        {
            warn!(
                tx_version = tx.version,
                pager_version = self.version.load(Ordering::Acquire),
                "write conflict detected!"
            );
            return true;
        }
        debug!("no conflict detected");
        false
    }
}
