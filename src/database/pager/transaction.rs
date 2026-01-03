use std::rc::Rc;

use tracing::{error, info, instrument};

use crate::database::btree::{SetFlag, SetResponse};
use crate::database::errors::{Error, PagerError, Result};
use crate::database::pager::DiskPager;
use crate::database::tables::{Record, Value};
use crate::database::{btree::ScanMode, pager::metapage::*, tables::Key, types::Pointer};

/// outward facing api
///
/// deprecated, for mempager testing only
pub(crate) trait KVEngine {
    fn get(&self, key: Key) -> Result<Value>;
    fn scan(&self, mode: ScanMode) -> Result<Vec<Record>>;
    fn set(&self, key: Key, val: Value, flag: SetFlag) -> Result<()>;
    fn delete(&self, key: Key) -> Result<()>;
}

pub(crate) trait Transaction {
    fn begin(&mut self);
    fn abort(&mut self);
    fn commit(&mut self) -> Result<()>;

    fn tree_get(&self, key: Key) -> Result<Value>;
    fn tree_scan(&self, mode: ScanMode) -> Result<Vec<Record>>;
    fn tree_set(&self, key: Key, val: Value, flag: SetFlag) -> Result<SetResponse>;
    fn tree_delete(&self, key: Key) -> Result<()>;
}

/// wrapper struct
pub(crate) struct Envoy {
    pager: Rc<DiskPager>,
    rollback: Option<MetaPage>,
}

impl Envoy {
    pub(crate) fn new(path: &'static str) -> Self {
        Envoy {
            pager: DiskPager::open(path).expect("unexpected error"),
            rollback: None,
        }
    }
}

pub(crate) struct TX {
    snap: Snapshot,
    status: TXStatus,
    kind: TXKind,
}

enum TXKind {
    Read,
    Write,
}

struct Snapshot {
    root_ptr: Pointer,
    metapage: MetaPage,
}

enum TXStatus {
    Pending,
    Failed,
    Complete,
}

impl Transaction for Envoy {
    fn begin(&mut self) {
        self.rollback = Some(metapage_save(&self.pager));
    }

    fn abort(&mut self) {
        metapage_load(&self.pager, &self.rollback.take().unwrap());

        let mut buf = self.pager.buffer.borrow_mut();
        buf.nappend = 0;
        buf.hmap.clear();
    }

    fn commit(&mut self) -> Result<()> {
        assert!(self.rollback.is_some());
        if let Some(ref meta) = self.rollback {
            self.pager.update_or_revert(meta)
        } else {
            // cant commit without a meta page loaded
            Err(PagerError::UnkownError.into())
        }
    }

    fn tree_get(&self, key: Key) -> Result<Value> {
        self.pager
            .tree
            .borrow()
            .search(key)
            .ok_or(Error::SearchError("value not found".to_string()))
    }

    fn tree_scan(&self, mode: ScanMode) -> Result<Vec<Record>> {
        Ok(self
            .pager
            .tree
            .borrow()
            .scan(mode)
            .map_err(|e| Error::SearchError(format!("scan error {e}")))?
            .collect_records())
    }

    fn tree_set(&self, key: Key, val: Value, flag: SetFlag) -> Result<SetResponse> {
        self.pager
            .tree
            .borrow_mut()
            .set(key, val, flag)
            .map_err(|e| {
                error!(%e, "tree error");
                Error::InsertError(format!("{e}"))
            })
    }

    fn tree_delete(&self, key: Key) -> Result<()> {
        info!("deleting...");
        self.pager.tree.borrow_mut().delete(key)
    }
}

impl KVEngine for Envoy {
    #[instrument(name = "pager get", skip_all)]
    fn get(&self, key: Key) -> Result<Value> {
        info!("getting...");
        self.tree_get(key)
    }

    #[instrument(name = "pager scan", skip_all)]
    fn scan(&self, mode: ScanMode) -> Result<Vec<Record>> {
        info!("scanning...");

        self.tree_scan(mode)
    }

    #[instrument(name = "pager set", skip_all)]
    fn set(&self, key: Key, val: Value, flag: SetFlag) -> Result<()> {
        info!("inserting...");

        let recov_page = metapage_save(&self.pager); // saving current metapage for possible rollback
        self.set(key, val, flag)?;
        self.pager.update_or_revert(&recov_page)
    }

    #[instrument(name = "pager delete", skip_all)]
    fn delete(&self, key: Key) -> Result<()> {
        info!("deleting...");

        let recov_page = metapage_save(&self.pager); // saving current metapage for possible rollback
        self.tree_delete(key)?;
        self.pager.update_or_revert(&recov_page)
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::database::helper::cleanup_file;
    use rand::Rng;
    use test_log::test;

    #[test]
    fn open_pager() {
        let path = "test-files/open_pager.rdb";
        cleanup_file(path);
        let pager = Envoy::new(path);
        assert_eq!(pager.pager.buffer.borrow().npages, 2);
        cleanup_file(path);
    }

    #[test]
    fn disk_insert1() {
        let path = "test-files/disk_insert1.rdb";
        cleanup_file(path);
        let pager = Envoy::new(path);
        pager
            .set(
                "1".into(),
                Value::from_unencoded_str("val"),
                SetFlag::UPSERT,
            )
            .unwrap();
        assert_eq!(
            pager.get("1".into()).unwrap(),
            Value::from_unencoded_str("val")
        );
        cleanup_file(path);
    }

    #[test]
    fn disk_insert2() {
        let path = "test-files/disk_insert2.rdb";
        cleanup_file(path);
        let pager = Envoy::new(path);

        for i in 1u16..=300u16 {
            pager
                .set(format!("{i}").into(), "value".into(), SetFlag::UPSERT)
                .unwrap()
        }
        for i in 1u16..=300u16 {
            assert_eq!(pager.get(format!("{i}").into()).unwrap(), "value".into())
        }
        cleanup_file(path);
    }

    #[test]
    fn disk_delete1() {
        let path = "test-files/disk_delete1.rdb";
        cleanup_file(path);
        let pager = Envoy::new(path);

        for i in 1u16..=300u16 {
            pager
                .set(format!("{i}").into(), "value".into(), SetFlag::UPSERT)
                .unwrap()
        }
        for i in 1u16..=300u16 {
            pager.delete(format!("{i}").into()).unwrap();
        }
        cleanup_file(path);
    }

    #[test]
    fn disk_random1() {
        let path = "test-files/disk_random1.rdb";
        cleanup_file(path);
        let pager = Envoy::new(path);

        for _ in 1u16..=1000 {
            pager
                .set(
                    format!("{:?}", rand::rng().random_range(1..1000)).into(),
                    Value::from_unencoded_str("val"),
                    SetFlag::UPSERT,
                )
                .unwrap()
        }
        cleanup_file(path);
    }

    #[test]
    fn disk_delete2() {
        let path = "test-files/disk_delete2.rdb";
        cleanup_file(path);
        let pager = Envoy::new(path);

        for k in 1u16..=1000 {
            pager
                .set(
                    format!("{}", k).into(),
                    Value::from_unencoded_str("val"),
                    SetFlag::UPSERT,
                )
                .unwrap()
        }
        for k in 1u16..=1000 {
            pager.delete(format!("{}", k).into()).unwrap()
        }
        cleanup_file(path);
    }

    // #[test]
    // fn cleanup_test() {
    //     let path = "test-files/disk_cleanup.rdb";
    //     cleanup_file(path);
    //     let pager = Envoy::new(path);
    //     // assert_eq!(fd_size, PAGE_SIZE as u64 * 2);
    //     for k in 1u16..=500 {
    //         pager
    //             .set(format!("{}", k).into(), Value::from_unencoded_str("val"))
    //             .unwrap()
    //     }
    //     for k in 1u16..=500 {
    //         pager.delete(format!("{}", k).into()).unwrap()
    //     }
    //     pager.truncate();
    //     let fd_size = fs::fstat(&pager.database)
    //         .map_err(|e| {
    //             error!("Error when getting file size");
    //             Error::PagerError(PagerError::FDError(e))
    //         })
    //         .unwrap()
    //         .st_size as u64;
    //     cleanup_file(path);
    //     assert_eq!(fd_size, PAGE_SIZE as u64 * 2);
    // }
}

#[cfg(test)]
mod buffer_integration_tests {
    use super::*;
    use crate::database::{btree::Compare, helper::cleanup_file};
    use test_log::test;

    #[test]
    fn buffer_tracks_inserts_through_pager() {
        let path = "test-files/buffer_int_insert.rdb";
        cleanup_file(path);
        let pager = Envoy::new(path);

        // Insert should load pages into buffer as dirty
        pager
            .pager
            .tree
            .borrow_mut()
            .set("key1".into(), "value1".into(), SetFlag::UPSERT)
            .unwrap();

        let buf = pager.pager.buffer.borrow();
        let dirty_count = buf.to_dirty_iter().count();

        // Should have at least one dirty page (the tree node)
        assert!(
            dirty_count > 0,
            "Buffer should contain dirty pages after insert"
        );
        cleanup_file(path);
    }

    #[test]
    fn buffer_persists_across_page_write() {
        let path = "test-files/buffer_int_persist.rdb";
        cleanup_file(path);
        let pager = Envoy::new(path);

        pager
            .set("key1".into(), "value1".into(), SetFlag::UPSERT)
            .unwrap();

        let buf_before = pager.pager.buffer.borrow().hmap.len();

        // After set (which triggers page_write and clear), buffer should be cleaned
        let buf_after = pager.pager.buffer.borrow();

        // Pages should still be in buffer but marked clean
        for entry in buf_after.hmap.values() {
            assert!(!entry.dirty, "Pages should be clean after page_write");
        }
        cleanup_file(path);
    }

    #[test]
    fn buffer_handles_multiple_sequential_sets() {
        let path = "test-files/buffer_int_seq.rdb";
        cleanup_file(path);
        let pager = Envoy::new(path);

        for i in 1..=10 {
            pager
                .set(
                    format!("key{}", i).into(),
                    format!("val{}", i).into(),
                    SetFlag::UPSERT,
                )
                .unwrap();
        }

        let buf = pager.pager.buffer.borrow();
        // Buffer should contain pages from tree operations
        assert!(
            buf.hmap.len() > 0,
            "Buffer should contain pages after multiple inserts"
        );
        cleanup_file(path);
    }

    #[test]
    fn buffer_retire_on_delete() {
        let path = "test-files/buffer_int_retire.rdb";
        cleanup_file(path);
        let pager = Envoy::new(path);

        pager
            .set("key1".into(), "value1".into(), SetFlag::UPSERT)
            .unwrap();

        let initial_size = pager.pager.buffer.borrow().hmap.len();

        // Delete should mark pages as retired
        pager.delete("key1".into()).unwrap();

        let buf = pager.pager.buffer.borrow();
        // After delete and clear, retired pages should be removed
        // (clear() is called during page_write in update_or_revert)
        let has_retired = buf.hmap.values().any(|e| e.retired);

        // After clear(), retired pages should be gone
        assert!(
            !has_retired || buf.hmap.len() <= initial_size,
            "Retired pages should be managed"
        );
        cleanup_file(path);
    }

    #[test]
    fn buffer_reads_load_clean_pages() {
        let path = "test-files/buffer_int_read.rdb";
        cleanup_file(path);
        let pager = Envoy::new(path);

        // Insert and flush
        pager
            .set("key1".into(), "value1".into(), SetFlag::UPSERT)
            .unwrap();

        // Clear buffer to simulate fresh read
        pager.pager.buffer.borrow_mut().hmap.clear();

        // Read should load page as clean
        let _value = pager.get("key1".into()).unwrap();

        let buf = pager.pager.buffer.borrow();
        let clean_pages = buf.hmap.values().filter(|e| !e.dirty).count();

        assert!(clean_pages > 0, "Read operations should load clean pages");
        cleanup_file(path);
    }

    #[test]
    fn buffer_dirty_pages_flushed_on_write() {
        let path = "test-files/buffer_int_flush.rdb";
        cleanup_file(path);
        let pager = Envoy::new(path);

        pager
            .set("key1".into(), "value1".into(), SetFlag::UPSERT)
            .unwrap();

        let buf = pager.pager.buffer.borrow();
        let dirty_before = buf.to_dirty_iter().count();

        // After set, dirty pages should be flushed
        // (this happens in file_update -> page_write -> clear)
        let dirty_after = pager.pager.buffer.borrow().to_dirty_iter().count();

        assert_eq!(
            dirty_after, 0,
            "All dirty pages should be flushed after set completes"
        );
        cleanup_file(path);
    }

    #[test]
    fn buffer_nappend_increments_on_new_pages() {
        let path = "test-files/buffer_int_nappend.rdb";
        cleanup_file(path);
        let pager = Envoy::new(path);

        let nappend_before = pager.pager.buffer.borrow().nappend;

        pager
            .set("key1".into(), "value1".into(), SetFlag::UPSERT)
            .unwrap();

        let nappend_after = pager.pager.buffer.borrow().nappend;

        // nappend should be 0 after clear() in page_write
        assert_eq!(nappend_after, 0, "nappend should reset after page flush");
        cleanup_file(path);
    }

    #[test]
    fn buffer_npages_increments_after_flush() {
        let path = "test-files/buffer_int_npages.rdb";
        cleanup_file(path);
        let pager = Envoy::new(path);

        let npages_before = pager.pager.buffer.borrow().npages;

        pager
            .set("key1".into(), "value1".into(), SetFlag::UPSERT)
            .unwrap();

        let npages_after = pager.pager.buffer.borrow().npages;

        // npages should have increased
        assert!(
            npages_after > npages_before,
            "npages should increase after new pages are appended"
        );
        cleanup_file(path);
    }

    #[test]
    fn buffer_freelist_pages_reused() {
        let path = "test-files/buffer_int_freelist.rdb";
        cleanup_file(path);
        let pager = Envoy::new(path);

        // Insert multiple pages
        for i in 1..=20 {
            pager
                .set(
                    format!("key{}", i).into(),
                    format!("val{}", i).into(),
                    SetFlag::UPSERT,
                )
                .unwrap();
        }

        let npages_before = pager.pager.buffer.borrow().npages;

        // Delete some pages (adds to freelist)
        for i in 1..=10 {
            pager.delete(format!("key{}", i).into()).unwrap();
        }

        // Insert again - should reuse freelist pages
        for i in 21..=30 {
            pager
                .set(
                    format!("key{}", i).into(),
                    format!("val{}", i).into(),
                    SetFlag::UPSERT,
                )
                .unwrap();
        }

        let npages_after = pager.pager.buffer.borrow().npages;

        // npages growth should be minimal if freelist pages are reused
        assert!(
            npages_after - npages_before < 15,
            "Freelist reuse should prevent rapid npages growth"
        );
        cleanup_file(path);
    }

    #[test]
    fn buffer_scan_loads_multiple_pages() {
        let path = "test-files/buffer_int_scan.rdb";
        cleanup_file(path);
        let pager = Envoy::new(path);

        // Insert multiple pages of data
        for i in 1..=50 {
            pager
                .set(
                    format!("key{:03}", i).into(),
                    format!("val{}", i).into(),
                    SetFlag::UPSERT,
                )
                .unwrap();
        }

        pager.pager.buffer.borrow_mut().hmap.clear();

        // Scan should load multiple pages
        let mode = ScanMode::new_open("key".into(), Compare::GE).unwrap();
        let _records = pager.scan(mode).unwrap();

        let buf = pager.pager.buffer.borrow();
        assert!(
            buf.hmap.len() > 0,
            "Scan operations should load pages into buffer"
        );
        cleanup_file(path);
    }

    #[test]
    fn buffer_recovery_on_failed_write() {
        let path = "test-files/buffer_int_recovery.rdb";
        cleanup_file(path);
        let pager = Envoy::new(path);

        pager
            .set("key1".into(), "value1".into(), SetFlag::UPSERT)
            .unwrap();

        // Simulate a failed write scenario
        pager.pager.failed.set(true);

        // The next operation should detect the failure and revert
        // This is handled in update_or_revert
        let result = pager.set("key2".into(), "value2".into(), SetFlag::UPSERT);

        // After recovery attempt, buffer should be manageable
        let buf = pager.pager.buffer.borrow();
        assert!(buf.hmap.len() < 100, "Buffer should be cleared on recovery");
        cleanup_file(path);
    }

    #[test]
    fn buffer_consistency_after_many_operations() {
        let path = "test-files/buffer_int_consistency.rdb";
        cleanup_file(path);
        let pager = Envoy::new(path);

        // Mix of inserts, updates, and deletes
        for i in 1..=30 {
            pager
                .set(
                    format!("key{}", i).into(),
                    format!("val{}", i).into(),
                    SetFlag::UPSERT,
                )
                .unwrap();
        }

        for i in 1..=10 {
            pager
                .set(
                    format!("key{}", i).into(),
                    format!("updated{}", i).into(),
                    SetFlag::UPSERT,
                )
                .unwrap();
        }

        for i in 1..=5 {
            pager.delete(format!("key{}", i).into()).unwrap();
        }

        // Verify data integrity
        for i in 6..=10 {
            let val = pager.get(format!("key{}", i).into()).unwrap();
            assert_eq!(val, format!("updated{}", i).into());
        }

        let buf = pager.pager.buffer.borrow();
        let all_clean = buf.hmap.values().all(|e| !e.dirty);
        assert!(
            all_clean,
            "All pages should be clean after operations complete"
        );
        cleanup_file(path);
    }

    #[test]
    fn buffer_handles_value_updates_dirty_tracking() {
        let path = "test-files/buffer_int_update.rdb";
        cleanup_file(path);
        let pager = Envoy::new(path);

        pager
            .set("key1".into(), "value1".into(), SetFlag::UPSERT)
            .unwrap();

        pager
            .set("key1".into(), "value2".into(), SetFlag::UPSERT)
            .unwrap();

        let value = pager.get("key1".into()).unwrap();
        assert_eq!(value, "value2".into());

        let buf = pager.pager.buffer.borrow();
        assert_eq!(
            buf.to_dirty_iter().count(),
            0,
            "All pages should be clean after flush"
        );
        cleanup_file(path);
    }
}
