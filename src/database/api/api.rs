use std::{
    collections::HashMap,
    rc::Rc,
    sync::{Arc, atomic::AtomicU64},
};

use parking_lot::RwLock;

use crate::database::{
    BTree,
    pager::{BufferEntry, DiskBuffer, DiskPager, MetaPage},
    transactions::kvdb::KVDB,
    types::Pointer,
};

// outward API
trait DatabaseAPI {
    fn create_table(&self);
    fn drop_table(&self);

    fn create_idx(&self);
    fn drop_idx(&self);

    fn insert(&self);
    fn select(&self);
    fn update(&self);
    fn delete(&self);
}

struct Database {
    db: Arc<KVDB>,
}

impl Database {
    fn new(path: &'static str) -> Self {
        Database {
            db: Arc::new(KVDB::new(path)),
        }
    }

    fn select() {
        // TX begin
        // IDX strategy
        // TX commit
    }
}
