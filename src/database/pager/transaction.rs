use std::rc::Rc;

use crate::database::btree::SetFlag;
use crate::database::errors::{Error, Result};
use crate::database::pager::EnvoyV1;
use crate::database::tables::{Record, Value};
use crate::database::{btree::ScanMode, pager::diskpager::MetaPage, tables::Key, types::Pointer};

/// outward facing api
pub(crate) trait KVEngine {
    fn get(&self, key: Key) -> Result<Value>;
    fn scan(&self, mode: ScanMode) -> Result<Vec<Record>>;
    fn set(&self, key: Key, val: Value, flag: SetFlag) -> Result<()>;
    fn delete(&self, key: Key) -> Result<()>;
}

pub(crate) trait TXEngine {
    fn begin();
    fn abort();
    fn commit();
}

/// wrapper struct
pub(crate) struct Envoy {
    pager: Rc<EnvoyV1>,
    // tx: Vec<Transaction>,
}

impl Envoy {
    pub(crate) fn new(path: &'static str) -> Self {
        Envoy {
            pager: EnvoyV1::open(path).expect("unexpected error"),
        }
    }
}

impl KVEngine for Envoy {
    fn get(&self, key: Key) -> Result<Value> {
        self.pager.get(key)
    }

    fn scan(&self, mode: ScanMode) -> Result<Vec<Record>> {
        self.pager.scan(mode)
    }

    fn set(&self, key: Key, val: Value, flag: SetFlag) -> Result<()> {
        self.pager.set(key, val, flag)
    }

    fn delete(&self, key: Key) -> Result<()> {
        self.pager.delete(key)
    }
}

pub(crate) struct TX {
    snap: Snapshot,
    status: TXStatus,
}

struct Snapshot {
    root_ptr: Pointer,
    metapage: MetaPage,
}

enum TXStatus {
    Pending,
    Failed,
    Aborted,
    Complete,
}
