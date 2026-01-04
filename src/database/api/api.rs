use std::sync::{Arc, atomic::AtomicU64};

use parking_lot::MutexGuard;

use crate::database::{pager::Envoy, tables::TableDB};

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
    db: Arc<TableDB<Envoy>>,
    version: Arc<AtomicU64>,
    write_lock: MutexGuard<'static, ()>,
}

pub struct ReadTx {
    version: u64,
    pager: Arc<TableDB<Envoy>>,
}

// pub struct WriteTx {
//     pager: Arc<TableDB<Envoy>>,
//     version: Arc<AtomicU64>,
//     _guard: MutexGuard<'static, ()>,
//     snapshot: MetaPage,
// }
