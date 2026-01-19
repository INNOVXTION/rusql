use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;
use tracing::instrument;

use crate::database::BTree;
use crate::database::btree::Tree;
use crate::database::pager::transaction::CommitStatus;
use crate::database::pager::transaction::Transaction;
use crate::database::pager::{DiskPager, Pager};
use crate::database::transactions::tx::{TX, TXKind};
use crate::database::{
    errors::{Error, Result},
    tables::{records::*, tables::*},
};
/*
 * |--------------KEY---------------|----Value-----|
 * |                  [Col1][Col2]..|[Col3][Col4]..|
 * |[TABLE ID][PREFIX][PK1 ][PK2 ]..|[ v1 ][ v2 ]..|
*/

// central shared struct
pub(crate) struct KVDB {
    pub pager: Arc<DiskPager>,
    pub t_def: TDefTable,
    pub t_buf: Mutex<HashMap<String, Arc<Table>>>, // read only buffer, table name as key
}

// pass through functions
impl Transaction for KVDB {
    fn begin(&self, db: &Arc<KVDB>, kind: TXKind) -> TX {
        self.pager.begin(db, kind)
    }

    fn abort(&self, tx: TX) {
        self.pager.abort(tx)
    }

    #[instrument(skip_all)]
    fn commit(&self, tx: TX) -> Result<CommitStatus> {
        self.pager.commit(tx)
    }
}

impl KVDB {
    pub fn new(path: &'static str) -> Self {
        KVDB {
            t_def: TDefTable::new(),
            t_buf: Mutex::new(HashMap::new()),
            pager: DiskPager::open(path).expect("DB initialize panic"),
        }
    }

    pub fn get_meta<P: Pager>(&self, tree: &BTree<P>) -> Arc<Table> {
        self.read_table_buffer(META_TABLE_NAME, tree)
            .expect("this always returns the table")
    }

    /// gets the schema for a table name, schema is stored inside buffer
    pub fn read_table_buffer<P: Pager>(&self, name: &str, tree: &BTree<P>) -> Option<Arc<Table>> {
        let mut buf = self.t_buf.lock();

        // check buffer
        if let Some(t) = buf.get(name) {
            return Some(t.clone());
        }

        if name == META_TABLE_NAME {
            return Some(Arc::new(MetaTable::new().as_table()));
        }

        // retrieve from tree
        let key = Query::by_col(&self.t_def)
            .add(DEF_TABLE_COL1, name)
            .encode()
            .ok()?;

        if let Some(t) = tree.get(key) {
            debug!("returning table from tree");
            buf.insert(name.to_string(), Arc::new(Table::decode(t).ok()?));
            Some(buf.get(&name.to_string())?.clone())
        } else {
            debug!("table not found");
            None
        }
    }
}
