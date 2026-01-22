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
use crate::database::types::TBUFFER_CAP;
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
    pub t_buf: Mutex<TableBuffer>, // read only buffer, table name as key
}

pub(crate) struct TableBuffer {
    map: HashMap<String, TBEntry>,
    cap: u16,
}

struct TBEntry {
    table: Arc<Table>,
    version: u64,
}

impl TableBuffer {
    fn new() -> Self {
        TableBuffer {
            map: HashMap::with_capacity(TBUFFER_CAP as usize),
            cap: TBUFFER_CAP,
        }
    }

    fn get(&self, tname: &str, version: u64) -> Option<Arc<Table>> {
        if let Some(e) = self.map.get(tname) {
            if e.version == version {
                return Some(e.table.clone());
            }
        }
        None
    }

    fn insert(&mut self, table: Table, version: u64) -> Result<()> {
        if self.map.len() == self.cap as usize {
            self.map.clear();
        }

        self.map.insert(
            table.name.clone(),
            TBEntry {
                table: Arc::new(table),
                version,
            },
        );

        Ok(())
    }

    fn delete(&mut self, tname: &str) -> Result<()> {
        self.map.remove(tname);
        Ok(())
    }
}

// pass through functions
impl Transaction for KVDB {
    fn begin(&self, db: &Arc<KVDB>, kind: TXKind) -> TX {
        self.pager.begin(db, kind)
    }

    fn abort(&self, tx: TX) -> Result<CommitStatus> {
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
            t_buf: Mutex::new(TableBuffer::new()),
            pager: DiskPager::open(path).expect("DB initialize panic"),
        }
    }

    pub fn get_meta<P: Pager>(&self, tree: &BTree<P>) -> Arc<Table> {
        self.read_table_buffer(META_TABLE_NAME, META_TABLE_VERSION, tree)
            .expect("this always returns the table")
    }

    pub fn evict_table(&self, table: &str) -> Result<()> {
        let mut buf = self.t_buf.lock();
        buf.delete(table)?;
        Ok(())
    }

    /// gets the schema for a table name, schema is stored inside buffer
    pub fn read_table_buffer<P: Pager>(
        &self,
        name: &str,
        version: u64,
        tree: &BTree<P>,
    ) -> Option<Arc<Table>> {
        let mut buf = self.t_buf.lock();

        // check buffer
        if let Some(t) = buf.get(name, version) {
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
            buf.insert(Table::decode(t).ok()?, version).ok();
            Some(buf.get(&name.to_string(), version)?.clone())
        } else {
            debug!("table not found");
            None
        }
    }
}
