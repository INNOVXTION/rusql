use parking_lot::{Mutex, MutexGuard, RawMutex, RwLock};
use std::cell::RefCell;
use std::collections::HashMap;
use std::iter::Scan;
use std::rc::Rc;
use std::sync::Arc;
use tracing::instrument;
use tracing::{debug, error, info};

use crate::database::BTree;
use crate::database::api::tx::{TX, TXKind};
use crate::database::btree::{SetResponse, Tree};
use crate::database::pager::metapage::metapage_save;
use crate::database::pager::transaction::Transaction;
use crate::database::pager::{DiskPager, MetaPage, Pager};
use crate::database::{
    btree::{Compare, ScanMode, SetFlag},
    codec::*,
    errors::{Error, Result, TableError},
    pager::KVEngine,
    tables::{keyvalues::*, records::*, tables::*},
    types::DataCell,
};
/*
 * |--------------KEY---------------|----Value-----|
 * |                  [Col1][Col2]..|[Col3][Col4]..|
 * |[TABLE ID][PREFIX][PK1 ][PK2 ]..|[ v1 ][ v2 ]..|
*/

pub(crate) struct KVDB {
    pub pager: Rc<DiskPager>,
    pub t_def: TDefTable,
    pub t_buf: Mutex<HashMap<String, Rc<Table>>>, // read only buffer, table name as key
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
    fn commit(&self, tx: TX) -> Result<()> {
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

    pub fn get_meta<P: Pager>(&self, tree: &BTree<P>) -> Rc<Table> {
        self.read_table(META_TABLE_NAME, tree)
            .expect("this always returns the table")
    }

    /// gets the schema for a table name, schema is stored inside buffer
    pub fn read_table<P: Pager>(&self, name: &str, tree: &BTree<P>) -> Option<Rc<Table>> {
        let mut buf = self.t_buf.lock();

        // check buffer
        if let Some(t) = buf.get(name) {
            return Some(t.clone());
        }

        if name == META_TABLE_NAME {
            return Some(Rc::new(MetaTable::new().as_table()));
        }

        // retrieve from tree
        let key = Query::with_key(&self.t_def)
            .add(DEF_TABLE_COL1, name)
            .encode()
            .ok()?;

        if let Some(t) = tree.get(key) {
            debug!("returning table from tree");
            buf.insert(name.to_string(), Rc::new(Table::decode(t).ok()?));
            Some(buf.get(&name.to_string())?.clone())
        } else {
            debug!("table not found");
            None
        }
    }

    // /// overwrites table in buffer
    // #[instrument(name = "insert table", skip_all)]
    // pub fn insert_table(&mut self, table: &Table) -> Result<()> {
    //     info!(?table, "inserting table");

    //     if self.get_table(&table.name).is_some() {
    //         error!(name = table.name, "table with provided name exists already");
    //         return Err(TableError::InsertTableError(
    //             "table with provided name exists already".into(),
    //         )
    //         .into());
    //     }

    //     // WIP FOR TESTS
    //     let (k, v) = Record::new()
    //         .add(table.name.clone())
    //         .add(table.encode()?)
    //         .encode(&self.t_def)?
    //         .next()
    //         .ok_or(TableError::InsertTableError(
    //             "record iterator failure".to_string(),
    //         ))?;

    //     self.kve.tree_set(k, v, SetFlag::UPSERT).map_err(|e| {
    //         error!("error when inserting");
    //         TableError::InsertTableError("error when inserting table".to_string())
    //     })?;

    //     // updating buffer
    //     self.t_buf.insert(table.name.clone(), table.clone());
    //     Ok(())
    // }

    // /// gets the schema for a table name, schema is stored inside buffer
    // #[instrument(name = "get table", skip_all)]
    // pub fn get_table(&mut self, name: &str) -> Option<&Table> {
    //     info!(name, "getting table");

    //     // check buffer
    //     if self.t_buf.contains_key(name) {
    //         debug!("returning table from buffer");
    //         return self.t_buf.get(name);
    //     }

    //     let key = Query::with_key(&self.t_def)
    //         .add(DEF_TABLE_COL1, name)
    //         .encode()
    //         .ok()?;

    //     if let Ok(t) = self.kve.get(key) {
    //         debug!("returning table from tree");
    //         self.t_buf.insert(name.to_string(), Table::decode(t).ok()?);
    //         Some(self.t_buf.get(&name.to_string())?)
    //     } else {
    //         debug!("table not found");
    //         None
    //     }
    // }

    // /// TODO: decrement/free up table id
    // ///
    // /// drops table from schema table
    // #[instrument(name = "drop table", skip_all)]
    // pub fn drop_table(&mut self, name: &str) -> Result<()> {
    //     info!(name, "dropping table");

    //     let table = match self.get_table(name) {
    //         Some(t) => t.clone(),
    //         None => {
    //             return Err(TableError::DeleteTableError("table doesnt exist".to_string()).into());
    //         }
    //     };

    //     // delete keys
    //     let recs = self.full_table_scan(&table)?;
    //     for rec in recs.into_iter() {
    //         if let Some(kv) = rec.encode(&table)?.next() {
    //             self.kve.tree_delete(kv.0)?
    //         } else {
    //             return Err(TableError::DeleteTableError("iterator error".to_string()).into());
    //         }
    //     }

    //     // delete from tdef
    //     let qu = Query::with_key(&self.t_def)
    //         .add(DEF_TABLE_COL1, name)
    //         .encode()?;

    //     self.t_buf.remove(name);
    //     self.kve.delete(qu).map_err(|e| {
    //         error!(%e, "error when dropping table");
    //         TableError::DeleteTableError("dropping table error when deleting".to_string()).into()
    //     })
    // }

    // pub fn get_rec(&self, key: Key) -> Option<Value> {
    //     info!(%key, "querying");

    //     self.kve.get(key).ok()
    // }

    // pub fn scan(&self, mode: ScanMode) -> Result<Vec<Record>> {
    //     self.kve.scan(mode)
    // }

    // pub fn full_table_scan(&self, schema: &Table) -> Result<Vec<Record>> {
    //     // writing a seek key with TID
    //     let mut buf = [0u8; TID_LEN];
    //     let _ = &mut buf[..].write_u32(schema.id);
    //     let seek_key = Key::from_encoded_slice(&buf);
    //     let seek_mode = ScanMode::new_open(seek_key, Compare::GT)?;

    //     self.kve.scan(seek_mode)
    // }

    // /// inserts a record, keeps secondary indices in sync
    // #[instrument(name = "insert rec", skip_all)]
    // pub fn insert_rec(&mut self, rec: Record, schema: &Table, flag: SetFlag) -> Result<()> {
    //     info!(?rec, "inserting record");
    //     let mut iter = rec.encode(schema)?.peekable();
    //     let mut old_rec;
    //     let old_pk;
    //     let mut updated = 0;

    //     // updating the primary key and retrieving the old one
    //     let primay_key = iter.next().ok_or(Error::InsertError(
    //         "record failed to generate a primary key".to_string(),
    //     ))?;

    //     let res = self.kve.tree_set(primay_key.0, primay_key.1, flag);
    //     if res.is_ok() {
    //         updated += 1;
    //     }
    //     match res {
    //         // update found (UPSERT or UPDATE)
    //         Ok(res) if res.updated => {
    //             if iter.peek().is_none() {
    //                 // there are no secondary keys, we are done
    //                 return Ok(());
    //             }

    //             old_pk = res.old.expect("update successful");
    //             updated += 1;

    //             // recreating the keys from the update
    //             old_rec = Record::from_kv(old_pk).encode(schema)?;
    //             old_rec.next(); // we skip the primary key since we already updated it

    //             // updating secondary keys
    //             for (k, v) in iter {
    //                 if let Some(old_kv) = old_rec.next() {
    //                     self.kve.tree_delete(old_kv.0)?;
    //                     let res = self.kve.tree_set(k, v, SetFlag::INSERT)?;

    //                     if !res.added {
    //                         return Err(Error::InsertError("couldnt insert record".to_string()));
    //                     }

    //                     updated += 1;
    //                 } else {
    //                     return Err(Error::InsertError("failed to retrieve old key".to_string()));
    //                 }
    //             }
    //         }
    //         // INSERT only
    //         Ok(res) if res.added => {
    //             for (k, v) in iter {
    //                 // inserting secondary keys
    //                 let res = self.kve.tree_set(k, v, SetFlag::INSERT)?;

    //                 if !res.added {
    //                     return Err(Error::InsertError("couldnt insert record".to_string()));
    //                 }

    //                 updated += 1;
    //             }
    //         }
    //         // Key couldnt be inserted/updated
    //         Err(e) => {
    //             return Err(Error::InsertError("couldnt insert record".to_string()));
    //         }
    //         _ => unreachable!("we accounted for all cases"),
    //     }

    //     debug_assert_eq!(updated, schema.indices.len());
    //     Ok(())
    // }

    // fn delete_rec(&mut self, rec: Record, schema: &Table) -> Result<()> {
    //     let mut iter = rec.encode(schema)?.peekable();
    //     let mut updated = 0;

    //     let primay_key = iter.next().ok_or(Error::DeleteError(
    //         "record failed to generate a primary key".to_string(),
    //     ))?;

    //     // deleting primary key
    //     if let Ok(()) = self.kve.tree_delete(primay_key.0) {
    //         updated += 1;
    //     } else {
    //         return Err(Error::DeleteError("key doesnt exist".to_string()));
    //     }

    //     // checking for secondary keys
    //     if iter.peek().is_none() {
    //         return Ok(());
    //     }

    //     // deleting secondary keys
    //     for (k, v) in iter {
    //         self.kve.tree_delete(k)?;
    //         updated += 1;
    //     }

    //     debug_assert_eq!(updated, schema.indices.len());
    //     Ok(())
    // }

    // fn create_index(&self, col: &str, table: &Table) {
    //     // TODO cant delete from tdef or meta
    // }

    // fn delete_index(&self, col: &str, table: &Table) {
    //     // TODO
    // }

    // fn del_prefix(schema: &Table, prefix: u16) -> Result<ScanMode> {
    //     let key = Query::with_prefix(schema, prefix);
    //     let scan = ScanMode::new_open(key, Compare::GT)?;

    //     Ok(scan)
    // }
}

// impl KVEngine for KVDB {
//     #[instrument(name = "pager get", skip_all)]
//     fn get(&self, key: Key) -> Result<Value> {
//         info!("getting...");
//         self.tree_get(key)
//     }

//     #[instrument(name = "pager scan", skip_all)]
//     fn scan(&self, mode: ScanMode) -> Result<Vec<Record>> {
//         info!("scanning...");

//         self.tree_scan(mode)
//     }

//     #[instrument(name = "pager set", skip_all)]
//     fn set(&self, key: Key, val: Value, flag: SetFlag) -> Result<()> {
//         info!("inserting...");

//         let recov_page = metapage_save(&self.pager); // saving current metapage for possible rollback
//         self.tree_set(key, val, flag)?;
//         self.pager.update_or_revert(&recov_page)
//     }

//     #[instrument(name = "pager delete", skip_all)]
//     fn delete(&self, key: Key) -> Result<()> {
//         info!("deleting...");

//         let recov_page = metapage_save(&self.pager); // saving current metapage for possible rollback
//         self.tree_delete(key)?;
//         self.pager.update_or_revert(&recov_page)
//     }
// }
