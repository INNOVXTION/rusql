use super::keyvalues::*;
use super::records::*;
use super::tables::*;
use std::collections::HashMap;
use std::iter::Scan;
use tracing::instrument;
use tracing::{debug, error, info};

use crate::database::pager::Transaction;
use crate::database::{
    btree::{Compare, ScanMode, SetFlag},
    codec::*,
    errors::{Error, Result, TableError},
    pager::KVEngine,
    types::DataCell,
};
/*
 * |--------------KEY---------------|----Value-----|
 * |                  [Col1][Col2]..|[Col3][Col4]..|
 * |[TABLE ID][PREFIX][PK1 ][PK2 ]..|[ v1 ][ v2 ]..|
*/

pub(crate) struct TableDB<KV: KVEngine + Transaction> {
    pub tdef: TDefTable,
    pub buffer: HashMap<String, Table>, // table name as key
    pub kve: KV,
}

impl<KV> TableDB<KV>
where
    KV: KVEngine + Transaction,
{
    pub fn new(pager: KV) -> Self {
        TableDB {
            tdef: TDefTable::new(),
            buffer: HashMap::new(),
            kve: pager,
        }
    }

    #[instrument(name = "new table id", skip_all)]
    pub fn new_tid(&mut self) -> Result<u32> {
        let key = Query::with_key(&self.get_meta())
            .add(META_TABLE_COL1, META_TABLE_ID_ROW) // we query name column, where pkey = tid
            .encode()?;

        match self.kve.get(key).ok() {
            Some(value) => {
                let meta = self.get_meta();
                let res = value.decode();

                if let DataCell::Int(i) = res[0] {
                    // incrementing the ID
                    // WIP FOR TESTING
                    let (k, v) = Record::new()
                        .add(META_TABLE_ID_ROW)
                        .add(i + 1)
                        .encode(&meta)?
                        .next()
                        .unwrap();

                    self.kve.set(k, v, SetFlag::UPDATE).map_err(|e| {
                        error!(?e);
                        TableError::TableIdError("error when retrieving id".to_string())
                    })?;
                    Ok(i as u32 + 1)
                } else {
                    // types dont match
                    return Err(TableError::TableIdError(
                        "id doesnt match expected int".to_string(),
                    ))?;
                }
            }
            // no id entry yet
            None => {
                let meta = self.get_meta();

                // WIP FOR TESTING
                let (k, v) = Record::new()
                    .add(META_TABLE_ID_ROW)
                    .add(3)
                    .encode(&meta)?
                    .next()
                    .unwrap();

                self.kve.set(k, v, SetFlag::INSERT).map_err(|e| {
                    error!(?e);
                    TableError::TableIdError("error when retrieving id".to_string())
                })?;

                Ok(LOWEST_PREMISSIABLE_TID)
            }
        }
    }

    fn get_meta(&mut self) -> &Table {
        // check buffer
        if self.buffer.contains_key(META_TABLE_NAME) {
            return self.buffer.get(META_TABLE_NAME).unwrap();
        }

        let meta = MetaTable::new().as_table();
        self.buffer.insert(META_TABLE_NAME.to_string(), meta);
        self.buffer.get(META_TABLE_NAME).expect("just inserted it")
    }

    /// overwrites table in buffer
    #[instrument(name = "insert table", skip_all)]
    pub fn insert_table(&mut self, table: &Table) -> Result<()> {
        info!(?table, "inserting table");

        if self.get_table(&table.name).is_some() {
            error!(name = table.name, "table with provided name exists already");
            return Err(TableError::InsertTableError(
                "table with provided name exists already".into(),
            )
            .into());
        }

        // WIP FOR TESTS
        let (k, v) = Record::new()
            .add(table.name.clone())
            .add(table.encode()?)
            .encode(&self.tdef)?
            .next()
            .ok_or(TableError::InsertTableError(
                "record iterator failure".to_string(),
            ))?;

        self.kve.set(k, v, SetFlag::UPSERT).map_err(|e| {
            error!("error when inserting");
            TableError::InsertTableError("error when inserting table".to_string())
        })?;

        // updating buffer
        self.buffer.insert(table.name.clone(), table.clone());
        Ok(())
    }

    /// gets the schema for a table name, schema is stored inside buffer
    #[instrument(name = "get table", skip_all)]
    pub fn get_table(&mut self, name: &str) -> Option<&Table> {
        info!(name, "getting table");

        // check buffer
        if self.buffer.contains_key(name) {
            debug!("returning table from buffer");
            return self.buffer.get(name);
        }

        let key = Query::with_key(&self.tdef)
            .add("name", name)
            .encode()
            .ok()?;

        if let Ok(t) = self.kve.get(key) {
            debug!("returning table from tree");
            self.buffer.insert(name.to_string(), Table::decode(t).ok()?);
            Some(self.buffer.get(&name.to_string())?)
        } else {
            debug!("table not found");
            None
        }
    }

    /// TODO: decrement/free up table id
    ///
    /// drops table from schema table
    #[instrument(name = "drop table", skip_all)]
    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        info!(name, "dropping table");

        let table = if let Some(t) = self.get_table(name) {
            t.clone()
        } else {
            return Err(TableError::DeleteTableError("table doesnt exist".to_string()).into());
        };

        // delete keys
        let recs = self.full_table_scan(&table)?;
        for rec in recs.into_iter() {
            if let Some(kv) = rec.encode(&table)?.next() {
                self.kve.tree_delete(kv.0)?
            } else {
                return Err(TableError::DeleteTableError("iterator error".to_string()).into());
            }
        }

        // delete from tdef
        let qu = Query::with_key(&self.tdef)
            .add(DEF_TABLE_COL1, name)
            .encode()?;

        self.buffer.remove(name);
        self.kve.delete(qu).map_err(|e| {
            error!(%e, "error when dropping table");
            TableError::DeleteTableError("dropping table error when deleting".to_string()).into()
        })
    }

    pub fn get_rec(&self, key: Key) -> Option<Value> {
        info!(%key, "querying");

        self.kve.get(key).ok()
    }

    pub fn scan(&self, mode: ScanMode) -> Result<Vec<Record>> {
        self.kve.scan(mode)
    }

    pub fn full_table_scan(&self, schema: &Table) -> Result<Vec<Record>> {
        // writing a seek key with TID
        let mut buf = [0u8; TID_LEN];
        let _ = &mut buf[..].write_u32(schema.id);
        let seek_key = Key::from_encoded_slice(&buf);
        let seek_mode = ScanMode::new_open(seek_key, Compare::GT)?;

        self.kve.scan(seek_mode)
    }

    /// inserts a record, keeps secondary indices in sync
    #[instrument(name = "insert rec", skip_all)]
    pub fn insert_rec(&mut self, rec: Record, schema: &Table, flag: SetFlag) -> Result<()> {
        info!(?rec, "inserting record");
        let mut iter = rec.encode(schema)?.peekable();
        let mut old_rec;
        let old_pk;
        let mut updated = 0;

        // updating the primary key and retrieving the old one
        let primay_key = iter.next().ok_or(Error::InsertError(
            "record failed to generate a primary key".to_string(),
        ))?;

        match self.kve.tree_set(primay_key.0, primay_key.1, flag) {
            // update found (UPSERT or UPDATE)
            Ok(res) if res.updated => {
                if iter.peek().is_none() {
                    // there are no secondary keys, we are done
                    return Ok(());
                }

                old_pk = res.old.expect("update successful");
                updated += 1;

                // recreating the keys from the update
                old_rec = Record::from_kv(old_pk).encode(schema)?;
                old_rec.next(); // we skip the primary key since we already updated it

                // updating secondary keys
                for (k, v) in iter {
                    if let Some(old_kv) = old_rec.next() {
                        self.kve.tree_delete(old_kv.0)?;
                        let res = self.kve.tree_set(k, v, SetFlag::INSERT)?;

                        if !res.added {
                            return Err(Error::InsertError("couldnt insert record".to_string()));
                        }

                        updated += 1;
                    } else {
                        return Err(Error::InsertError("failed to retrieve old key".to_string()));
                    }
                }
            }
            // INSERT only
            Ok(res) if res.added => {
                for (k, v) in iter {
                    // inserting secondary keys
                    let res = self.kve.tree_set(k, v, SetFlag::INSERT)?;

                    if !res.added {
                        return Err(Error::InsertError("couldnt insert record".to_string()));
                    }

                    updated += 1;
                }
            }
            // Key couldnt be inserted/updated
            Err(e) => {
                return Err(Error::InsertError("couldnt insert record".to_string()));
            }
            _ => unreachable!("we accounted for all cases"),
        }

        debug_assert_eq!(updated, schema.indices.len());
        Ok(())
    }

    fn delete_rec(&mut self, rec: Record, schema: &Table) -> Result<()> {
        let mut iter = rec.encode(schema)?.peekable();
        let mut updated = 0;

        let primay_key = iter.next().ok_or(Error::DeleteError(
            "record failed to generate a primary key".to_string(),
        ))?;

        // deleting primary key
        if let Ok(()) = self.kve.tree_delete(primay_key.0) {
            updated += 1;
        } else {
            return Err(Error::DeleteError("key doesnt exist".to_string()));
        }

        // checking for secondary keys
        if iter.peek().is_none() {
            return Ok(());
        }

        // deleting secondary keys
        for (k, v) in iter {
            self.kve.tree_delete(k)?;
            updated += 1;
        }

        debug_assert_eq!(updated, schema.indices.len());
        Ok(())
    }

    fn create_index(&self, col: &str, table: &Table) {
        // TODO cant delete from tdef or meta
    }

    fn delete_index(&self, col: &str, table: &Table) {}

    fn del_prefix(schema: &Table, prefix: u16) -> Result<ScanMode> {
        let key = Query::with_prefix(schema, prefix);
        let scan = ScanMode::new_open(key, Compare::GT)?;

        Ok(scan)
    }
}
