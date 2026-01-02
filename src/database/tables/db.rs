use super::keyvalues::*;
use super::records::*;
use super::tables::*;
use std::collections::HashMap;
use tracing::instrument;
use tracing::{debug, error, info};

use crate::database::{
    btree::{Compare, ScanMode, SetFlag},
    codec::*,
    errors::{Error, Result, TableError},
    pager::diskpager::KVEngine,
    types::DataCell,
};
/*
 * |--------------KEY---------------|----Value-----|
 * |                  [Col1][Col2]..|[Col3][Col4]..|
 * |[TABLE ID][PREFIX][PK1 ][PK2 ]..|[ v1 ][ v2 ]..|
*/

pub(crate) struct Database<KV: KVEngine> {
    pub tdef: TDefTable,
    pub buffer: HashMap<String, Table>, // table name as key
    pub kve: KV,
}

pub(crate) enum SearchRequest {
    Lookup(Key),
    Range(ScanMode),
    FullTable(ScanMode),
}

pub(crate) struct SetRequest {
    payload: Vec<(Key, Value)>,
    flag: SetFlag,
}

pub(crate) enum DeleteRequest {
    Table(ScanMode),
    Prefix(ScanMode),
    Row(Key),
}

impl DeleteRequest {
    fn table(schema: &Table) -> Result<Self> {
        let key = Query::with_tid(schema);
        let scan = ScanMode::new_open(key, Compare::GT)?;

        Ok(DeleteRequest::Table(scan))
    }

    fn prefix(schema: &Table, prefix: u16) -> Result<Self> {
        let key = Query::with_prefix(schema, prefix);
        let scan = ScanMode::new_open(key, Compare::GT)?;

        Ok(DeleteRequest::Table(scan))
    }

    fn row(schema: &Table, cols: &[&str], mut data: Vec<DataCell>) -> Result<Self> {
        let mut key = Query::with_key(schema);
        for i in 0..data.len() {
            key = key.add(&cols[i], data.remove(i));
        }
        Ok(DeleteRequest::Row(key.encode()?))
    }
}

impl<KV: KVEngine> Database<KV> {
    pub fn new(pager: KV) -> Self {
        Database {
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

                    self.kve.set(k, v, SetFlag::UPSERT).map_err(|e| {
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

                self.kve.set(k, v, SetFlag::UPSERT).map_err(|e| {
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
        self.buffer.get(META_TABLE_NAME).unwrap()
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
    #[instrument(name = "drop table", skip_all)]
    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        info!(name, "dropping table");

        if self.get_table(name).is_none() {
            error!("table doesnt exist");
            return Err(TableError::DeleteTableError(
                "table doesnt exist".to_string(),
            ))?;
        }

        let qu = Query::with_key(&self.tdef)
            .add(DEF_TABLE_COL1, name)
            .encode()?;

        self.buffer.remove(name);
        self.kve.delete(qu).map_err(|e| {
            error!(%e, "error when dropping table");
            TableError::DeleteTableError("dropping table error when deleting".to_string()).into()
        })
    }

    #[instrument(name = "insert rec", skip_all)]
    pub fn insert_rec(&mut self, rec: Record, schema: &Table, flag: SetFlag) -> Result<()> {
        info!(?rec, "inserting record");

        // WIP FOR TESTING
        let (key, value) = rec.encode(schema)?.next().unwrap();

        self.kve.set(key, value, flag)?;
        Ok(())
    }

    pub fn get_rec(&self, key: Key) -> Option<Value> {
        info!(%key, "querying");

        self.kve.get(key).ok()
    }

    pub fn scan(&self, mode: ScanMode) -> Result<Vec<Record>> {
        self.kve.scan(mode)
    }

    pub fn full_table_scan(&self, schema: &Table) -> Result<Vec<Record>> {
        let mut buf = [0u8; TID_LEN];

        // writing a seek key with TID
        let _ = &mut buf[..].write_u32(schema.id);
        let seek_key = Key::from_encoded_slice(&buf);
        let seek_mode = ScanMode::Open(seek_key, Compare::GT);

        self.scan(seek_mode)
    }

    fn creat_index() {}

    fn delete_index() {}
}
