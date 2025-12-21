use std::{collections::HashMap, marker::PhantomData, ops::Deref};

use crate::database::{
    codec::Codec,
    errors::{Error, Result, TableError},
    pager::diskpager::KVEngine,
    tables::{
        Key,
        records::{DataCell, Query, Record, Value},
    },
};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, instrument};

/*
 * Encoding Layout:
 * |-----------KEY----------|----Value-----|
 * |          [Col1][Col2]..|[Col3][Col4]..|
 * |[TABLE ID][PK1 ][PK2 ]..|[ v1 ][ v2 ]..|
 *
 * Key: 0 Val: Tdef Schema
 *
 * Tdef, id = 1:
 * |-----KEY---|----Val---|
 * |   [ Col1 ]|[  Col2  ]|
 * |[1][ name ]|[  def   ]|
 *
 * Meta, id = 2:
 * |-----KEY---|----Val---|
 * |   [ Col1 ]|[  Col2  ]|
 * |[2][ key  ]|[  val   ]|
 *
 * Data Path:
 * User Input -> DataCell -> Record -> Key, Value -> Tree
 *
 */

const DEF_TABLE_NAME: &'static str = "tdef";
const DEF_TABLE_COL1: &'static str = "name";
const DEF_TABLE_COL2: &'static str = "def";

const DEF_TABLE_ID: u64 = 1;
const DEF_TABLE_PKEYS: u16 = 1;

const META_TABLE_NAME: &'static str = "tmeta";
const META_TABLE_COL1: &'static str = "name";
const META_TABLE_COL2: &'static str = "tid";
const META_TABLE_ID_ROW: &'static str = "tid";

const META_TABLE_ID: u64 = 2;
const META_TABLE_PKEYS: u16 = 1;

/// wrapper for sentinal value
#[derive(Serialize, Deserialize)]
struct MetaTable(Table);

impl MetaTable {
    fn new() -> Self {
        MetaTable(Table {
            name: META_TABLE_NAME.to_string(),
            id: META_TABLE_ID,
            cols: vec![
                Column {
                    title: META_TABLE_COL1.to_string(),
                    data_type: TypeCol::BYTES,
                },
                Column {
                    title: META_TABLE_COL2.to_string(),
                    data_type: TypeCol::INTEGER,
                },
            ],
            pkeys: META_TABLE_PKEYS,
            _priv: PhantomData,
        })
    }

    fn as_table(self) -> Table {
        self.0
    }
}

/// wrapper for sentinal value
#[derive(Serialize, Deserialize)]
struct TDefTable(Table);

impl TDefTable {
    fn new() -> Self {
        TDefTable(Table {
            name: DEF_TABLE_NAME.to_string(),
            id: DEF_TABLE_ID,
            cols: vec![
                Column {
                    title: DEF_TABLE_COL1.to_string(),
                    data_type: TypeCol::BYTES,
                },
                Column {
                    title: DEF_TABLE_COL2.to_string(),
                    data_type: TypeCol::BYTES,
                },
            ],
            pkeys: DEF_TABLE_PKEYS,
            _priv: PhantomData,
        })
    }
}

impl Deref for TDefTable {
    type Target = Table;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub(crate) struct TableBuilder {
    name: Option<String>,
    id: Option<u64>,
    cols: Vec<Column>,
    pkeys: Option<u16>,
}

impl TableBuilder {
    pub fn new() -> Self {
        TableBuilder {
            name: None,
            id: None,
            cols: vec![],
            pkeys: None,
        }
    }

    pub fn name(mut self, name: &str) -> Self {
        self.name = Some(name.to_string());
        self
    }

    /// DEPRECTATED, for testing purposes only
    pub fn id(mut self, id: u64) -> Self {
        self.id = Some(id);
        self
    }

    pub fn add_col(mut self, title: &str, data_type: TypeCol) -> Self {
        self.cols.push(Column {
            title: title.to_string(),
            data_type,
        });
        self
    }

    pub fn pkey(mut self, nkeys: u16) -> Self {
        self.pkeys = Some(nkeys);
        self
    }

    /// parsing is WIP
    pub fn build<KV: KVEngine>(self, pager: &mut Database<KV>) -> Result<Table> {
        let name = match self.name {
            Some(n) => {
                if n.is_empty() {
                    error!("table creation error");
                    Err(TableError::TableBuildError(
                        "must provide a name".to_string(),
                    ))?;
                }
                n
            }
            None => {
                error!("table creation error");
                return Err(TableError::TableBuildError(
                    "must provide a name".to_string(),
                ))?;
            }
        };

        let id = match self.id {
            Some(id) => id,
            None => pager.new_tid()?,
        };

        let cols = self.cols;
        if cols.len() < 1 {
            error!("table creation error");
            return Err(TableError::TableBuildError(
                "must provide at least 2 columns".to_string(),
            ))?;
        }

        let pkeys = match self.pkeys {
            Some(pk) => {
                if pk == 0 {
                    error!("table creation error");
                    return Err(TableError::TableBuildError(
                        "primary key cant be zero".to_string(),
                    ))?;
                }
                if cols.len() < pk as usize {
                    error!("table creation error");
                    return Err(TableError::TableBuildError(
                        "cant have more primary keys than columns".to_string(),
                    ))?;
                }
                pk
            }
            None => {
                error!("table creation error");
                return Err(TableError::TableBuildError(
                    "must designate primary keys".to_string(),
                ))?;
            }
        };

        Ok(Table {
            name,
            id,
            cols,
            pkeys,
            _priv: PhantomData,
        })
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub(crate) struct Table {
    pub(crate) name: String,
    pub(crate) id: u64,
    pub(crate) cols: Vec<Column>,
    pub(crate) pkeys: u16,

    // ensures tables are built through constructor
    _priv: PhantomData<bool>,
}

impl Table {
    /// encodes table to JSON string
    pub fn encode(&self) -> Result<String> {
        serde_json::to_string(&self).map_err(|e| {
            error!(%e, "error when encoding table");
            TableError::EncodeTableError(e).into()
        })
    }

    /// decodes JSON string into table
    pub fn decode(value: Value) -> Result<Self> {
        serde_json::from_str(&value.to_string()?).map_err(|e| {
            error!(%e, "error when decoding table");
            TableError::EncodeTableError(e).into()
        })
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub(crate) struct Column {
    pub title: String,
    pub data_type: TypeCol,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub(crate) enum TypeCol {
    BYTES = 1,
    INTEGER = 2,
}

impl TypeCol {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(TypeCol::BYTES),
            2 => Some(TypeCol::INTEGER),
            _ => None,
        }
    }
}

pub(crate) struct Database<KV: KVEngine> {
    tdef: TDefTable,
    buffer: HashMap<String, Table>, // table name as key
    kve: KV,
}

enum SetFlag {
    INSERT, // only add new rows
    UPDATE, // only modifies existing
    UPSERT, // add or modify
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
    pub fn new_tid(&mut self) -> Result<u64> {
        let key = Query::new()
            .add(META_TABLE_COL1, META_TABLE_ID_ROW) // we query name column, where pkey = tid
            .encode(&self.get_meta())
            .expect("this cant fail");

        match self.kve.get(key).ok() {
            Some(value) => {
                let meta = self.get_meta();
                let res = value.decode();

                if let DataCell::Int(i) = res[0] {
                    // incrementing the ID
                    let (k, v) = Record::new()
                        .add(META_TABLE_ID_ROW)
                        .add(i + 1)
                        .encode(&meta)
                        .expect("this cant fail");

                    self.kve.set(k, v).map_err(|e| {
                        error!(?e);
                        TableError::TableIdError("error when retrieving id".to_string())
                    })?;
                    Ok(i as u64 + 1)
                } else {
                    // error when types dont match
                    return Err(TableError::TableIdError(
                        "id doesnt match expected int".to_string(),
                    ))?;
                }
            }
            // no id entry yet
            None => {
                let meta = self.get_meta();

                let (k, v) = Record::new()
                    .add(META_TABLE_ID_ROW)
                    .add(3)
                    .encode(&meta)
                    .expect("this cant fail"); // tid 1 and 2 are taken

                self.kve.set(k, v).map_err(|e| {
                    error!(?e);
                    TableError::TableIdError("error when retrieving id".to_string())
                })?;
                Ok(3)
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
            ))?;
        }

        let (key, value) = Record::new()
            .add(table.name.clone())
            .add(table.encode()?)
            .encode(&self.tdef)?;

        self.kve.set(key, value).map_err(|e| {
            error!("error when inserting");
            TableError::InsertTableError("error when inserting table".to_string())
        })?;

        // updating buffer
        self.buffer.insert(table.name.clone(), table.clone());
        Ok(())
    }

    #[instrument(name = "get table", skip_all)]
    /// gets the schema for a table name, schema is stored inside buffer
    pub fn get_table(&mut self, name: &str) -> Option<&Table> {
        info!(name, "getting table");
        // check buffer
        if self.buffer.contains_key(name) {
            debug!("returning table from buffer");
            return self.buffer.get(name);
        }
        let key = Query::new().add("name", name).encode(&self.tdef).ok()?;

        if let Ok(t) = self.kve.get(key) {
            debug!("returning table from tree");
            self.buffer.insert(name.to_string(), Table::decode(t).ok()?);
            Some(self.buffer.get(&name.to_string())?)
        } else {
            debug!("table not found");
            None
        }
    }

    #[instrument(name = "drop table", skip_all)]
    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        info!(name, "dropping table");
        if self.get_table(name).is_none() {
            error!("table doesnt exist");
            return Err(TableError::DeleteTableError(
                "table doesnt exist".to_string(),
            ))?;
        }
        let qu = Query::new().add(DEF_TABLE_COL1, name).encode(&self.tdef)?;

        self.kve.delete(qu).map_err(|e| {
            error!(%e, "error when dropping table");
            TableError::DeleteTableError("dropping table error when deleting".to_string()).into()
        })
    }

    #[instrument(name = "insert rec", skip_all)]
    fn insert_rec(&mut self, rec: Record, schema: &Table) -> Result<()> {
        info!(?rec, "inserting record");

        let (key, value) = rec.encode(schema)?;
        let _ = self.kve.set(key, value);
        Ok(())
    }
    fn get_rec(&self, query: Query, schema: &Table) -> Option<Value> {
        info!(?query, "querying");
        self.kve.get(query.encode(schema).ok()?).ok()
    }
}

// outward API
trait DatabaseAPI {
    fn create_table(&self);
    fn drop_table(&self);

    fn get(&self);
    fn insert(&self);
    fn update(&self);
    fn delete(&self);
}

#[cfg(test)]
mod test {
    use crate::database::{
        pager::{diskpager::Envoy, mempage_tree},
        tables::tables,
    };

    use super::*;
    use crate::database::helper::cleanup_file;
    use test_log::test;

    #[test]
    fn meta_page() {
        let path = "table1.rdb";
        cleanup_file(path);
        let pager = Envoy::new(path);
        let db = Database::new(pager);
        cleanup_file(path);
    }

    #[test]
    fn tables_insert1() {
        let pager = mempage_tree();
        let mut db = Database::new(pager);

        let table = TableBuilder::new()
            .id(3)
            .name("mytable")
            .add_col("name", TypeCol::BYTES)
            .add_col("age", TypeCol::INTEGER)
            .pkey(1)
            .build(&mut db)
            .unwrap();

        db.insert_table(&table);

        let dec_table = db.get_table("mytable").unwrap();
        assert_eq!(*dec_table, table);

        // should reject duplicate
        assert!(db.insert_table(&table).is_err());
    }

    #[test]
    fn tables_insert2() {
        let pager = mempage_tree();
        let mut db = Database::new(pager);

        let table = TableBuilder::new()
            .id(3)
            .name("mytable")
            .add_col("name", TypeCol::BYTES)
            .add_col("age", TypeCol::INTEGER)
            .add_col("id", TypeCol::INTEGER)
            .pkey(1)
            .build(&mut db)
            .unwrap();

        db.insert_table(&table);
        let tables = db.get_table("mytable");

        let mut entries = vec![];
        entries.push(Record::new().add("Alice").add(20).add(1));
        entries.push(Record::new().add("Bob").add(15).add(2));
        entries.push(Record::new().add("Charlie").add(25).add(3));

        for entry in entries {
            db.insert_rec(entry, &table).unwrap()
        }

        let q1 = Query::new().add("name", "Alice");
        let q2 = Query::new().add("name", "Bob");
        let q3 = Query::new().add("name", "Charlie");

        let q1_res = db.get_rec(q1, &table).unwrap().decode();
        assert_eq!(q1_res[0], DataCell::Int(20));
        assert_eq!(q1_res[1], DataCell::Int(1));

        let q2_res = db.get_rec(q2, &table).unwrap().decode();
        assert_eq!(q2_res[0], DataCell::Int(15));
        assert_eq!(q2_res[1], DataCell::Int(2));

        let q3_res = db.get_rec(q3, &table).unwrap().decode();
        assert_eq!(q3_res[0], DataCell::Int(25));
        assert_eq!(q3_res[1], DataCell::Int(3));
    }

    #[test]
    fn query_input() {
        let pager = mempage_tree();
        let mut db = Database::new(pager);

        let table = TableBuilder::new()
            .id(3)
            .name("mytable")
            .add_col("name", TypeCol::BYTES)
            .add_col("age", TypeCol::INTEGER)
            .add_col("id", TypeCol::INTEGER)
            .pkey(2)
            .build(&mut db)
            .unwrap();

        db.insert_table(&table).unwrap();
        let tables = db.get_table("mytable");

        let good_query = Query::new().add("name", "Alice").add("age", 10);

        assert!(good_query.encode(&table).is_ok());

        let good_query = Query::new().add("name", "Alice").add("age", 10);
        let unordered = Query::new().add("age", 10).add("name", "Alice");
        assert_eq!(
            good_query.encode(&table).unwrap(),
            unordered.encode(&table).unwrap()
        );

        let bad_query = Query::new().add("name", "Alice");
        assert!(bad_query.encode(&table).is_err());

        let bad_query = Query::new().add("dfasdf", "fasdf");
        assert!(bad_query.encode(&table).is_err());

        let bad_query = Query::new();
        assert!(bad_query.encode(&table).is_err())
    }

    #[test]
    fn table_ids() {
        let pager = mempage_tree();
        let mut db = Database::new(pager);
        assert_eq!(db.new_tid().unwrap(), 3);
        assert_eq!(db.new_tid().unwrap(), 4);
        assert_eq!(db.new_tid().unwrap(), 5);
    }
}
