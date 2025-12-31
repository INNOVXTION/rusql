use std::{collections::HashMap, marker::PhantomData, ops::Deref};

use crate::database::{
    btree::{Compare, ScanMode, SetFlag},
    codec::{Codec, NumEncode, PREFIX_LEN, TID_LEN},
    errors::{Error, Result, TableError},
    pager::diskpager::KVEngine,
    tables::{
        Key, Value,
        records::{Query, Record},
    },
    types::{BTREE_MAX_VAL_SIZE, DataCell},
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
 */

const DEF_TABLE_NAME: &'static str = "tdef";
const DEF_TABLE_COL1: &'static str = "name";
const DEF_TABLE_COL2: &'static str = "def";

const DEF_TABLE_ID: u32 = 1;
const DEF_TABLE_PKEYS: u16 = 1;

const META_TABLE_NAME: &'static str = "tmeta";
const META_TABLE_COL1: &'static str = "name";
const META_TABLE_COL2: &'static str = "tid";
const META_TABLE_ID_ROW: &'static str = "tid";

const META_TABLE_ID: u32 = 2;
const META_TABLE_PKEYS: u16 = 1;

const PKEY_PREFIX: u16 = 0;

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
            indices: vec![Index {
                name: META_TABLE_COL1.to_string(),
                columns: (0..META_TABLE_PKEYS).collect(),
                prefix: PKEY_PREFIX,
                kind: IdxKind::Primary,
            }],
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
            indices: vec![Index {
                name: DEF_TABLE_COL1.to_string(),
                columns: (0..DEF_TABLE_PKEYS).collect(),
                prefix: PKEY_PREFIX,
                kind: IdxKind::Primary,
            }],
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
    id: Option<u32>,
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
    pub fn id(mut self, id: u32) -> Self {
        self.id = Some(id);
        self
    }

    /// adds a column to the table, order sensitive
    pub fn add_col(mut self, title: &str, data_type: TypeCol) -> Self {
        self.cols.push(Column {
            title: title.to_string(),
            data_type,
        });
        self
    }

    /// declares amount of columns as primary keys, starting in order in which columns were added
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

        let primary_idx = Index {
            name: cols[0].title.clone(),
            columns: (0..pkeys).collect(),
            prefix: PKEY_PREFIX,
            kind: IdxKind::Primary,
        };

        Ok(Table {
            name,
            id,
            cols,
            pkeys,
            indices: vec![primary_idx],
            _priv: PhantomData,
        })
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub(crate) struct Table {
    pub(crate) name: String,
    pub(crate) id: u32,
    pub(crate) cols: Vec<Column>,
    pub(crate) pkeys: u16,
    pub(crate) indices: Vec<Index>,

    // ensures tables are built through constructor
    _priv: PhantomData<bool>,
}

impl Table {
    /// encodes table to JSON string
    pub fn encode(&self) -> Result<String> {
        let data = serde_json::to_string(&self).map_err(|e| {
            error!(%e, "error when encoding table");
            TableError::SerializeTableError(e)
        })?;
        if data.len() > BTREE_MAX_VAL_SIZE {
            return Err(TableError::EncodeTableError(
                "Table schema exceeds value size limit".to_string(),
            )
            .into());
        }
        Ok(data)
    }

    /// decodes JSON string into table
    pub fn decode(value: Value) -> Result<Self> {
        serde_json::from_str(&value.to_string()).map_err(|e| {
            error!(%e, "error when decoding table");
            TableError::SerializeTableError(e).into()
        })
    }

    /// checks if col by name exists and matches data type
    pub fn valid_col(&self, title: &str, data: &DataCell) -> bool {
        let cell_type = match data {
            DataCell::Str(_) => TypeCol::BYTES,
            DataCell::Int(_) => TypeCol::INTEGER,
        };

        for col in self.cols.iter() {
            if col.title == title && col.data_type == cell_type {
                return true;
            }
        }
        false
    }

    fn col_exists(&self, title: &str) -> Option<u16> {
        for (i, col) in self.cols.iter().enumerate() {
            if col.title == title {
                return Some(i as u16);
            }
        }
        None
    }

    fn idx_exists(&self, title: &str) -> Option<usize> {
        for (i, idx) in self.indices.iter().enumerate() {
            if idx.name == title {
                return Some(i);
            }
        }
        None
    }

    pub fn add_index(&mut self, col: &str) -> Result<()> {
        let col_idx = match self.col_exists(col) {
            Some(i) => i,
            None => {
                return Err(TableError::IndexCreateError("column doesnt exist".to_string()).into());
            }
        };
        self.indices.push(Index {
            name: col.to_string(),
            columns: vec![col_idx],
            prefix: self.indices.len() as u16,
            kind: IdxKind::Secondary,
        });
        Ok(())
    }

    pub fn remove_index(&mut self, name: &str) -> Result<()> {
        let idx = match self.idx_exists(name) {
            Some(i) => i,
            None => {
                return Err(TableError::IndexDeleteError("index doesnt exist!".to_string()).into());
            }
        };
        self.indices.remove(idx);
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub(crate) struct Index {
    pub name: String,
    /// indices into the table.cols
    pub columns: Vec<u16>,
    /// prefix for encoding
    pub prefix: u16,
    pub kind: IdxKind,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub(crate) enum IdxKind {
    Primary,
    Secondary,
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

pub(crate) struct SetRequest {
    payload: Vec<(Key, Value)>,
    flag: SetFlag,
}

pub(crate) enum SearchRequest {
    Lookup(Key),
    Range(ScanMode),
    FullTable(Key),
}

pub(crate) enum DeleteRequest {
    Table,
    Column,
    Record,
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
        let key = Query::new()
            .with_key(META_TABLE_COL1, META_TABLE_ID_ROW) // we query name column, where pkey = tid
            .encode(&self.get_meta())?;

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
                Ok(3) // tid 1 and 2 are taken
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
        let key = Query::new()
            .with_key("name", name)
            .encode(&self.tdef)
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
        let qu = Query::new()
            .with_key(DEF_TABLE_COL1, name)
            .encode(&self.tdef)?;

        self.buffer.remove(name);
        self.kve.delete(qu).map_err(|e| {
            error!(%e, "error when dropping table");
            TableError::DeleteTableError("dropping table error when deleting".to_string()).into()
        })
    }

    #[instrument(name = "insert rec", skip_all)]
    fn insert_rec(&mut self, rec: Record, schema: &Table, flag: SetFlag) -> Result<()> {
        info!(?rec, "inserting record");

        // WIP FOR TESTING
        let (key, value) = rec.encode(schema)?.next().unwrap();

        self.kve.set(key, value, flag)?;
        Ok(())
    }

    fn get_rec(&self, query: Query, schema: &Table) -> Option<Value> {
        info!(?query, "querying");

        self.kve.get(query.encode(schema).ok()?).ok()
    }

    fn scan(&self, mode: ScanMode) -> Result<Vec<Record>> {
        self.kve.scan(mode)
    }

    fn full_table_scan(&self, schema: &Table) -> Result<Vec<Record>> {
        let mut buf = [0u8; TID_LEN + PREFIX_LEN];

        // writing a seek key with TID + PREFIX = 0
        let _ = &mut buf[..].write_u32(schema.id).write_u16(0);
        let seek_key = Key::from_encoded_slice(&buf);
        let seek_mode = ScanMode::Open(seek_key, Compare::GE);

        self.scan(seek_mode)
    }

    fn creat_index() {}

    fn delete_index() {}
}

#[cfg(test)]
mod test {
    use crate::database::{
        btree::Compare,
        pager::{diskpager::Envoy, mempage_tree},
        types::DataCell,
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
    fn tables_encode_decode() {
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

        let _ = db.insert_table(&table);

        let dec_table = db.get_table("mytable").unwrap();
        assert_eq!(*dec_table, table);

        // should reject duplicate
        assert!(db.insert_table(&table).is_err());
    }

    #[test]
    fn records_insert_search() {
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

        let _ = db.insert_table(&table);
        let tables = db.get_table("mytable");

        let mut entries = vec![];
        entries.push(Record::new().add("Alice").add(20).add(1));
        entries.push(Record::new().add("Bob").add(15).add(2));
        entries.push(Record::new().add("Charlie").add(25).add(3));

        for entry in entries {
            db.insert_rec(entry, &table, SetFlag::UPSERT).unwrap()
        }

        let q1 = Query::new().with_key("name", "Alice");
        let q2 = Query::new().with_key("name", "Bob");
        let q3 = Query::new().with_key("name", "Charlie");

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

        let good_query = Query::new().with_key("name", "Alice").with_key("age", 10);

        assert!(good_query.encode(&table).is_ok());

        let good_query = Query::new().with_key("name", "Alice").with_key("age", 10);
        let unordered = Query::new().with_key("age", 10).with_key("name", "Alice");
        assert_eq!(
            good_query.encode(&table).unwrap(),
            unordered.encode(&table).unwrap()
        );

        let bad_query = Query::new().with_key("name", "Alice");
        assert!(bad_query.encode(&table).is_err());

        let bad_query = Query::new().with_key("dfasdf", "fasdf");
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

    #[test]
    fn table_builder_validations() {
        let pager = mempage_tree();
        let mut db = Database::new(pager);

        // empty name
        assert!(
            TableBuilder::new()
                .name("")
                .id(10)
                .add_col("a", TypeCol::BYTES)
                .pkey(1)
                .build(&mut db)
                .is_err()
        );

        // zero pkeys
        assert!(
            TableBuilder::new()
                .name("t")
                .id(11)
                .add_col("a", TypeCol::BYTES)
                .pkey(0)
                .build(&mut db)
                .is_err()
        );

        // more pkeys than cols
        assert!(
            TableBuilder::new()
                .name("t")
                .id(12)
                .add_col("a", TypeCol::BYTES)
                .pkey(2)
                .build(&mut db)
                .is_err()
        );

        // not enough columns (less than required for your logic)
        assert!(
            TableBuilder::new()
                .name("t")
                .id(13)
                .pkey(1)
                .build(&mut db)
                .is_err()
        );
    }

    #[test]
    fn duplicate_table_name_rejected() {
        let pager = mempage_tree();
        let mut db = Database::new(pager);

        let table = TableBuilder::new()
            .id(12)
            .name("dup")
            .add_col("x", TypeCol::BYTES)
            .pkey(1)
            .build(&mut db)
            .unwrap();

        assert!(db.insert_table(&table).is_ok());
        assert!(db.insert_table(&table).is_err());
    }

    #[test]
    fn drop_table_removes_table() {
        let pager = mempage_tree();
        let mut db = Database::new(pager);

        let table = TableBuilder::new()
            .id(13)
            .name("droppable")
            .add_col("x", TypeCol::BYTES)
            .pkey(1)
            .build(&mut db)
            .unwrap();

        db.insert_table(&table).unwrap();
        assert!(db.get_table("droppable").is_some());

        db.drop_table("droppable").unwrap();
        assert!(db.get_table("droppable").is_none());
    }

    #[test]
    fn new_tid_persists_with_envoy() {
        let path = "test-files/tid_persist.rdb";
        cleanup_file(path);
        {
            let pager = Envoy::new(path);
            let mut db = Database::new(pager);
            assert_eq!(db.new_tid().unwrap(), 3);
            assert_eq!(db.new_tid().unwrap(), 4);
        }
        // reopen
        {
            let pager = Envoy::new(path);
            let mut db = Database::new(pager);
            // next tid continues
            assert_eq!(db.new_tid().unwrap(), 5);
        }
        cleanup_file(path);
    }

    #[test]
    fn invalid_queries_rejected() {
        let pager = mempage_tree();
        let mut db = Database::new(pager);

        let table = TableBuilder::new()
            .id(16)
            .name("invalid")
            .add_col("x", TypeCol::BYTES)
            .pkey(1)
            .build(&mut db)
            .unwrap();

        db.insert_table(&table).unwrap();

        // missing primary key
        assert!(Query::new().encode(&table).is_err());

        // wrong column name
        assert!(Query::new().with_key("nope", "x").encode(&table).is_err());
    }

    #[test]
    fn scan_open() -> Result<()> {
        let path = "test-files/scan.rdb";
        cleanup_file(path);
        let pager = Envoy::new("test-files/scan.rdb");
        let mut db = Database::new(pager);

        let table1 = TableBuilder::new()
            .id(5)
            .name("table_1")
            .add_col("name", TypeCol::BYTES)
            .add_col("age", TypeCol::INTEGER)
            .pkey(1)
            .build(&mut db)
            .unwrap();

        let table2 = TableBuilder::new()
            .id(7)
            .name("table_2")
            .add_col("id", TypeCol::INTEGER)
            .add_col("name", TypeCol::BYTES)
            .add_col("job", TypeCol::BYTES)
            .pkey(1)
            .build(&mut db)
            .unwrap();

        db.insert_table(&table1)?;
        db.insert_table(&table2)?;

        assert!(db.get_table("table_1").is_some());
        assert!(db.get_table("table_2").is_some());

        let mut entries_t1 = vec![];
        entries_t1.push(Record::new().add("Alice").add(20));
        entries_t1.push(Record::new().add("Bob").add(15));
        entries_t1.push(Record::new().add("Charlie").add(25));

        for entry in entries_t1 {
            db.insert_rec(entry, &table1, SetFlag::UPSERT)?
        }

        let mut entries_t2 = vec![];
        entries_t2.push(Record::new().add(20).add("Alice").add("teacher"));
        entries_t2.push(Record::new().add(15).add("Bob").add("clerk"));
        entries_t2.push(Record::new().add(25).add("Charlie").add("fire fighter"));

        for entry in entries_t2 {
            db.insert_rec(entry, &table2, SetFlag::UPSERT)?
        }

        let open = ScanMode::Open(
            Query::new().with_key("name", "Alice").encode(&table1)?,
            Compare::GE,
        );
        let res = db.scan(open)?;

        assert_eq!(res.len(), 3);
        assert_eq!(res[0].to_string(), "Alice 20");
        assert_eq!(res[1].to_string(), "Bob 15");
        assert_eq!(res[2].to_string(), "Charlie 25");

        let open = ScanMode::Open(
            Query::new().with_key("id", 20).encode(&table2)?,
            Compare::GE,
        );
        let res = db.scan(open)?;

        assert_eq!(res.len(), 2);
        assert_eq!(res[0].to_string(), "20 Alice teacher");
        assert_eq!(res[1].to_string(), "25 Charlie fire fighter");

        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn full_table_scan_seek() -> Result<()> {
        let path = "test-files/scan.rdb";
        cleanup_file(path);
        let pager = Envoy::new("test-files/scan.rdb");
        let mut db = Database::new(pager);

        let table1 = TableBuilder::new()
            .id(5)
            .name("table_1")
            .add_col("name", TypeCol::BYTES)
            .add_col("age", TypeCol::INTEGER)
            .pkey(1)
            .build(&mut db)
            .unwrap();

        let table2 = TableBuilder::new()
            .id(7)
            .name("table_2")
            .add_col("id", TypeCol::INTEGER)
            .add_col("name", TypeCol::BYTES)
            .add_col("job", TypeCol::BYTES)
            .pkey(1)
            .build(&mut db)
            .unwrap();

        db.insert_table(&table1)?;
        db.insert_table(&table2)?;

        let mut entries_t1 = vec![];
        entries_t1.push(Record::new().add("Alice").add(20));
        entries_t1.push(Record::new().add("Bob").add(15));
        entries_t1.push(Record::new().add("Charlie").add(25));

        for entry in entries_t1 {
            db.insert_rec(entry, &table1, SetFlag::UPSERT)?
        }

        let mut entries_t2 = vec![];
        entries_t2.push(Record::new().add(20).add("Alice").add("teacher"));
        entries_t2.push(Record::new().add(15).add("Bob").add("clerk"));
        entries_t2.push(Record::new().add(25).add("Charlie").add("fire fighter"));

        for entry in entries_t2 {
            db.insert_rec(entry, &table2, SetFlag::UPSERT)?
        }

        let res = db.full_table_scan(&table1)?;
        assert_eq!(res.len(), 3);

        let res = db.full_table_scan(&table2)?;
        assert_eq!(res.len(), 3);

        cleanup_file(path);
        Ok(())
    }
}

// these tests were cooked up by claude, good luck!
#[cfg(test)]
mod scan {
    use crate::database::{btree::Compare, pager::diskpager::Envoy};

    use super::*;
    use crate::database::helper::cleanup_file;
    use test_log::test;

    #[test]
    fn scan_range_between_keys() -> Result<()> {
        let path = "test-files/scan_range.rdb";
        cleanup_file(path);
        let pager = Envoy::new(path);
        let mut db = Database::new(pager);

        let table = TableBuilder::new()
            .id(10)
            .name("range_table")
            .add_col("id", TypeCol::INTEGER)
            .add_col("name", TypeCol::BYTES)
            .pkey(1)
            .build(&mut db)
            .unwrap();

        db.insert_table(&table)?;

        // Insert records with ids 1-20
        for i in 1..=20 {
            db.insert_rec(
                Record::new().add(i as i64).add(format!("name_{}", i)),
                &table,
                SetFlag::UPSERT,
            )?;
        }

        // Scan from id 5 to 15
        let lo_key = Query::new().with_key("id", 5i64).encode(&table)?;
        let hi_key = Query::new().with_key("id", 15i64).encode(&table)?;

        let range = ScanMode::new_range((lo_key, Compare::GE), (hi_key, Compare::LE))?;

        let res = db.scan(range)?;

        assert_eq!(res.len(), 11); // 5 through 15 inclusive
        assert_eq!(res[0].to_string(), "5 name_5");
        assert_eq!(res[10].to_string(), "15 name_15");

        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn scan_multiple_tables_isolation() -> Result<()> {
        let path = "test-files/scan_isolation.rdb";
        cleanup_file(path);
        let pager = Envoy::new(path);
        let mut db = Database::new(pager);

        let table1 = TableBuilder::new()
            .id(20)
            .name("table_a")
            .add_col("key", TypeCol::BYTES)
            .add_col("value", TypeCol::BYTES)
            .pkey(1)
            .build(&mut db)
            .unwrap();

        let table2 = TableBuilder::new()
            .id(21)
            .name("table_b")
            .add_col("key", TypeCol::BYTES)
            .add_col("value", TypeCol::BYTES)
            .pkey(1)
            .build(&mut db)
            .unwrap();

        db.insert_table(&table1)?;
        db.insert_table(&table2)?;

        // Insert into table1
        for i in 1..=10 {
            db.insert_rec(
                Record::new()
                    .add(format!("key_a_{}", i))
                    .add(format!("val_a_{}", i)),
                &table1,
                SetFlag::UPSERT,
            )?;
        }

        // Insert into table2
        for i in 1..=10 {
            db.insert_rec(
                Record::new()
                    .add(format!("key_b_{}", i))
                    .add(format!("val_b_{}", i)),
                &table2,
                SetFlag::UPSERT,
            )?;
        }

        // Scan table1
        let open = ScanMode::new_open(
            Query::new().with_key("key", "key_a_1").encode(&table1)?,
            Compare::GE,
        )?;
        let res1 = db.scan(open)?;

        // Should only contain table1 records
        assert!(res1.iter().all(|r| r.to_string().contains("val_a_")));
        assert_eq!(res1.len(), 10);

        // Scan table2
        let open = ScanMode::new_open(
            Query::new().with_key("key", "key_b_1").encode(&table2)?,
            Compare::GE,
        )?;
        let res2 = db.scan(open)?;

        // Should only contain table2 records
        assert!(res2.iter().all(|r| r.to_string().contains("val_b_")));
        assert_eq!(res2.len(), 10);

        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn scan_with_lt_predicate() -> Result<()> {
        let path = "test-files/scan_lt.rdb";
        cleanup_file(path);
        let pager = Envoy::new(path);
        let mut db = Database::new(pager);

        let table = TableBuilder::new()
            .id(30)
            .name("lt_table")
            .add_col("score", TypeCol::INTEGER)
            .add_col("player", TypeCol::BYTES)
            .pkey(1)
            .build(&mut db)
            .unwrap();

        db.insert_table(&table)?;

        for i in 1..=10 {
            db.insert_rec(
                Record::new()
                    .add(i as i64 * 10)
                    .add(format!("player_{}", i)),
                &table,
                SetFlag::UPSERT,
            )?;
        }

        // Scan backwards from score 50
        let open = ScanMode::new_open(
            Query::new().with_key("score", 50i64).encode(&table)?,
            Compare::LT,
        )?;

        let res = db.scan(open)?;

        // Should return scores 40, 30, 20, 10
        assert_eq!(res.len(), 4);
        assert_eq!(res[0].to_string(), "40 player_4");
        assert_eq!(res[3].to_string(), "10 player_1");

        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn scan_empty_result_set() -> Result<()> {
        let path = "test-files/scan_empty.rdb";
        cleanup_file(path);
        let pager = Envoy::new(path);
        let mut db = Database::new(pager);

        let table = TableBuilder::new()
            .id(40)
            .name("empty_table")
            .add_col("id", TypeCol::INTEGER)
            .add_col("data", TypeCol::BYTES)
            .pkey(1)
            .build(&mut db)
            .unwrap();

        db.insert_table(&table)?;

        for i in 1..=5 {
            db.insert_rec(
                Record::new().add(i as i64).add(format!("data_{}", i)),
                &table,
                SetFlag::UPSERT,
            )?;
        }

        // Scan for values greater than max
        let open = ScanMode::new_open(
            Query::new().with_key("id", 100i64).encode(&table)?,
            Compare::GT,
        )?;

        let result = db.scan(open);
        assert!(result.is_err()); // Should error on empty result

        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn scan_single_record_result() -> Result<()> {
        let path = "test-files/scan_single.rdb";
        cleanup_file(path);
        let pager = Envoy::new(path);
        let mut db = Database::new(pager);

        let table = TableBuilder::new()
            .id(50)
            .name("single_table")
            .add_col("code", TypeCol::BYTES)
            .add_col("value", TypeCol::INTEGER)
            .pkey(1)
            .build(&mut db)
            .unwrap();

        db.insert_table(&table)?;

        db.insert_rec(
            Record::new().add("CODE_001").add(42i64),
            &table,
            SetFlag::UPSERT,
        )?;

        let open = ScanMode::new_open(
            Query::new().with_key("code", "CODE_001").encode(&table)?,
            Compare::EQ,
        );
        // EQ is not allowed in open scans, should error
        assert!(open.is_err());

        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn scan_with_le_predicate() -> Result<()> {
        let path = "test-files/scan_le.rdb";
        cleanup_file(path);
        let pager = Envoy::new(path);
        let mut db = Database::new(pager);

        let table = TableBuilder::new()
            .id(60)
            .name("le_table")
            .add_col("value", TypeCol::INTEGER)
            .add_col("label", TypeCol::BYTES)
            .pkey(1)
            .build(&mut db)
            .unwrap();

        db.insert_table(&table)?;

        for i in 10..=50 {
            db.insert_rec(
                Record::new().add(i as i64).add(format!("label_{}", i)),
                &table,
                SetFlag::UPSERT,
            )?;
        }

        // Scan from 35 backwards (LE)
        let open = ScanMode::new_open(
            Query::new().with_key("value", 35i64).encode(&table)?,
            Compare::LE,
        )?;

        let res = db.scan(open)?;

        // Should include 35 and go backwards to 10
        assert_eq!(res.len(), 26); // 35 down to 10
        assert_eq!(res[0].to_string(), "35 label_35");

        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn scan_large_dataset() -> Result<()> {
        let path = "test-files/scan_large.rdb";
        cleanup_file(path);
        let pager = Envoy::new(path);
        let mut db = Database::new(pager);

        let table = TableBuilder::new()
            .id(70)
            .name("large_table")
            .add_col("id", TypeCol::INTEGER)
            .add_col("name", TypeCol::BYTES)
            .pkey(1)
            .build(&mut db)
            .unwrap();

        db.insert_table(&table)?;

        // Insert 500 records
        for i in 1..=500 {
            db.insert_rec(
                Record::new().add(i as i64).add(format!("name_{:04}", i)),
                &table,
                SetFlag::UPSERT,
            )?;
        }

        // Scan from id 100 onwards
        let open = ScanMode::new_open(
            Query::new().with_key("id", 100i64).encode(&table)?,
            Compare::GT,
        )?;

        let res = db.scan(open)?;

        assert_eq!(res.len(), 400); // 101 to 500
        assert_eq!(res[0].to_string(), "101 name_0101");
        assert_eq!(res[399].to_string(), "500 name_0500");

        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn scan_byte_string_keys() -> Result<()> {
        let path = "test-files/scan_bytes.rdb";
        cleanup_file(path);
        let pager = Envoy::new(path);
        let mut db = Database::new(pager);

        let table = TableBuilder::new()
            .id(80)
            .name("byte_table")
            .add_col("name", TypeCol::BYTES)
            .add_col("data", TypeCol::BYTES)
            .pkey(1)
            .build(&mut db)
            .unwrap();

        db.insert_table(&table)?;

        let names = vec!["alice", "bob", "charlie", "david", "eve"];
        for name in &names {
            db.insert_rec(
                Record::new().add(*name).add(format!("data_{}", name)),
                &table,
                SetFlag::UPSERT,
            )?;
        }

        // Scan from "bob" onwards
        let open = ScanMode::new_open(
            Query::new().with_key("name", "bob").encode(&table)?,
            Compare::GE,
        )?;

        let res = db.scan(open)?;

        assert_eq!(res.len(), 4); // bob, charlie, david, eve
        assert_eq!(res[0].to_string(), "bob data_bob");

        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn scan_after_deletes() -> Result<()> {
        let path = "test-files/scan_after_delete.rdb";
        cleanup_file(path);
        let pager = Envoy::new(path);
        let mut db = Database::new(pager);

        let table = TableBuilder::new()
            .id(90)
            .name("delete_table")
            .add_col("id", TypeCol::INTEGER)
            .add_col("value", TypeCol::BYTES)
            .pkey(1)
            .build(&mut db)
            .unwrap();

        db.insert_table(&table)?;

        // Insert 10 records
        for i in 1..=10 {
            db.insert_rec(
                Record::new().add(i as i64).add(format!("val_{}", i)),
                &table,
                SetFlag::UPSERT,
            )?;
        }

        // Delete some records
        for i in [3, 5, 7].iter() {
            let key = Query::new().with_key("id", *i as i64).encode(&table)?;
            db.kve.delete(key)?;
        }

        // Scan from beginning
        let open = ScanMode::new_open(
            Query::new().with_key("id", 1i64).encode(&table)?,
            Compare::GE,
        )?;

        let res = db.scan(open)?;

        // Should have 7 records (10 - 3 deleted)
        assert_eq!(res.len(), 7);
        assert!(!res.iter().any(|r| r.to_string().contains("val_3")));
        assert!(!res.iter().any(|r| r.to_string().contains("val_5")));
        assert!(!res.iter().any(|r| r.to_string().contains("val_7")));

        cleanup_file(path);
        Ok(())
    }
}
