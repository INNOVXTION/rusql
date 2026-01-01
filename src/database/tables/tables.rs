use std::{collections::HashMap, marker::PhantomData, ops::Deref};

use crate::database::{
    btree::{Compare, ScanMode, SetFlag},
    codec::{Codec, NumEncode, PREFIX_LEN, TID_LEN},
    errors::{Error, Result, TableError},
    pager::diskpager::KVEngine,
    tables::{
        Key, Value,
        db::Database,
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

pub const DEF_TABLE_NAME: &'static str = "tdef";
pub const DEF_TABLE_COL1: &'static str = "name";
pub const DEF_TABLE_COL2: &'static str = "def";

pub const DEF_TABLE_ID: u32 = 1;
pub const DEF_TABLE_PKEYS: u16 = 1;

pub const META_TABLE_NAME: &'static str = "tmeta";
pub const META_TABLE_COL1: &'static str = "name";
pub const META_TABLE_COL2: &'static str = "tid";
pub const META_TABLE_ID_ROW: &'static str = "tid";

pub const META_TABLE_ID: u32 = 2;
pub const META_TABLE_PKEYS: u16 = 1;

pub const PKEY_PREFIX: u16 = 0;

/// wrapper for sentinal value
#[derive(Serialize, Deserialize)]
pub(super) struct MetaTable(Table);

impl MetaTable {
    pub fn new() -> Self {
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

    pub fn as_table(self) -> Table {
        self.0
    }
}

/// wrapper for sentinal value
#[derive(Serialize, Deserialize)]
pub(super) struct TDefTable(Table);

impl TDefTable {
    pub fn new() -> Self {
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

    /// deprecated, for testing purposes only
    ///
    /// build() requests a free TID when omitted
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
    pub name: String,
    pub id: u32,
    pub cols: Vec<Column>,
    pub pkeys: u16,
    pub indices: Vec<Index>,

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

    /// adds a secondary index
    pub fn add_index(&mut self, col: &str) -> Result<()> {
        // check if column exists
        let col_idx = match self.col_exists(col) {
            Some(i) => i,
            None => {
                return Err(TableError::IndexCreateError("column doesnt exist".to_string()).into());
            }
        };

        // check for duplicate indices
        if self.idx_exists(col).is_some() {
            return Err(TableError::IndexCreateError("index already exists".to_string()).into());
        }

        self.indices.push(Index {
            name: col.to_string(),
            columns: vec![col_idx],
            prefix: self.indices.len() as u16,
            kind: IdxKind::Secondary,
        });

        Ok(())
    }

    /// removes a secondary index
    pub fn remove_index(&mut self, name: &str) -> Result<()> {
        let idx = match self.idx_exists(name) {
            Some(i) => i,
            None => {
                return Err(TableError::IndexDeleteError("index doesnt exist!".to_string()).into());
            }
        };
        if self.indices[idx].kind == IdxKind::Primary {
            return Err(
                TableError::IndexDeleteError("cant delete primary index".to_string()).into(),
            );
        }
        self.indices.remove(idx);
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub(crate) struct Index {
    /// unique identifier
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

#[cfg(test)]
mod test {
    use crate::database::{
        btree::Compare,
        pager::{diskpager::Envoy, mempage_tree},
        types::DataCell,
    };

    use super::super::db::*;
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
    fn records_insert_search() -> Result<()> {
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

        let q1 = Query::with_key(&table).add("name", "Alice").encode()?;
        let q2 = Query::with_key(&table).add("name", "Bob").encode()?;
        let q3 = Query::with_key(&table).add("name", "Charlie").encode()?;

        let q1_res = db.get_rec(q1).unwrap().decode();
        assert_eq!(q1_res[0], DataCell::Int(20));
        assert_eq!(q1_res[1], DataCell::Int(1));

        let q2_res = db.get_rec(q2).unwrap().decode();
        assert_eq!(q2_res[0], DataCell::Int(15));
        assert_eq!(q2_res[1], DataCell::Int(2));

        let q3_res = db.get_rec(q3).unwrap().decode();
        assert_eq!(q3_res[0], DataCell::Int(25));
        assert_eq!(q3_res[1], DataCell::Int(3));
        Ok(())
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

        let good_query = Query::with_key(&table).add("name", "Alice").add("age", 10);

        assert!(good_query.encode().is_ok());

        let good_query = Query::with_key(&table).add("name", "Alice").add("age", 10);
        let unordered = Query::with_key(&table).add("age", 10).add("name", "Alice");
        assert_eq!(good_query.encode().unwrap(), unordered.encode().unwrap());

        let bad_query = Query::with_key(&table).add("name", "Alice");
        assert!(bad_query.encode().is_err());

        let bad_query = Query::with_key(&table).add("dfasdf", "fasdf");
        assert!(bad_query.encode().is_err());

        let bad_query = Query::with_key(&table);
        assert!(bad_query.encode().is_err())
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
        assert!(Query::with_key(&table).encode().is_err());

        // wrong column name
        assert!(Query::with_key(&table).add("nope", "x").encode().is_err());
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
            Query::with_key(&table1).add("name", "Alice").encode()?,
            Compare::GE,
        );
        let res = db.scan(open)?;

        assert_eq!(res.len(), 3);
        assert_eq!(res[0].to_string(), "Alice 20");
        assert_eq!(res[1].to_string(), "Bob 15");
        assert_eq!(res[2].to_string(), "Charlie 25");

        let open = ScanMode::Open(
            Query::with_key(&table2).add("id", 20).encode()?,
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

    use super::super::db::*;
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
        let lo_key = Query::with_key(&table).add("id", 5i64).encode()?;
        let hi_key = Query::with_key(&table).add("id", 15i64).encode()?;

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
            Query::with_key(&table1).add("key", "key_a_1").encode()?,
            Compare::GE,
        )?;
        let res1 = db.scan(open)?;

        // Should only contain table1 records
        assert!(res1.iter().all(|r| r.to_string().contains("val_a_")));
        assert_eq!(res1.len(), 10);

        // Scan table2
        let open = ScanMode::new_open(
            Query::with_key(&table2).add("key", "key_b_1").encode()?,
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
            Query::with_key(&table).add("score", 50i64).encode()?,
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
            Query::with_key(&table).add("id", 100i64).encode()?,
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
            Query::with_key(&table).add("code", "CODE_001").encode()?,
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
            Query::with_key(&table).add("value", 35i64).encode()?,
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
            Query::with_key(&table).add("id", 100i64).encode()?,
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
            Query::with_key(&table).add("name", "bob").encode()?,
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
            let key = Query::with_key(&table).add("id", *i as i64).encode()?;
            db.kve.delete(key)?;
        }

        // Scan from beginning
        let open = ScanMode::new_open(
            Query::with_key(&table).add("id", 1i64).encode()?,
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
