use std::{marker::PhantomData, ops::Deref};

use crate::database::{
    errors::{Error, Result, TableError},
    pager::KVEngine,
    tables::{Key, Value},
    transactions::tx::TX,
    types::{BTREE_MAX_VAL_SIZE, DataCell},
};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, instrument};

/*
 * Encoding Layout:
 * |--------------KEY---------------|----Value-----|
 * |                  [Col1][Col2]..|[Col3][Col4]..|
 * |[TABLE ID][PREFIX][PK1 ][PK2 ]..|[ v1 ][ v2 ]..|
 *
 * Key: 0 Val: Tdef Schema
 *
 * Tdef, id = 1:
 * |-----KEY------|----Val---|
 * |      [ Col1 ]|[  Col2  ]|
 * |[1][0][ name ]|[  def   ]|
 *
 * Meta, id = 2:
 * |-----KEY------|----Val---|
 * |      [ Col1 ]|[  Col2  ]|
 * |[2][0][ key  ]|[  val   ]|
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

pub const LOWEST_PREMISSIABLE_TID: u32 = DEF_TABLE_ID + META_TABLE_ID;
pub const PKEY_PREFIX: u16 = 0;

/// wrapper for sentinal value
#[derive(Serialize, Deserialize)]
pub(crate) struct MetaTable(Table);

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
                columns: (0..META_TABLE_PKEYS as usize).collect(),
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
pub(crate) struct TDefTable(Table);

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
                columns: (0..DEF_TABLE_PKEYS as usize).collect(),
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
    pub fn build(self, pager: &mut TX) -> Result<Table> {
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
            columns: (0..pkeys as usize).collect(),
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
        if title.is_empty() {
            return false;
        }
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

    /// returns idx in column array matching title
    pub fn col_exists(&self, title: &str) -> Option<usize> {
        if title.is_empty() {
            return None;
        }
        for (i, col) in self.cols.iter().enumerate() {
            if col.title == title {
                return Some(i);
            }
        }
        None
    }

    /// returns idx into indices array matching name
    fn idx_exists(&self, idx_name: &str) -> Option<usize> {
        if idx_name.is_empty() {
            return None;
        }

        for (i, idx) in self.indices.iter().enumerate() {
            if idx.name == idx_name {
                return Some(i);
            }
        }
        None
    }

    pub fn create_index(&mut self, name: &str) -> Result<()> {
        if name.is_empty() {
            return Err(
                TableError::IndexDeleteError("index name cant be empty".to_string()).into(),
            );
        }

        // check for duplicate indices
        if self.idx_exists(name).is_some() {
            return Err(TableError::IndexCreateError("index already exists".to_string()).into());
        }

        self.indices.push(Index {
            name: name.to_string(),
            columns: vec![],
            prefix: self.indices.len() as u16,
            kind: IdxKind::Secondary,
        });

        Ok(())
    }

    /// adds a secondary index, returns idx into the col arr
    pub fn add_col_to_index(&mut self, idx_name: &str, col: &str) -> Result<()> {
        if col.is_empty() || idx_name.is_empty() {
            return Err(
                TableError::IndexDeleteError("index name cant be empty".to_string()).into(),
            );
        }

        // check if column exists
        let col_idx = match self.col_exists(col) {
            Some(i) => i,
            None => {
                return Err(TableError::IndexCreateError("column doesnt exist".to_string()).into());
            }
        };

        for e in self.indices.iter_mut() {
            if e.name == idx_name {
                if e.columns.contains(&col_idx) {
                    return Err(TableError::IndexCreateError(
                        "Column is already part of the index".to_string(),
                    )
                    .into());
                }
                e.columns.push(col_idx);
                return Ok(());
            }
        }
        Err(TableError::IndexCreateError("idx doesnt exist".to_string()).into())
    }

    /// removes a secondary index
    pub fn remove_index(&mut self, name: &str) -> Result<()> {
        if name.is_empty() {
            return Err(
                TableError::IndexDeleteError("index name cant be empty".to_string()).into(),
            );
        }

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
    pub columns: Vec<usize>,
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
mod secondary_index_tests {
    use super::*;
    use crate::database::{
        helper::cleanup_file,
        pager::transaction::Transaction,
        tables::{Record, TypeCol},
        transactions::{kvdb::KVDB, tx::TXKind},
    };
    use std::sync::Arc;
    use test_log::test;

    #[test]
    fn add_single_secondary_index() {
        let path = "test-files/add_single_index.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let mut table = TableBuilder::new()
            .name("users")
            .id(10)
            .pkey(1)
            .add_col("id", TypeCol::INTEGER)
            .add_col("email", TypeCol::BYTES)
            .add_col("name", TypeCol::BYTES)
            .build(&mut tx)
            .unwrap();

        assert_eq!(table.indices.len(), 1); // only primary key

        table.create_index("email").unwrap();
        let result = table.add_col_to_index("email", "email");

        assert!(result.is_ok());
        assert_eq!(table.indices.len(), 2);
        assert_eq!(table.indices[1].name, "email");
        assert_eq!(table.indices[1].kind, IdxKind::Secondary);
        assert_eq!(table.indices[1].prefix, 1);

        let _ = db.commit(tx);
        cleanup_file(path);
    }

    #[test]
    fn add_multiple_secondary_indices() {
        let path = "test-files/add_multiple_indices.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let mut table = TableBuilder::new()
            .name("products")
            .id(11)
            .pkey(1)
            .add_col("id", TypeCol::INTEGER)
            .add_col("name", TypeCol::BYTES)
            .add_col("category", TypeCol::BYTES)
            .add_col("price", TypeCol::INTEGER)
            .build(&mut tx)
            .unwrap();

        assert_eq!(table.indices.len(), 1);

        table.create_index("name").unwrap();
        table.create_index("category").unwrap();
        table.create_index("price").unwrap();

        table.add_col_to_index("name", "name").unwrap();
        table.add_col_to_index("category", "category").unwrap();
        table.add_col_to_index("price", "price").unwrap();

        assert_eq!(table.indices.len(), 4);
        assert_eq!(table.indices[1].name, "name");
        assert_eq!(table.indices[2].name, "category");
        assert_eq!(table.indices[3].name, "price");
        assert_eq!(table.indices[1].prefix, 1);
        assert_eq!(table.indices[2].prefix, 2);
        assert_eq!(table.indices[3].prefix, 3);

        let _ = db.commit(tx);
        cleanup_file(path);
    }

    #[test]
    fn add_col_to_nonexistant_idx() {
        let path = "test-files/add_index_nonexistent_col.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let mut table = TableBuilder::new()
            .name("test_table")
            .id(12)
            .pkey(1)
            .add_col("id", TypeCol::INTEGER)
            .add_col("value", TypeCol::BYTES)
            .build(&mut tx)
            .unwrap();

        let result = table.add_col_to_index("nonexistent", "nonexistent");

        assert!(result.is_err());
        assert_eq!(table.indices.len(), 1); // unchanged

        let _ = db.commit(tx);
        cleanup_file(path);
    }

    #[test]
    fn add_duplicate_index_fails() {
        let path = "test-files/add_duplicate_index.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let mut table = TableBuilder::new()
            .name("duplicates")
            .id(13)
            .pkey(1)
            .add_col("id", TypeCol::INTEGER)
            .add_col("email", TypeCol::BYTES)
            .build(&mut tx)
            .unwrap();

        table.create_index("email").unwrap();

        table.add_col_to_index("email", "email").unwrap();
        let result = table.add_col_to_index("email", "email");

        assert!(result.is_err());
        assert_eq!(table.indices.len(), 2); // unchanged

        let _ = db.commit(tx);
        cleanup_file(path);
    }

    #[test]
    fn add_empty_index_name_fails() {
        let path = "test-files/add_empty_index_name.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let mut table = TableBuilder::new()
            .name("empty_idx")
            .id(14)
            .pkey(1)
            .add_col("id", TypeCol::INTEGER)
            .build(&mut tx)
            .unwrap();

        let result = table.create_index("");
        assert!(result.is_err());

        let _ = db.commit(tx);
        cleanup_file(path);
    }

    #[test]
    fn remove_secondary_index() {
        let path = "test-files/remove_secondary_index.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let mut table = TableBuilder::new()
            .name("remove_test")
            .id(15)
            .pkey(1)
            .add_col("id", TypeCol::INTEGER)
            .add_col("username", TypeCol::BYTES)
            .build(&mut tx)
            .unwrap();

        table.create_index("username").unwrap();
        table.add_col_to_index("username", "username").unwrap();

        assert_eq!(table.indices.len(), 2);

        let result = table.remove_index("username");
        assert!(result.is_ok());
        assert_eq!(table.indices.len(), 1);
        assert_eq!(table.indices[0].kind, IdxKind::Primary);

        let _ = db.commit(tx);
        cleanup_file(path);
    }

    #[test]
    fn remove_nonexistent_index_fails() {
        let path = "test-files/remove_nonexistent_index.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let mut table = TableBuilder::new()
            .name("no_remove")
            .id(16)
            .pkey(1)
            .add_col("id", TypeCol::INTEGER)
            .add_col("data", TypeCol::BYTES)
            .build(&mut tx)
            .unwrap();

        let result = table.remove_index("nonexistent");
        assert!(result.is_err());
        assert_eq!(table.indices.len(), 1);

        let _ = db.commit(tx);
        cleanup_file(path);
    }

    #[test]
    fn remove_primary_index_fails() {
        let path = "test-files/remove_primary_index.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let mut table = TableBuilder::new()
            .name("protect_primary")
            .id(17)
            .pkey(1)
            .add_col("id", TypeCol::INTEGER)
            .build(&mut tx)
            .unwrap();

        let result = table.remove_index("id");
        assert!(result.is_err());
        assert_eq!(table.indices.len(), 1);

        let _ = db.commit(tx);
        cleanup_file(path);
    }

    #[test]
    fn remove_empty_index_name_fails() {
        let path = "test-files/remove_empty_index_name.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let mut table = TableBuilder::new()
            .name("empty_remove")
            .id(18)
            .pkey(1)
            .add_col("id", TypeCol::INTEGER)
            .build(&mut tx)
            .unwrap();

        let result = table.remove_index("");
        assert!(result.is_err());

        let _ = db.commit(tx);
        cleanup_file(path);
    }

    #[test]
    fn record_encoding_with_secondary_index() {
        let path = "test-files/record_encoding_secondary.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let mut table = TableBuilder::new()
            .name("record_test")
            .id(19)
            .pkey(1)
            .add_col("id", TypeCol::INTEGER)
            .add_col("status", TypeCol::BYTES)
            .add_col("description", TypeCol::BYTES)
            .build(&mut tx)
            .unwrap();

        table.create_index("status").unwrap();
        table.add_col_to_index("status", "status").unwrap();

        let rec = Record::new()
            .add(42i64)
            .add("active")
            .add("test description");

        let kv_pairs: Vec<_> = rec.encode(&table).unwrap().collect();

        // Should have 2 key-value pairs (primary + secondary)
        assert_eq!(kv_pairs.len(), 2);

        // Primary key
        assert_eq!(kv_pairs[0].0.to_string(), "19 0 42");
        assert_eq!(kv_pairs[0].1.to_string(), "active test description");

        // Secondary key (status index)
        assert_eq!(kv_pairs[1].0.to_string(), "19 1 active 42");
        assert_eq!(kv_pairs[1].1.to_string(), "test description");

        let _ = db.commit(tx);
        cleanup_file(path);
    }

    #[test]
    fn record_encoding_with_multiple_secondary_indices() {
        let path = "test-files/record_encoding_multi_secondary.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let mut table = TableBuilder::new()
            .name("multi_idx")
            .id(20)
            .pkey(1)
            .add_col("id", TypeCol::INTEGER)
            .add_col("name", TypeCol::BYTES)
            .add_col("city", TypeCol::BYTES)
            .add_col("age", TypeCol::INTEGER)
            .build(&mut tx)
            .unwrap();

        table.create_index("name").unwrap();
        table.create_index("city").unwrap();
        table.add_col_to_index("name", "name").unwrap();
        table.add_col_to_index("city", "city").unwrap();

        let rec = Record::new().add(1i64).add("Alice").add("NYC").add(30i64);

        let kv_pairs: Vec<_> = rec.encode(&table).unwrap().collect();

        // Should have 3 key-value pairs (primary + 2 secondary)
        assert_eq!(kv_pairs.len(), 3);

        // Primary key
        assert_eq!(kv_pairs[0].0.to_string(), "20 0 1");
        assert_eq!(kv_pairs[0].1.to_string(), "Alice NYC 30");

        // Secondary index on name
        assert_eq!(kv_pairs[1].0.to_string(), "20 1 Alice 1");
        assert_eq!(kv_pairs[1].1.to_string(), "NYC 30");

        // Secondary index on city
        assert_eq!(kv_pairs[2].0.to_string(), "20 2 NYC 1");
        assert_eq!(kv_pairs[2].1.to_string(), "Alice 30");

        let _ = db.commit(tx);
        cleanup_file(path);
    }

    #[test]
    fn add_and_remove_multiple_indices_sequentially() {
        let path = "test-files/add_remove_sequential.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let mut table = TableBuilder::new()
            .name("sequential_test")
            .id(21)
            .pkey(1)
            .add_col("id", TypeCol::INTEGER)
            .add_col("col_a", TypeCol::BYTES)
            .add_col("col_b", TypeCol::BYTES)
            .add_col("col_c", TypeCol::BYTES)
            .build(&mut tx)
            .unwrap();

        // Add indices
        table.create_index("col_a").unwrap();
        table.create_index("col_b").unwrap();
        table.create_index("col_c").unwrap();
        table.add_col_to_index("col_a", "col_a").unwrap();
        table.add_col_to_index("col_b", "col_b").unwrap();
        table.add_col_to_index("col_c", "col_c").unwrap();
        assert_eq!(table.indices.len(), 4);

        // Remove middle index
        table.remove_index("col_b").unwrap();
        assert_eq!(table.indices.len(), 3);
        assert!(
            table
                .indices
                .iter()
                .find(|idx| idx.name == "col_b")
                .is_none()
        );
        assert!(
            table
                .indices
                .iter()
                .find(|idx| idx.name == "col_a")
                .is_some()
        );
        assert!(
            table
                .indices
                .iter()
                .find(|idx| idx.name == "col_c")
                .is_some()
        );

        // Remove first secondary index
        table.remove_index("col_a").unwrap();
        assert_eq!(table.indices.len(), 2);

        // Remove last secondary index
        table.remove_index("col_c").unwrap();
        assert_eq!(table.indices.len(), 1);
        assert_eq!(table.indices[0].kind, IdxKind::Primary);

        let _ = db.commit(tx);
        cleanup_file(path);
    }

    #[test]
    fn index_prefix_values_correct() {
        let path = "test-files/index_prefix_values.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let mut table = TableBuilder::new()
            .name("prefix_test")
            .id(22)
            .pkey(1)
            .add_col("id", TypeCol::INTEGER)
            .add_col("a", TypeCol::BYTES)
            .add_col("b", TypeCol::BYTES)
            .add_col("c", TypeCol::BYTES)
            .add_col("d", TypeCol::BYTES)
            .build(&mut tx)
            .unwrap();

        // Primary key should have prefix 0
        assert_eq!(table.indices[0].prefix, 0);

        // Add secondary indices and verify prefix increments
        table.create_index("a").unwrap();
        table.add_col_to_index("a", "a").unwrap();
        assert_eq!(table.indices[1].prefix, 1);

        table.create_index("b").unwrap();
        table.add_col_to_index("b", "b").unwrap();
        assert_eq!(table.indices[2].prefix, 2);

        table.create_index("c").unwrap();
        table.add_col_to_index("c", "c").unwrap();
        assert_eq!(table.indices[3].prefix, 3);

        table.create_index("d").unwrap();
        table.add_col_to_index("d", "d").unwrap();
        assert_eq!(table.indices[4].prefix, 4);

        let _ = db.commit(tx);
        cleanup_file(path);
    }

    #[test]
    fn table_serialization_with_indices() {
        let path = "test-files/table_serialization_indices.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let mut table = TableBuilder::new()
            .name("serialized_table")
            .id(23)
            .pkey(1)
            .add_col("id", TypeCol::INTEGER)
            .add_col("email", TypeCol::BYTES)
            .add_col("name", TypeCol::BYTES)
            .build(&mut tx)
            .unwrap();

        table.create_index("email").unwrap();
        table.add_col_to_index("email", "email").unwrap();

        // Serialize
        let encoded = table.encode().unwrap();

        // Deserialize
        let decoded = Table::decode(encoded.into()).unwrap();

        // Verify structure is preserved
        assert_eq!(decoded.name, "serialized_table");
        assert_eq!(decoded.id, 23);
        assert_eq!(decoded.indices.len(), 2);
        assert_eq!(decoded.indices[0].kind, IdxKind::Primary);
        assert_eq!(decoded.indices[1].kind, IdxKind::Secondary);
        assert_eq!(decoded.indices[1].name, "email");

        let _ = db.commit(tx);
        cleanup_file(path);
    }

    #[test]
    fn index_columns_match_table_structure() {
        let path = "test-files/index_columns_match.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let mut table = TableBuilder::new()
            .name("columns_match")
            .id(24)
            .pkey(1)
            .add_col("id", TypeCol::INTEGER)
            .add_col("first_name", TypeCol::BYTES)
            .add_col("last_name", TypeCol::BYTES)
            .add_col("age", TypeCol::INTEGER)
            .build(&mut tx)
            .unwrap();

        table.create_index("first_name").unwrap();
        table.create_index("age").unwrap();
        table.add_col_to_index("first_name", "first_name").unwrap();
        table.add_col_to_index("age", "age").unwrap();

        // Find first_name index (column 1)
        let first_name_idx = table
            .indices
            .iter()
            .find(|idx| idx.name == "first_name")
            .unwrap();
        assert_eq!(first_name_idx.columns, vec![1]);

        // Find age index (column 3)
        let age_idx = table.indices.iter().find(|idx| idx.name == "age").unwrap();
        assert_eq!(age_idx.columns, vec![3]);

        let _ = db.commit(tx);
        cleanup_file(path);
    }
}
