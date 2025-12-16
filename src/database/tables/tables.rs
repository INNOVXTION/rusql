use std::{collections::HashMap, rc::Rc};

use crate::database::{
    errors::TableError,
    pager::diskpager::KVEngine,
    tables::records::{Record, Value},
};
use serde::{Deserialize, Serialize};

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
 * User Input -> DataCell -> Record -> Key, Value
 *
 */

// fixed table which holds all the schemas
const DEF_TABLE_NAME: &'static str = "tdef";
const DEF_TABLE_COL1: &'static str = "tname";
const DEF_TABLE_COL2: &'static str = "def";
const DEF_TABLE_ID: u64 = 1;
const DEF_TABLE_PKEYS: u16 = 1;

// fixed meta table holding all the unique table ids
const META_TABLE_NAME: &'static str = "tmeta";
const META_TABLE_COL1: &'static str = "tname";
const META_TABLE_COL2: &'static str = "tid";
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
                    data_type: TypeCol::BYTES,
                },
            ],
            pkeys: META_TABLE_PKEYS,
        })
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
        })
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

    /// default 0
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

    // TODO: extend validation
    pub fn build(self) -> Result<Table, TableError> {
        let name = match self.name {
            Some(n) => n,
            None => return Err(TableError::TableError("invalid name".to_string())),
        };
        // TODO: check meta table for unique id
        let id = match self.id {
            Some(id) => id,
            None => return Err(TableError::TableError("invalid id".to_string())),
        };
        let cols = self.cols;
        let pkeys = match self.pkeys {
            Some(pk) => pk,
            None => return Err(TableError::TableError("invalid pkeys".to_string())),
        };
        Ok(Table {
            name,
            id,
            cols,
            pkeys,
        })
    }
}

// serialize to json
#[derive(Serialize, Deserialize)]
pub(crate) struct Table {
    name: String,
    id: u64,
    cols: Vec<Column>,
    pkeys: u16,
}

struct EncodedTable(String);

impl EncodedTable {
    pub fn decode(self) -> Table {
        let table = serde_json::from_str(&self.0).unwrap();
        table
    }
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl Table {
    /// deprecated, go through builder
    pub fn new(name: &str) -> Self {
        Table {
            name: name.to_string(),
            id: 0,
            cols: vec![],
            pkeys: 0,
        }
    }

    /// encode schema to json
    pub fn encode(self) -> EncodedTable {
        EncodedTable(serde_json::to_string(&self).unwrap())
    }

    pub fn cols(&self) -> &[Column] {
        &self.cols[..]
    }
    pub fn id(&self) -> u64 {
        self.id
    }
    pub fn pkeys(&self) -> u16 {
        self.pkeys
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Column {
    pub title: String,
    pub data_type: TypeCol,
}

#[derive(Serialize, Deserialize, Debug)]
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

struct Database<KV: KVEngine> {
    tdef: TDefTable,
    mtab: MetaTable,
    buffer: HashMap<String, Table>,
    kv_engine: KV,
}

impl<KV: KVEngine> Database<KV> {
    fn new(pager: KV) -> Self {
        Database {
            tdef: TDefTable::new(),
            mtab: MetaTable::new(),
            buffer: HashMap::new(),
            kv_engine: pager,
        }
    }
}

// internal API
trait TableInterface {
    type KVStore: KVEngine;

    fn insert_table();
    fn get_table();
}

// outward API
trait DatabaseAPI<DB>
where
    DB: TableInterface,
{
    type DB: TableInterface;

    fn create_table(&self);
    fn drop_table(&self);

    fn get(&self);
    fn insert(&self);
    fn update(&self);
    fn delete(&self);
}

#[cfg(test)]
mod test {
    use crate::database::pager::{EnvoyV1, diskpager::Envoy, mempage_tree};

    use super::*;
    use crate::database::helper::cleanup_file;
    use test_log::test;
    use tracing::{Level, info, span};

    #[test]
    fn meta_page() {
        let path = "table1.rdb";
        cleanup_file(path);
        let pager = Envoy::new(path);
        let db = Database::new(pager);
        cleanup_file(path);
    }
}
