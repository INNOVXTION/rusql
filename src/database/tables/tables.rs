use std::collections::HashMap;

use crate::database::{
    pager::diskpager::KVEngine,
    tables::records::{Record, Value},
};
use serde::{Deserialize, Serialize};

// fixed table which holds all the schemas
const DEF_TABLE_NAME: &'static str = "tdef";
const DEF_TABLE_COL1: &'static str = "name";
const DEF_TABLE_COL2: &'static str = "def";
const DEF_TABLE_ID: u64 = 1;
const DEF_TABLE_PKEYS: u16 = 1;

// fixed meta table
const META_TABLE_NAME: &'static str = "tmeta";
const META_TABLE_COL1: &'static str = "key";
const META_TABLE_COL2: &'static str = "val";
const META_TABLE_ID: u64 = 2;
const META_TABLE_PKEYS: u16 = 1;

/*
 * Encoding Layout:
 * |-----------KEY----------|----Value-----|
 * |          [Col1][Col2]..|[Col3][Col4]..|
 * |[TABLE ID][PK1 ][PK2 ]..|[ v1 ][ v2 ]..|
 *
 * Tdef:
 * |-----KEY---|----Val---|
 * |   [ Col1 ]|[  Col2  ]|
 * |[1][ name ]|[  def   ]|
 */

/// wrapper for sentinal value
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

struct TableBuilder {
    name: Option<String>,
    id: Option<u64>,
    cols: Vec<Column>,
    pkeys: Option<u16>,
}

impl TableBuilder {
    fn new() -> Self {
        TableBuilder {
            name: None,
            id: None,
            cols: vec![],
            pkeys: None,
        }
    }

    fn name(&mut self, name: &str) -> &mut Self {
        self.name = Some(name.to_string());
        self
    }

    fn id(&mut self, id: u64) -> &mut Self {
        self.id = Some(id);
        self
    }

    fn add_col(&mut self, title: &str, data_type: TypeCol) -> &mut Self {
        self.cols.push(Column {
            title: title.to_string(),
            data_type,
        });
        self
    }

    fn pkey(&mut self, nkeys: u16) -> &mut Self {
        self.pkeys = Some(nkeys);
        self
    }

    fn build(&self) -> Table {
        todo!()
    }
}

// serialize to json
#[derive(Serialize, Deserialize)]
pub(crate) struct Table {
    pub(crate) name: String,
    pub(crate) id: u64,
    pub(crate) cols: Vec<Column>,
    pub(crate) pkeys: u16,
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
    pub fn encode(self) -> Value {
        Value::from_slice(&serde_json::to_string(&self).unwrap().into_bytes())
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
    buffer: HashMap<String, Table>,
    kv_engine: KV,
}

// internal API
trait TableInterface {
    type KVStore: KVEngine;

    fn init();

    fn get_record();
    fn insert_record(records: &[Record], schema: &Table, pager: &Self::KVStore);
    fn delete_record();

    fn get_table(id: u64, pager: &Self::KVStore) -> Table;
    fn insert_table(table: Table, pager: &Self::KVStore);
    fn delete_table();
}

// outward API
trait DatabaseAPI {
    type DB: TableInterface;

    fn create_table(pager: &Self::DB);
    fn drop_table(pager: &Self::DB);

    fn get(pager: &Self::DB);
    fn insert(pager: &Self::DB);
    fn update(pager: &Self::DB);
    fn delete(pager: &Self::DB);
}

#[cfg(test)]
mod test {
    use crate::database::pager::{EnvoyV1, mempage_tree};

    use super::*;
    use crate::database::helper::cleanup_file;
    use test_log::test;
    use tracing::{Level, info, span};

    #[test]
    fn meta_page() {
        let path = "table1.rdb";
        cleanup_file(path);
        let envoy = EnvoyV1::open(path).unwrap();
    }

    #[test]
    fn json_test() {
        let tree = mempage_tree();
    }
}
