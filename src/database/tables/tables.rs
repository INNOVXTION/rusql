use std::rc::Rc;

use crate::database::{pager::diskpager::KVEngine, types::Pointer};
use serde::{Deserialize, Serialize};

// naming for tdef table
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

// serialize to json
#[derive(Serialize, Deserialize)]
struct Table {
    name: String,
    id: u64,
    cols: Vec<Column>,
    pkeys: u16,
}

impl Table {
    fn new(name: &str) -> Self {
        Table {
            name: name.to_string(),
            id: 0,
            cols: vec![],
            pkeys: 0,
        }
    }

    fn tdef_new() -> Self {
        Table {
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
        }
    }

    fn meta_new() -> Self {
        Table {
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
        }
    }

    // decode a table from disk
    fn decode(ptr: Pointer) -> Self {
        todo!()
    }

    // encode table to disk
    fn encode(self) {
        todo!()
    }
}

#[derive(Serialize, Deserialize)]
struct Column {
    title: String,
    data_type: TypeCol,
}

#[derive(Serialize, Deserialize)]
enum TypeCol {
    BYTES = 1,
    INTEGER = 2,
}

struct Record {
    key: Vec<DataCell>,
    val: Vec<DataCell>,
}

enum DataCell {
    Str(String),
    Int(i64),
}

trait TableInterface {
    fn insert_record();
    fn insert_table();
    fn lookup_table();
}

trait DataCodec {
    fn encode(data: &DataCell) -> Rc<[u8]>;
    fn decode(data: &[u8]) -> DataCell;
}

trait TableAPI {
    type KVStore: KVEngine;

    fn create_table(pager: &Self::KVStore);
    fn drop_table(pager: &Self::KVStore);

    fn get(pager: &Self::KVStore);
    fn insert(pager: &Self::KVStore);
    fn update(pager: &Self::KVStore);
    fn delete(pager: &Self::KVStore);
}

#[cfg(test)]
mod test {
    use crate::database::pager::EnvoyV1;

    use super::*;
    use test_log::test;
    use tracing::{Level, info, span};

    #[test]
    fn meta_page() {
        let tdef = Table::tdef_new();
        let meta = Table::meta_new();
        let envoy = EnvoyV1::open("table1.rdb").unwrap();
    }
}
