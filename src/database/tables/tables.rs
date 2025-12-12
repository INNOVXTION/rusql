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

/*
 * Encoding Layout:
 * |-----------KEY----------|----Value-----|
 * |          [Col1][Col2]..|[Col3][Col4]..|
 * |[TABLE ID][PK1 ][PK2 ]..|[ v1 ][ v2 ]..|
 *
 * Tdef:
 * |-------KEY--------|---Val----|
 * |       [   Col1  ]|[  Col2  ]|
 * |[TID:1][PK1: name]|[  def   ]|
 */

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

    fn new_tdef() -> Self {
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

    fn new_meta() -> Self {
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

    // some function to add an auto incremending id
    fn set_id() {}

    // decode a table from disk
    fn decode(ptr: Pointer) -> Self {
        todo!()
    }

    fn to_json(self) -> serde_json::Value {
        serde_json::to_value(self).unwrap()
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

impl DataCell {
    fn into_bytes(self) -> Rc<[u8]> {
        match self {
            DataCell::Str(s) => {
                let len = s.len();
                let mut buf = Vec::<u8>::with_capacity(len + 4);
                buf[..4].copy_from_slice(&(len as u32).to_be_bytes());
                buf[4..].copy_from_slice(&s.into_bytes());
                Rc::from(buf)
            }
            DataCell::Int(i) => Rc::from(i.to_le_bytes()),
        }
    }
    fn from_bytes(data: &[u8], encode_type: TypeCol) -> DataCell {
        match encode_type {
            TypeCol::BYTES => {
                let mut buf = [0u8; 4];
                buf.copy_from_slice(&data[..4]);
                let len = u32::from_le_bytes(buf) as usize;
                assert_eq!(data.len(), len + 4);
                // SAFETY: we encode in UTF-8
                DataCell::Str(unsafe { String::from_utf8_unchecked(data[4..4 + len].to_vec()) })
            }
            TypeCol::INTEGER => {
                assert_eq!(data.len(), 8);
                let mut buf = [0u8; 8];
                buf.copy_from_slice(data);
                DataCell::Int(i64::from_le_bytes(buf))
            }
        }
    }
}

trait TableInterface {
    type KVStore: KVEngine;

    fn insert_row(records: &[Record], schema: &Table, pager: &Self::KVStore);

    fn insert_table(table: Table, pager: &Self::KVStore);

    fn lookup_table(id: u64, pager: &Self::KVStore) -> Table;
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
    use crate::database::pager::{EnvoyV1, mempage_tree};

    use super::*;
    use crate::database::helper::cleanup_file;
    use test_log::test;
    use tracing::{Level, info, span};

    #[test]
    fn meta_page() {
        let path = "table1.rdb";
        cleanup_file(path);
        let tdef = Table::new_tdef();
        let meta = Table::new_meta();
        let envoy = EnvoyV1::open(path).unwrap();
    }

    #[test]
    fn json_test() {
        let tree = mempage_tree();
        let tdef = Table::new_tdef();
        let meta = Table::new_meta();
    }
}
