use std::{collections::HashMap, hash::Hash, rc::Rc};

use crate::database::{
    errors::TableError,
    pager::diskpager::KVEngine,
    tables::codec::{INT_LEN, IntegerCodec, STR_PRE_LEN, StringCodec},
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

struct TableBuffer<P: KVEngine> {
    tables: TDefTable,
    pager: P,
}

struct MetaTable(Table);

impl MetaTable {
    fn new_meta() -> Self {
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

    // some function to add an auto incremending id
    fn set_id() {}

    /// encode schema to json
    fn encode(self) -> Value {
        Value(Rc::from(serde_json::to_string(&self).unwrap().into_bytes()))
    }
}

#[derive(Serialize, Deserialize)]
struct Column {
    title: String,
    data_type: TypeCol,
}

#[derive(Serialize, Deserialize)]
pub(crate) enum TypeCol {
    BYTES = 1,
    INTEGER = 2,
}

struct Record {
    data: Vec<DataCell>,
}

struct Key(Rc<[u8]>);
struct Value(Rc<[u8]>);

impl Record {
    fn new() -> Self {
        Record { data: vec![] }
    }

    fn push(&mut self, data: DataCell) {
        self.data.push(data)
    }

    fn push_strings(&mut self, data: &[&str]) {
        for s in data {
            self.push(s.to_string().into());
        }
    }

    /// encodes a Record according to a schema into a key value pair starting with table id
    ///
    /// validates that record matches with column in schema
    fn encode(self, schema: &Table) -> Result<(Key, Value), TableError<impl Into<String>>> {
        if schema.cols.len() != self.data.len() {
            return Err(TableError::RecordError("input doesnt match column count"));
        }

        let mut buf = Vec::<u8>::new();
        let mut idx: usize = 0;
        let mut key_idx: usize = 0;

        // starting with table id
        buf.extend_from_slice(&schema.id.to_le_bytes());
        idx += 8;

        // composing byte array by iterating through all columns design ated as primary key
        for col in schema.cols.iter().enumerate() {
            // remembering the cutoff point between keys and values
            if col.0 == schema.pkeys as usize {
                key_idx = idx;
            }
            match col.1.data_type {
                TypeCol::BYTES => {
                    if let DataCell::Str(s) = &self.data[col.0] {
                        let len = s.len() + STR_PRE_LEN;
                        buf.extend_from_slice(&s.encode());
                        idx += len;
                    } else {
                        return Err(TableError::RecordError("expected str, got int"));
                    }
                }
                TypeCol::INTEGER => {
                    if let DataCell::Str(s) = &self.data[col.0] {
                        let len = INT_LEN;
                        buf.extend_from_slice(&s.encode());
                        idx += len;
                    } else {
                        return Err(TableError::RecordError("expected int, got str"));
                    }
                }
            }
        }
        Ok((
            Key(Rc::from(&buf[..key_idx])),
            Value(Rc::from(&buf[key_idx..])),
        ))
    }
}

pub(crate) enum DataCell {
    Str(String),
    Int(i64),
}

impl DataCell {
    /// turns data cell into encoded byte array according to Codec
    ///
    /// for a string: 1B Type + 2B len + str.len()
    fn into_bytes(self) -> Rc<[u8]> {
        match self {
            DataCell::Str(s) => s.encode(),
            DataCell::Int(i) => i.encode(),
        }
    }
    fn from_bytes(data: &[u8], encode_type: TypeCol) -> DataCell {
        match encode_type {
            TypeCol::BYTES => DataCell::Str(String::decode(data)),
            TypeCol::INTEGER => DataCell::Int(i64::decode(data)),
        }
    }
}

impl From<String> for DataCell {
    fn from(value: String) -> Self {
        DataCell::Str(value)
    }
}

impl From<i64> for DataCell {
    fn from(value: i64) -> Self {
        DataCell::Int(value)
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
    fn insert_row(records: &[Record], schema: &Table, pager: &Self::KVStore);
    fn insert_table(table: Table, pager: &Self::KVStore);
    fn get_table(id: u64, pager: &Self::KVStore) -> Table;
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
