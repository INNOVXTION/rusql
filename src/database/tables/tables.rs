use std::fmt::Write;
use std::{collections::HashMap, rc::Rc};

use crate::database::tables::codec::TID_LEN;
use crate::database::{
    errors::TableError,
    pager::diskpager::KVEngine,
    tables::codec::{Codec, INT_LEN, STR_PRE_LEN, TYPE_LEN},
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

#[repr(u8)]
#[derive(Serialize, Deserialize)]
pub(crate) enum TypeCol {
    BYTES = 1,
    INTEGER = 2,
}

impl TypeCol {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(TypeCol::BYTES),
            2 => Some(TypeCol::INTEGER),
            _ => None,
        }
    }
}

/// encoded and parsed key for the pager, should only be created from encoding a record
///
// ----------------------KEY--------------------------|
// [ TID][      INT      ][            STR           ]|
// [ 8B ][1B TYPE][8B INT][1B TYPE][2B STRLEN][nB STR]|
struct Key(Rc<[u8]>);

impl Key {
    /// creates key from properly encoded data
    fn from_slice(data: &[u8]) -> Self {
        Key(Rc::from(data))
    }

    /// turns key into string
    pub fn into_string(self) -> String {
        let mut st = String::new();
        write!(st, "{}", self.get_tid()).unwrap();
        for cell in self {
            match cell {
                DataCell::Str(s) => write!(st, "{}", s).unwrap(),
                DataCell::Int(i) => write!(st, "{}", i).unwrap(),
            };
        }
        st
    }

    fn get_tid(&self) -> u64 {
        u64::from_le_bytes(self.0[..TID_LEN].try_into().unwrap())
    }
}

struct KeyIter {
    data: Key,
    count: usize,
}

impl Iterator for KeyIter {
    type Item = DataCell;

    fn next(&mut self) -> Option<Self::Item> {
        if self.data.0.len() == self.count {
            return None;
        }
        match TypeCol::from_u8(self.data.0[self.count]) {
            Some(TypeCol::BYTES) => {
                let str = String::decode(&self.data.0[self.count..]);
                self.count += TYPE_LEN + STR_PRE_LEN + str.len();
                Some(DataCell::Str(str))
            }
            Some(TypeCol::INTEGER) => {
                let int = i64::decode(&self.data.0[self.count..]);
                self.count += TYPE_LEN + INT_LEN;
                Some(DataCell::Int(int))
            }
            None => None,
        }
    }
}

impl IntoIterator for Key {
    type Item = DataCell;
    type IntoIter = KeyIter;

    fn into_iter(self) -> Self::IntoIter {
        KeyIter {
            data: self,
            count: TID_LEN, // skipping the table id
        }
    }
}
struct Value(Rc<[u8]>);

impl Value {
    fn from_slice(data: &[u8]) -> Self {
        Value(Rc::from(data))
    }
    /// turns value into string
    pub fn into_string(self) -> String {
        let mut st = String::new();
        for cell in self {
            match cell {
                DataCell::Str(s) => write!(st, "{}", s).unwrap(),
                DataCell::Int(i) => write!(st, "{}", i).unwrap(),
            };
        }
        st
    }
}

struct ValueIter {
    data: Value,
    count: usize,
}

impl Iterator for ValueIter {
    type Item = DataCell;

    fn next(&mut self) -> Option<Self::Item> {
        if self.data.0.len() == self.count {
            return None;
        }
        match TypeCol::from_u8(self.data.0[self.count]) {
            Some(TypeCol::BYTES) => {
                let str = String::decode(&self.data.0[self.count..]);
                self.count += TYPE_LEN + STR_PRE_LEN + str.len();
                Some(DataCell::Str(str))
            }
            Some(TypeCol::INTEGER) => {
                let int = i64::decode(&self.data.0[self.count..]);
                self.count += TYPE_LEN + INT_LEN;
                Some(DataCell::Int(int))
            }
            None => None,
        }
    }
}

impl IntoIterator for Value {
    type Item = DataCell;
    type IntoIter = ValueIter;

    fn into_iter(self) -> Self::IntoIter {
        ValueIter {
            data: self,
            count: 0,
        }
    }
}

struct Record {
    data: Vec<DataCell>,
}

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
    fn encode(self, schema: &Table) -> Result<(Key, Value), TableError> {
        if schema.cols.len() != self.data.len() {
            return Err(TableError::RecordError(
                "input doesnt match column count".to_string(),
            ));
        }

        let mut buf = Vec::<u8>::new();
        let mut idx: usize = 0;
        let mut key_idx: usize = 0;

        // starting with table id
        buf.extend_from_slice(&schema.id.to_le_bytes());
        idx += TID_LEN;

        // composing byte array by iterating through all columns design ated as primary key
        for col in schema.cols.iter().enumerate() {
            // remembering the cutoff point between keys and values
            if col.0 == schema.pkeys as usize {
                key_idx = idx;
            }
            match col.1.data_type {
                TypeCol::BYTES => {
                    if let DataCell::Str(s) = &self.data[col.0] {
                        let len = TYPE_LEN + STR_PRE_LEN + s.len();
                        buf.extend_from_slice(&s.encode());
                        idx += len;
                    } else {
                        return Err(TableError::RecordError("expected str, got int".to_string()));
                    }
                }
                TypeCol::INTEGER => {
                    if let DataCell::Int(i) = &self.data[col.0] {
                        let len = TYPE_LEN + INT_LEN;
                        buf.extend_from_slice(&i.encode());
                        idx += len;
                    } else {
                        return Err(TableError::RecordError("expected int, got str".to_string()));
                    }
                }
            }
        }
        Ok((
            Key::from_slice(&buf[..key_idx]),
            Value::from_slice(&buf[key_idx..]),
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

impl From<DataCell> for String {
    fn from(value: DataCell) -> Self {
        match value {
            DataCell::Str(s) => s,
            DataCell::Int(i) => i.to_string(),
        }
    }
}

impl From<String> for DataCell {
    fn from(value: String) -> Self {
        DataCell::Str(value)
    }
}

impl From<&str> for DataCell {
    fn from(value: &str) -> Self {
        DataCell::Str(value.to_string())
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

    #[test]
    fn record1() {
        let mut table = Table::new("mytable");

        let mut cols = Vec::<Column>::new();
        cols.push(Column {
            title: "greeter".into(),
            data_type: TypeCol::BYTES,
        });
        cols.push(Column {
            title: "number".into(),
            data_type: TypeCol::INTEGER,
        });
        cols.push(Column {
            title: "greetee".into(),
            data_type: TypeCol::BYTES,
        });

        table.id = 2;
        table.cols = cols;
        table.pkeys = 2;

        let s1 = "hello";
        let i1 = 10;
        let s2 = "world";

        let mut rec = Record::new();
        rec.push(s1.into());
        rec.push(i1.into());
        rec.push(s2.into());

        let (key, value) = rec.encode(&table).unwrap();
        assert_eq!(key.into_string(), "2hello10");
        assert_eq!(value.into_string(), "world");
    }
}
