use std::cmp::min;
use std::rc::Rc;
use std::{cmp::Ordering, fmt::Write};

use super::codec::*;
use crate::database::{
    errors::TableError,
    tables::{
        codec::{STR_PRE_LEN, TID_LEN, TYPE_LEN},
        tables::{Table, TypeCol},
    },
};

/// encoded and parsed key for the pager, should only be created from encoding a record
///
// ----------------------KEY--------------------------|
// [ TID][      INT      ][            STR           ]|
// [ 8B ][1B TYPE][8B INT][1B TYPE][2B STRLEN][nB STR]|
#[derive(Debug)]
pub(crate) struct Key(Rc<[u8]>);

impl Key {
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

    pub fn get_tid(&self) -> u64 {
        u64::from_le_bytes(self.0[..TID_LEN].try_into().unwrap())
    }

    /// creates key from properly encoded data
    fn from_slice(data: &[u8]) -> Self {
        Key(Rc::from(data))
    }
}

impl Eq for Key {}

impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        if let Ordering::Equal = self.cmp(other) {
            return true;
        }
        false
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.get_tid().cmp(&other.get_tid()) {
            Ordering::Equal => {}
            o => return o,
        }
        let mut key_a = &self.0[TID_LEN..]; // self
        let mut key_b = &other.0[TID_LEN..];

        loop {
            if key_a.is_empty() && key_b.is_empty() {
                return Ordering::Equal;
            }
            if key_a.is_empty() {
                return Ordering::Less;
            }
            if key_b.is_empty() {
                return Ordering::Greater;
            }

            match TypeCol::from_u8(read_u8(&mut key_a)) {
                Some(TypeCol::BYTES) => {
                    let len_a = read_u32(&mut key_a) as usize;
                    let len_b = read_u32(&mut key_a) as usize;
                    let min = min(len_a, len_b);

                    match key_a[..min].cmp(&key_b[..min]) {
                        Ordering::Equal => {
                            key_a = &key_a[len_a..];
                            key_b = &key_b[len_b..];
                        }
                        o => return o,
                    }
                }
                Some(TypeCol::INTEGER) => {
                    // flipping the sign bit for comparison
                    let int_a = read_i64(&mut key_a) as u64 ^ 0x8000_0000_0000_0000;
                    let int_b = read_i64(&mut key_b) as u64 ^ 0x8000_0000_0000_0000;

                    match int_a.cmp(&int_b) {
                        Ordering::Equal => {}
                        o => return o,
                    }
                }
                None => unreachable!(),
            }
        }
    }
}

pub(crate) struct KeyIter {
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

pub(crate) struct Value(Rc<[u8]>);

impl Value {
    pub fn from_slice(data: &[u8]) -> Self {
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

pub(crate) struct ValueIter {
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

pub(crate) struct Record {
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

    fn decode(data: (Key, Value)) -> Self {
        let mut rec = Record::new();
        rec.data.extend(data.0.into_iter());
        rec.data.extend(data.1.into_iter());
        rec
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

#[cfg(test)]
mod test {

    use crate::database::tables::tables::{Column, Table};

    use super::*;
    use test_log::test;
    use tracing::{Level, info, span};

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
