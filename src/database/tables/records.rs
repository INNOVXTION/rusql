use std::cmp::min;
use std::rc::Rc;
use std::{cmp::Ordering, fmt::Write};

use tracing::debug;

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

    // reads the first 8 bytes
    pub fn get_tid(&self) -> u64 {
        u64::from_le_bytes(self.0[..TID_LEN].try_into().unwrap())
    }

    /// its up to the caller to ensure the data is properly formatted
    fn from_slice(data: &[u8]) -> Self {
        Key(Rc::from(data))
    }
}

impl Eq for Key {}

impl PartialEq<Key> for Key {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl PartialOrd<Key> for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.get_tid().cmp(&other.get_tid()) {
            Ordering::Equal => {
                debug!("table id is equal");
            }
            o => return o,
        }
        let mut key_a = &self.0[TID_LEN..];
        let mut key_b = &other.0[TID_LEN..];

        loop {
            if key_a.is_empty() && key_b.is_empty() {
                debug!("both keys empty");
                return Ordering::Equal;
            }
            if key_a.is_empty() {
                debug!("key a empty");
                return Ordering::Less;
            }
            if key_b.is_empty() {
                debug!("key b empty");
                return Ordering::Greater;
            }

            // advancing the type bit
            let ta = key_a.read_u8();
            let tb = key_b.read_u8();

            debug_assert_eq!(ta, tb);
            match ta.cmp(&tb) {
                Ordering::Equal => {}
                o => return o,
            }

            match TypeCol::from_u8(ta) {
                Some(TypeCol::BYTES) => {
                    let len_a = key_a.read_u32() as usize;
                    let len_b = key_b.read_u32() as usize;
                    let min = min(len_a, len_b);

                    debug!(len_a, len_b, min, "comparing strings...");
                    match key_a[..min].cmp(&key_b[..min]) {
                        Ordering::Equal => {
                            debug!("strings are equal");
                            key_a = &key_a[len_a..];
                            key_b = &key_b[len_b..];
                        }
                        o => return o,
                    }
                }
                Some(TypeCol::INTEGER) => {
                    // flipping the sign bit for comparison
                    let int_a = key_a.read_i64() as u64 ^ 0x8000_0000_0000_0000;
                    let int_b = key_b.read_i64() as u64 ^ 0x8000_0000_0000_0000;
                    debug!(int_a, int_b, "comparing integer");
                    match int_a.cmp(&int_b) {
                        Ordering::Equal => {
                            debug!("integer are equal");
                        }
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
    pub fn decode(self) -> Record {
        let mut r = Record::new();
        for cell in self {
            r.add(cell);
        }
        r
    }
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

/// prelude list of data
pub(crate) struct Record {
    data: Vec<DataCell>,
}

impl Record {
    fn new() -> Self {
        Record { data: vec![] }
    }

    fn add<T: InputData>(&mut self, data: T) -> &mut Self {
        self.data.push(data.into_cell());
        self
    }

    /// encodes a Record according to a schema into a key value pair starting with table id
    ///
    /// validates that record matches with column in schema
    fn encode(&mut self, schema: &Table) -> Result<(Key, Value), TableError> {
        if schema.cols().len() != self.data.len() {
            return Err(TableError::RecordError(
                "input doesnt match column count".to_string(),
            ));
        }

        let mut buf = Vec::<u8>::new();
        let mut idx: usize = 0;
        let mut key_idx: usize = 0;

        // starting with table id
        buf.extend_from_slice(&schema.id().to_le_bytes());
        idx += TID_LEN;

        // composing byte array by iterating through all columns designated as primary key
        for col in schema.cols().iter().enumerate() {
            // remembering the cutoff point between keys and values
            if col.0 == schema.pkeys() as usize {
                key_idx = idx;
            }
            match col.1.data_type {
                TypeCol::BYTES => {
                    if let DataCell::Str(s) = &self.data[col.0] {
                        let s = s.encode();
                        idx += s.len();
                        buf.extend_from_slice(&s);
                        debug!(idx, "encoding string");
                    } else {
                        return Err(TableError::RecordError("expected str, got int".to_string()));
                    }
                }
                TypeCol::INTEGER => {
                    if let DataCell::Int(i) = &self.data[col.0] {
                        let i = i.encode();
                        idx += i.len();
                        buf.extend_from_slice(&i);
                        debug!(idx, "encoding integer");
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

trait InputData {
    fn into_cell(self) -> DataCell;
}

impl InputData for DataCell {
    fn into_cell(self) -> DataCell {
        self
    }
}
impl InputData for String {
    fn into_cell(self) -> DataCell {
        DataCell::Str(self)
    }
}

impl InputData for &str {
    fn into_cell(self) -> DataCell {
        DataCell::Str(self.to_string())
    }
}

impl InputData for i64 {
    fn into_cell(self) -> DataCell {
        DataCell::Int(self)
    }
}

impl InputData for i32 {
    fn into_cell(self) -> DataCell {
        DataCell::Int(self as i64)
    }
}

impl InputData for i16 {
    fn into_cell(self) -> DataCell {
        DataCell::Int(self as i64)
    }
}

impl InputData for i8 {
    fn into_cell(self) -> DataCell {
        DataCell::Int(self as i64)
    }
}

#[cfg(test)]
mod test {

    use super::super::tables::TableBuilder;
    use super::*;
    use test_log::test;
    use tracing::{Level, info, span};

    #[test]
    fn record1() {
        let table = TableBuilder::new()
            .name("mytable")
            .id(2)
            .pkey(2)
            .add_col("greeter", TypeCol::BYTES)
            .add_col("number", TypeCol::INTEGER)
            .add_col("gretee", TypeCol::BYTES)
            .build()
            .unwrap();

        let s1 = "hello";
        let i1 = 10;
        let s2 = "world";

        let mut rec = Record::new();
        rec.add(s1);
        rec.add(i1);
        rec.add(s2);

        let (key, value) = rec.encode(&table).unwrap();
        assert_eq!(key.into_string(), "2hello10");
        assert_eq!(value.into_string(), "world");
    }

    #[test]
    fn key_cmp1() -> Result<(), Box<dyn std::error::Error>> {
        let span = span!(Level::DEBUG, "cmp span");
        let _guard = span.enter();

        let table = TableBuilder::new()
            .name("mytable")
            .id(2)
            .pkey(2)
            .add_col("greeter", TypeCol::BYTES)
            .add_col("number", TypeCol::INTEGER)
            .add_col("gretee", TypeCol::BYTES)
            .build()?;

        let (key1, value1) = Record::new()
            .add("hello")
            .add(10)
            .add("world")
            .encode(&table)?;

        let (key2, value2) = Record::new()
            .add("hello")
            .add(10)
            .add("world")
            .encode(&table)?;

        assert_eq!(key1, key2);

        let (key3, value3) = Record::new()
            .add("smol")
            .add(5)
            .add("world")
            .encode(&table)?;

        assert!(key1 < key3);
        Ok(())
    }
}
