use std::cmp::min;
use std::collections::HashMap;
use std::ffi::OsString;
use std::rc::Rc;
use std::{cmp::Ordering, fmt::Write};

use tracing::{debug, error};

use crate::database::codec::*;
use crate::database::tables::tables::Column;
use crate::database::types::{BTREE_MAX_KEY_SIZE, BTREE_MAX_VAL_SIZE};
use crate::database::{
    errors::TableError,
    tables::tables::{Table, TypeCol},
};

/// encoded and parsed key for the pager, should only be created from encoding a record
///
// -----------------------------KEY--------------------------------|
// [ TID ][IDX PREFIX][      INT      ][            STR           ]|
// [ 8B  ][    2B    ][1B TYPE][8B INT][1B TYPE][2B STRLEN][nB STR]|

#[derive(Debug)]
pub(crate) struct Key(Rc<[u8]>);

impl Key {
    pub fn iter(&self) -> KeyIterRef<'_> {
        KeyIterRef {
            data: self,
            count: TID_LEN,
        }
    }

    // reads the first 8 bytes
    pub fn get_tid(&self) -> u64 {
        u64::from_le_bytes(self.0[..TID_LEN].try_into().expect("this cant fail"))
    }

    /// its up to the caller to ensure the data is properly encoded
    pub fn from_encoded_slice(data: &[u8]) -> Self {
        Key(Rc::from(data))
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0[..]
    }

    /// utility function for unit tests
    /// adds TID 1
    pub fn from_unencoded_str<S: ToString>(str: S) -> Self {
        let mut buf: Vec<u8> = vec![0; 8];
        // artificial tid for testing purposes
        buf.write_u64(1);
        buf.extend_from_slice(&str.to_string().encode());
        Key(Rc::from(buf))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// turns key back into Records, doesnt return TID
    fn decode(self) -> Vec<DataCell> {
        self.into_iter().collect()
    }
}

// the following conversions should only be used for testing!
impl From<&str> for Key {
    fn from(value: &str) -> Self {
        Key::from_unencoded_str(value)
    }
}
impl From<String> for Key {
    fn from(value: String) -> Self {
        Key::from_unencoded_str(value)
    }
}
impl From<i64> for Key {
    fn from(value: i64) -> Self {
        let mut buf: Vec<u8> = vec![0; 8];
        // artificial tid for testing purposes
        buf.write_u64(1);
        buf.extend_from_slice(&value.encode());
        Key(Rc::from(buf))
    }
}

impl std::fmt::Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.get_tid())?;
        for cell in self.iter() {
            match cell {
                DataCellRef::Str(s) => write!(f, " {}", s)?,
                DataCellRef::Int(i) => write!(f, " {}", i)?,
            };
        }
        Ok(())
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
            Ordering::Equal => {}
            o => return o,
        }
        let mut key_a = &self.0[TID_LEN..];
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

            // advancing the type bit
            let ta = key_a.read_u8();
            let tb = key_b.read_u8();

            // debug_assert_eq!(ta, tb);
            match ta.cmp(&tb) {
                Ordering::Equal => {}
                o => return o,
            }

            match TypeCol::from_u8(ta) {
                Some(TypeCol::BYTES) => {
                    let len_a = key_a.read_u32() as usize;
                    let len_b = key_b.read_u32() as usize;
                    let min = min(len_a, len_b);

                    // debug!(key_a = &key_a[..min], key_b = &key_b[..min], "comparing");
                    // debug!(len_a, len_b);

                    match key_a[..min].cmp(&key_b[..min]) {
                        // comparing a tail string like "1" with "11" would return equal because for min = 1: "1" == "1"
                        // after it would move the slice up, empyting both keys, returning equal,
                        // therefore another match is needed to compare lengths
                        Ordering::Equal => match len_a.cmp(&len_b) {
                            Ordering::Equal => {
                                key_a = &key_a[len_a..];
                                key_b = &key_b[len_b..];
                            }
                            o => return o,
                        },
                        o => return o,
                    }
                }
                Some(TypeCol::INTEGER) => {
                    // flipping the sign bit for comparison
                    let int_a = key_a.read_i64() as u64 ^ 0x8000_0000_0000_0000;
                    let int_b = key_b.read_i64() as u64 ^ 0x8000_0000_0000_0000;
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

pub enum DataCellRef<'a> {
    Int(i64),
    Str(&'a str), // or &'a [u8] if you want raw bytes
}

pub(crate) struct KeyIterRef<'a> {
    data: &'a Key,
    count: usize,
}

impl<'a> Iterator for KeyIterRef<'a> {
    type Item = DataCellRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let buf = &self.data.0;

        if self.count >= self.data.0.len() {
            return None;
        }

        match TypeCol::from_u8(buf[self.count]) {
            Some(TypeCol::BYTES) => {
                let len = (&buf[self.count + TYPE_LEN..]).read_u32() as usize;
                let offset = self.count + TYPE_LEN + STR_PRE_LEN;

                // SAFETY: we only ever input strings in utf8
                let s = unsafe { std::str::from_utf8_unchecked(&buf[offset..offset + len]) };

                self.count += TYPE_LEN + STR_PRE_LEN + len;
                Some(DataCellRef::Str(s))
            }

            Some(TypeCol::INTEGER) => {
                let int = i64::decode(&buf[self.count..]);

                self.count += TYPE_LEN + INT_LEN;
                Some(DataCellRef::Int(int))
            }

            None => None,
        }
    }
}

#[derive(Debug)]
pub(crate) struct Value(Rc<[u8]>);

impl Value {
    pub fn decode(self) -> Vec<DataCell> {
        self.into_iter().collect()
    }
    /// assumes proper encoding
    pub fn from_encoded_slice(data: &[u8]) -> Self {
        Value(Rc::from(data))
    }

    pub fn iter(&self) -> ValueIterRef<'_> {
        ValueIterRef {
            data: self,
            count: 0,
        }
    }
    /// utility function for unit tests, assigns table id 1
    pub fn from_unencoded_str<S: ToString>(str: S) -> Self {
        Value(str.to_string().encode())
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn as_slice(&self) -> &[u8] {
        &self.0[..]
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Value::from_unencoded_str(value)
    }
}
impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::from_unencoded_str(value)
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

pub(crate) struct ValueIterRef<'a> {
    data: &'a Value,
    count: usize,
}

impl<'a> Iterator for ValueIterRef<'a> {
    type Item = DataCellRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let buf = &self.data.0;
        if self.count >= self.data.0.len() {
            return None;
        }

        match TypeCol::from_u8(buf[self.count]) {
            Some(TypeCol::BYTES) => {
                let len = (&buf[self.count + TYPE_LEN..]).read_u32() as usize;
                let offset = self.count + TYPE_LEN + STR_PRE_LEN;

                // SAFETY: we only ever input strings in utf8
                let s = unsafe { std::str::from_utf8_unchecked(&buf[offset..offset + len]) };

                self.count += TYPE_LEN + STR_PRE_LEN + len;
                Some(DataCellRef::Str(s))
            }
            Some(TypeCol::INTEGER) => {
                let int = i64::decode(&buf[self.count..]);

                self.count += TYPE_LEN + INT_LEN;
                Some(DataCellRef::Int(int))
            }
            None => None,
        }
    }
}

impl Eq for Value {}

impl PartialEq<Value> for Value {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl PartialOrd<Value> for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        let mut val_a = &self.0[..];
        let mut val_b = &other.0[..];

        loop {
            if val_a.is_empty() && val_b.is_empty() {
                debug!("both keys empty");
                return Ordering::Equal;
            }
            if val_a.is_empty() {
                debug!("key a empty");
                return Ordering::Less;
            }
            if val_b.is_empty() {
                debug!("key b empty");
                return Ordering::Greater;
            }

            // advancing the type bit
            let ta = val_a.read_u8();
            let tb = val_b.read_u8();

            debug_assert_eq!(ta, tb);
            match ta.cmp(&tb) {
                Ordering::Equal => {}
                o => return o,
            }

            match TypeCol::from_u8(ta) {
                Some(TypeCol::BYTES) => {
                    let len_a = val_a.read_u32() as usize;
                    let len_b = val_b.read_u32() as usize;
                    let min = min(len_a, len_b);

                    debug!(len_a, len_b, min, "comparing strings...");
                    match val_a[..min].cmp(&val_b[..min]) {
                        // comparing a tail string like "1" with "11" would return equal because for min = 1: "1" == "1"
                        // after it would move the slice up, empyting both keys, returning equal,
                        // therefore another match is needed to compare lengths
                        Ordering::Equal => match len_a.cmp(&len_b) {
                            Ordering::Equal => {
                                val_a = &val_a[len_a..];
                                val_b = &val_b[len_b..];
                            }
                            o => return o,
                        },
                        o => return o,
                    }
                }
                Some(TypeCol::INTEGER) => {
                    // flipping the sign bit for comparison
                    let int_a = val_a.read_i64() as u64 ^ 0x8000_0000_0000_0000;
                    let int_b = val_b.read_i64() as u64 ^ 0x8000_0000_0000_0000;
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

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut str = String::new();
        for cell in self.iter() {
            match cell {
                DataCellRef::Str(s) => write!(str, "{} ", s)?,
                DataCellRef::Int(i) => write!(str, "{} ", i)?,
            };
        }
        write!(f, "{}", str.trim())?;
        Ok(())
    }
}
/// Record object used to insert data
#[derive(Debug)]
pub(crate) struct Record {
    data: Vec<DataCell>,
}

impl Record {
    pub fn new() -> Self {
        Record { data: vec![] }
    }

    /// add a datacell to the record
    ///
    /// sensitive to order in which input is added
    pub fn add<T: InputData>(mut self, data: T) -> Self {
        self.data.push(data.into_cell());
        self
    }

    /// encodes a Record according to a schema into a key value pair starting with schema table id
    ///
    /// validates that record matches with column in schema
    pub fn encode(self, schema: &Table) -> Result<(Key, Value), TableError> {
        if schema.cols.len() != self.data.len() {
            error!(?schema, "input doesnt match column count");
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

        // composing byte array by iterating through all columns designated as primary key
        for (i, col) in schema.cols.iter().enumerate() {
            // remembering the cutoff point between keys and values
            if i == schema.pkeys as usize {
                key_idx = idx;
            }
            match col.data_type {
                TypeCol::BYTES => {
                    if let DataCell::Str(str) = &self.data[i] {
                        let str = str.encode();
                        idx += str.len();
                        buf.extend_from_slice(&str);
                        debug!(idx, "encoding string");
                    } else {
                        error!(
                            "expected {:?}, got {}",
                            TypeCol::BYTES,
                            format!("{:?}", &self.data[i])
                        );
                        return Err(TableError::RecordEncodeError {
                            expected: TypeCol::BYTES,
                            found: format!("{:?}", &self.data[i]),
                        });
                    }
                }
                TypeCol::INTEGER => {
                    if let DataCell::Int(num) = self.data[i] {
                        let num = num.encode();
                        idx += num.len();
                        buf.extend_from_slice(&num);
                        debug!(idx, "encoding integer");
                    } else {
                        error!(
                            "expected {:?}, got {}",
                            TypeCol::BYTES,
                            format!("{:?}", &self.data[i])
                        );
                        return Err(TableError::RecordEncodeError {
                            expected: TypeCol::INTEGER,
                            found: format!("{:?}", &self.data[i]),
                        });
                    }
                }
            }
        }
        let key_slice = &buf[..key_idx];
        let val_slice = &buf[key_idx..];

        if key_slice.len() > BTREE_MAX_KEY_SIZE {
            return Err(TableError::RecordError(
                "maximum key size exceeded".to_string(),
            ));
        }
        if val_slice.len() > BTREE_MAX_VAL_SIZE {
            return Err(TableError::RecordError(
                "maximum value size exceeded".to_string(),
            ));
        }

        Ok((
            Key::from_encoded_slice(key_slice),
            Value::from_encoded_slice(val_slice),
        ))
    }

    pub fn from_kv(kv: (Key, Value)) -> Record {
        let mut v = Vec::new();
        v.extend(kv.0.into_iter());
        v.extend(kv.1.into_iter());
        Record { data: v }
    }

    fn test(id: u64, key: &str, val: &str) -> Self {
        todo!()
    }
}

impl std::fmt::Display for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = String::new();
        for cell in self.data.iter() {
            match cell {
                DataCell::Str(str) => write!(s, "{} ", str)?,
                DataCell::Int(i) => write!(s, "{} ", i)?,
            };
        }
        write!(f, "{}", s.trim())?;
        Ok(())
    }
}

/// Query object used to construct a key
///
/// validates that record matches with primary key columns in schema
#[derive(Debug)]
pub(crate) struct Query {
    data: HashMap<String, DataCell>,
}

impl Query {
    pub fn new() -> Self {
        Query {
            data: HashMap::new(),
        }
    }

    /// add the column and primary key which you want to query
    ///
    /// keys dont have to be inserted in order
    pub fn add<T: InputData>(mut self, col: &str, value: T) -> Self {
        self.data.insert(col.to_string(), value.into_cell());
        self
    }

    /// encodes a Query according into a key
    ///
    /// will error if primary keys are missing or the data type doesnt match
    pub fn encode(self, schema: &Table) -> Result<Key, TableError> {
        let mut buf = Vec::<u8>::new();

        // encoding table id
        buf.extend_from_slice(&schema.id.to_le_bytes());

        // encoding primary keys
        for i in 0..schema.pkeys {
            let col = &schema.cols[i as usize];

            match self.data.get(&col.title) {
                Some(DataCell::Str(s)) => {
                    if col.data_type == TypeCol::BYTES {
                        buf.extend_from_slice(&String::encode(s));
                    } else {
                        error!("expected {:?}, got {:?}", TypeCol::BYTES, col.data_type);
                        return Err(TableError::QueryEncodeError {
                            expected: TypeCol::BYTES,
                            found: format!("{:?}", col.data_type),
                        });
                    }
                }
                Some(DataCell::Int(int)) => {
                    if col.data_type == TypeCol::INTEGER {
                        buf.extend_from_slice(&i64::encode(int));
                    } else {
                        error!("expected {:?}, got {:?}", TypeCol::BYTES, col.data_type);
                        return Err(TableError::QueryEncodeError {
                            expected: TypeCol::BYTES,
                            found: format!("{:?}", col.data_type),
                        });
                    }
                }
                // invalid column name
                None => return Err(TableError::QueryError("invalid column name".to_string())),
            }
        }
        Ok(Key::from_encoded_slice(&buf))
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum DataCell {
    Str(String),
    Int(i64),
}

pub(crate) trait InputData {
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
    use crate::database::{pager::mempage_tree, tables::tables::Database};

    use super::super::tables::TableBuilder;
    use super::*;
    use test_log::test;
    use tracing::{Level, info, span};

    #[test]
    fn record1() -> Result<(), Box<dyn std::error::Error>> {
        let pager = mempage_tree();
        let mut db = Database::new(pager);

        let table = TableBuilder::new()
            .name("mytable")
            .id(2)
            .pkey(2)
            .add_col("greeter", TypeCol::BYTES)
            .add_col("number", TypeCol::INTEGER)
            .add_col("gretee", TypeCol::BYTES)
            .build(&mut db)
            .unwrap();

        let s1 = "hello";
        let i1 = 10;
        let s2 = "world";

        let rec = Record::new().add(s1).add(i1).add(s2);

        let (key, value) = rec.encode(&table).unwrap();
        assert_eq!(key.to_string(), "2 hello 10");
        assert_eq!(value.to_string(), "world");
        Ok(())
    }

    #[test]
    fn key_cmp1() -> Result<(), Box<dyn std::error::Error>> {
        let pager = mempage_tree();
        let mut db = Database::new(pager);

        let table = TableBuilder::new()
            .name("mytable")
            .id(2)
            .pkey(2)
            .add_col("greeter", TypeCol::BYTES)
            .add_col("number", TypeCol::INTEGER)
            .add_col("gretee", TypeCol::BYTES)
            .build(&mut db)?;

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
        assert_eq!(key1.to_string(), "2 hello 10");

        let (key3, value3) = Record::new()
            .add("smol")
            .add(5)
            .add("world")
            .encode(&table)?;

        assert!(key2 < key3);
        assert_eq!(key3.to_string(), "2 smol 5");
        Ok(())
    }

    #[test]
    fn records_test_str() -> Result<(), Box<dyn std::error::Error>> {
        let key = Key::from_unencoded_str("hello");
        assert_eq!(key.to_string(), "1 hello");

        let key: Key = "hello".into();
        assert_eq!(key.to_string(), "1 hello");

        let key = Key::from_unencoded_str("owned hello".to_string());
        assert_eq!(key.to_string(), "1 owned hello");

        let val: Value = "world".into();
        assert_eq!(val.to_string(), "world");
        Ok(())
    }

    #[test]
    fn key_cmp2() -> Result<(), Box<dyn std::error::Error>> {
        let k2: Key = "9".into();
        let k3: Key = "10".into();
        let k1: Key = "1".into();
        let k4: Key = "1".into();
        assert!(k3 < k2);
        assert!(k1 < k2);
        assert!(k1 < k3);
        assert!(k1 == k4);
        Ok(())
    }
}
