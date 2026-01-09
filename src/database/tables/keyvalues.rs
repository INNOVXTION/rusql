use std::cell;
use std::cmp::min;
use std::path::Prefix;
use std::rc::Rc;
use std::{cmp::Ordering, fmt::Write};

use tracing::debug;

use crate::database::codec::*;
use crate::database::errors::{Error, Result};
use crate::database::tables::tables::TypeCol;
use crate::database::types::DataCell;

// encoded and parsed key for the pager, should only be created from encoding a record
//
// -----------------------------KEY--------------------------------|
// [ TID ][IDX PREFIX][      INT      ][            STR           ]|
// [ 4B  ][    2B    ][1B TYPE][8B INT][1B TYPE][4B STRLEN][nB STR]|

/// owned object of encoded data used for tree operations
#[derive(Debug, Clone)]
pub(crate) struct Key(Rc<[u8]>);

impl Key {
    /// sentinal empty key with value "0 0" 6 Bytes
    pub fn new_empty() -> Self {
        Key(Rc::from([0u8; 6]))
    }

    /// checks if key is the empty key, for len of the actual key use the len() method
    pub fn is_empty(&self) -> bool {
        let e = [0u8; 6];
        e == *self.0
    }

    pub fn iter(&self) -> KeyIterRef<'_> {
        KeyIterRef {
            data: self,
            count: TID_LEN + PREFIX_LEN,
        }
    }

    // reads the first 8 bytes
    pub fn get_tid(&self) -> u32 {
        u32::from_le_bytes(self.0[..TID_LEN].try_into().expect("this cant fail"))
    }

    pub fn get_prefix(&self) -> u16 {
        u16::from_le_bytes(
            self.0[TID_LEN..TID_LEN + PREFIX_LEN]
                .try_into()
                .expect("this cant fail"),
        )
    }

    /// its up to the caller to ensure the data is properly encoded
    pub fn from_encoded_slice(data: &[u8]) -> Self {
        Key(Rc::from(data))
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0[..]
    }

    /// utility function for unit tests
    ///
    /// adds TID = 1, PREFIX = 0
    pub fn from_unencoded_type<S: Codec>(data: S) -> Self {
        let mut buf: Vec<u8> = vec![];

        // TID 1 and PREFIX 0
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.extend_from_slice(&0u16.to_le_bytes());
        buf.extend_from_slice(&data.encode());

        Key(Rc::from(buf))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// turns key back into Cells, the TID is lost in conversion
    fn decode(self) -> Vec<DataCell> {
        self.into_iter().collect()
    }
}

// the following conversions should only be used for testing!
impl From<&str> for Key {
    fn from(value: &str) -> Self {
        Key::from_unencoded_type(value.to_string())
    }
}
impl From<String> for Key {
    fn from(value: String) -> Self {
        Key::from_unencoded_type(value)
    }
}
impl From<i64> for Key {
    fn from(value: i64) -> Self {
        Key::from_unencoded_type(value)
    }
}

impl std::fmt::Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.get_tid())?;
        write!(f, " {}", self.get_prefix())?;
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
        key_cmp(self.as_slice(), other.as_slice())
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
            count: TID_LEN + PREFIX_LEN, // skipping the table id and prefix
        }
    }
}

pub(crate) struct KeyRef<'a>(&'a [u8]);

impl<'a> KeyRef<'a> {
    pub fn from_slice(slice: &'a [u8]) -> Self {
        KeyRef(slice)
    }

    pub fn to_owned(self) -> Key {
        Key::from_encoded_slice(self.0)
    }

    pub fn as_slice(self) -> &'a [u8] {
        self.0
    }

    // reads the first 8 bytes
    pub fn get_tid(&self) -> u32 {
        u32::from_le_bytes(self.0[..TID_LEN].try_into().expect("this cant fail"))
    }

    pub fn get_prefix(&self) -> u16 {
        u16::from_le_bytes(
            self.0[TID_LEN..TID_LEN + PREFIX_LEN]
                .try_into()
                .expect("this cant fail"),
        )
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
        cell_cmp(&self.0[..], &other.0[..])
    }
}

/// iterates over encoded datacells to determine order
fn cell_cmp(a: &[u8], b: &[u8]) -> Ordering {
    let mut val_a = a;
    let mut val_b = b;

    #[cfg(test)]
    {
        if let Ok("debug") = std::env::var("RUSQL_LOG_CMP").as_deref() {
            debug!(len_a = val_a.len());
            debug!(?val_a);
            debug!(len_b = val_b.len());
            debug!(?val_b);
        }
    }

    loop {
        if let Some(o) = len_cmp(val_a, val_b) {
            return o;
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
                let int_a = val_a.read_i64();
                let int_b = val_b.read_i64();

                // debug!(int_a, int_b, "comparing integer");

                // flipping the sign bit for comparison
                let in_a = int_a as u64 ^ 0x8000_0000_0000_0000;
                let in_b = int_b as u64 ^ 0x8000_0000_0000_0000;

                match int_a.cmp(&int_b) {
                    Ordering::Equal => {
                        // debug!("integer are equal");
                    }
                    o => return o,
                }
            }
            None => unreachable!(),
        }
    }
}

// assumes the slice is an encoded key
fn key_cmp(mut key_a: &[u8], mut key_b: &[u8]) -> Ordering {
    let tid_a = key_a.read_u32();
    let tid_b = key_b.read_u32();

    match tid_a.cmp(&tid_b) {
        Ordering::Equal => (),
        o => return o,
    }

    if let Some(o) = len_cmp(key_a, key_b) {
        return o;
    }

    let prefix_a = key_a.read_u16();
    let prefix_b = key_b.read_u16();

    match prefix_a.cmp(&prefix_b) {
        Ordering::Equal => (),
        o => return o,
    }

    if let Some(o) = len_cmp(key_a, key_b) {
        return o;
    }

    cell_cmp(key_a, key_b)
}

/// returns ordering based on empty slice
fn len_cmp(a: &[u8], b: &[u8]) -> Option<Ordering> {
    if a.is_empty() && b.is_empty() {
        debug!("both keys empty");
        return Some(Ordering::Equal);
    }
    if a.is_empty() {
        debug!("key a empty");
        return Some(Ordering::Less);
    }
    if b.is_empty() {
        debug!("key b empty");
        return Some(Ordering::Greater);
    }
    None
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

// #[cfg(test)]
// mod test {
//     use crate::database::{pager::mempage_tree, tables::Record};

//     use super::super::tables::TableBuilder;
//     use super::*;
//     use test_log::test;

//     #[test]
//     fn key_cmp1() -> Result<()> {
//         let pager = mempage_tree();
//         let mut db = KVDB::new(pager);

//         let table = TableBuilder::new()
//             .name("mytable")
//             .id(2)
//             .pkey(2)
//             .add_col("greeter", TypeCol::BYTES)
//             .add_col("number", TypeCol::INTEGER)
//             .add_col("gretee", TypeCol::BYTES)
//             .build(&mut db)?;

//         let kv1 = Record::new()
//             .add("hello")
//             .add(10)
//             .add("world")
//             .encode(&table)?
//             .next()
//             .unwrap();

//         let kv2 = Record::new()
//             .add("hello")
//             .add(10)
//             .add("world")
//             .encode(&table)?
//             .next()
//             .unwrap();

//         assert_eq!(kv1, kv2);
//         assert_eq!(kv1.0.to_string(), "2 0 hello 10");

//         let kv3 = Record::new()
//             .add("smol")
//             .add(5)
//             .add("world")
//             .encode(&table)?
//             .next()
//             .unwrap();

//         assert!(kv2.0 < kv3.0);
//         assert_eq!(kv3.0.to_string(), "2 0 smol 5");
//         Ok(())
//     }

//     #[test]
//     fn key_cmp2() -> Result<()> {
//         let k2: Key = "9".into();
//         let k3: Key = "10".into();
//         let k1: Key = "1".into();
//         let k4: Key = "1".into();
//         assert!(k3 < k2);
//         assert!(k1 < k2);
//         assert!(k1 < k3);
//         assert!(k1 == k4);
//         Ok(())
//     }

//     #[test]
//     fn empty_key() {
//         let k: Key = "".into();

//         // 4 tid, 2 prefix, 1 type bit, 4 str len
//         assert_eq!(k.as_slice().len(), 11);
//         assert_eq!(k.to_string(), "1 0 ");

//         let e = Key::new_empty();
//         assert_eq!(e.len(), 6);
//         assert!(e.is_empty());
//     }
// }

// Updated tests for keyvalues.rs - replace the test module starting at line 523

#[cfg(test)]
mod test {
    use crate::database::api::{kvdb::KVDB, tx::TXKind};
    use crate::database::pager::transaction::Transaction;
    use crate::database::{pager::mempage_tree, tables::Record};
    use std::sync::Arc;

    use super::super::tables::TableBuilder;
    use super::*;
    use crate::database::helper::cleanup_file;
    use test_log::test;

    #[test]
    fn key_cmp1() -> Result<()> {
        let path = "test-files/key_cmp1.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let table = TableBuilder::new()
            .name("mytable")
            .id(2)
            .pkey(2)
            .add_col("greeter", TypeCol::BYTES)
            .add_col("number", TypeCol::INTEGER)
            .add_col("gretee", TypeCol::BYTES)
            .build(&mut tx)?;

        let kv1 = Record::new()
            .add("hello")
            .add(10)
            .add("world")
            .encode(&table)?
            .next()
            .unwrap();

        let kv2 = Record::new()
            .add("hello")
            .add(10)
            .add("world")
            .encode(&table)?
            .next()
            .unwrap();

        assert_eq!(kv1, kv2);
        assert_eq!(kv1.0.to_string(), "2 0 hello 10");

        let kv3 = Record::new()
            .add("smol")
            .add(5)
            .add("world")
            .encode(&table)?
            .next()
            .unwrap();

        assert!(kv2.0 < kv3.0);
        assert_eq!(kv3.0.to_string(), "2 0 smol 5");
        db.commit(tx)?;
        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn key_cmp2() -> Result<()> {
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

    #[test]
    fn empty_key() {
        let k: Key = "".into();

        // 4 tid, 2 prefix, 1 type bit, 4 str len
        assert_eq!(k.as_slice().len(), 11);
        assert_eq!(k.to_string(), "1 0 ");

        let e = Key::new_empty();
        assert_eq!(e.len(), 6);
        assert!(e.is_empty());
    }
}
