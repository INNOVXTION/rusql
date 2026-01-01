use std::collections::HashMap;
use std::fmt::Write;

use tracing::{debug, error};

use crate::database::codec::*;
use crate::database::tables::tables::IdxKind;
use crate::database::tables::{Key, Value};
use crate::database::types::{BTREE_MAX_KEY_SIZE, BTREE_MAX_VAL_SIZE, DataCell, InputData};
use crate::database::{
    errors::{Result, TableError},
    tables::tables::{Table, TypeCol},
};

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

    /// encodes a record into the necessary key value pairs to fulfil all indices of a given table schema
    pub fn encode(self, schema: &Table) -> Result<impl Iterator<Item = (Key, Value)>> {
        if schema.cols.len() != self.data.len() {
            error!(?schema, "input doesnt match column count");
            return Err(
                TableError::RecordError("input doesnt match column count".to_string()).into(),
            );
        }

        // validation
        for (i, cell) in self.data.iter().enumerate() {
            let cell_type = match cell {
                DataCell::Str(_) => TypeCol::BYTES,
                DataCell::Int(_) => TypeCol::INTEGER,
            };
            if schema.cols[i].data_type != cell_type {
                return Err(
                    TableError::RecordError("Record doesnt match column".to_string()).into(),
                );
            }
        }

        let mut pkey_slice = &self.data[..]; // primary key cells
        let mut skey_slice = &self.data[..]; // secondary key cells
        let mut cursor: usize = 0; // encoded columns in dataset
        let mut res = vec![];

        for (i, idx) in schema.indices.iter().enumerate() {
            let n_cols = idx.columns.len(); // number of columns for an index

            match idx.kind {
                IdxKind::Primary => {
                    if i != 0 {
                        // first index has to be primary key
                        return Err(TableError::RecordError(format!(
                            "expected index 0 found {i} for primary keys"
                        ))
                        .into());
                    }
                    debug!(?pkey_slice, ?skey_slice);

                    let kv = encode_to_kv(schema.id, idx.prefix, pkey_slice, Some(n_cols))?;
                    assert!(!kv.0.as_slice().len() > TID_LEN + PREFIX_LEN);
                    res.push(kv);
                    pkey_slice = &self.data[..n_cols];
                }
                IdxKind::Secondary => {
                    // secondary indices have empty values to make sure primary keys stay unique
                    skey_slice = &self.data[cursor..cursor + n_cols];
                    debug!(?pkey_slice, ?skey_slice);

                    // we reorganize the slice to be encoded:
                    // [TID][PREFIX][SECONDARY COLS][PRIMARY COLS]
                    let data_iter = skey_slice.iter().chain(pkey_slice.iter());

                    let kv = encode_to_kv(schema.id, idx.prefix, data_iter, None)?;
                    assert!(!kv.0.as_slice().len() > TID_LEN + PREFIX_LEN);
                    res.push(kv);
                }
            };

            cursor += n_cols;
        }
        assert_eq!(res.len(), schema.indices.len());
        Ok(res.into_iter())
    }

    pub fn from_kv(kv: (Key, Value)) -> Record {
        let mut v = Vec::new();
        v.extend(kv.0.into_iter());
        v.extend(kv.1.into_iter());
        Record { data: v }
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
#[derive(Debug)]
pub(crate) struct Query;

impl Query {
    /// constructs a key for direct row lookup
    pub fn with_key(schema: &Table) -> QueryKey {
        QueryKey {
            data: HashMap::new(),
            schema,
        }
    }

    /// constructs a key with only the TID
    pub fn with_table(schema: &Table) -> QueryTable {
        QueryTable { tid: schema.id }
    }

    /// constructs a key with TID + Prefix
    pub fn with_prefix(schema: &Table, prefix: u16) -> QueryPrefix {
        QueryPrefix {
            tid: schema.id,
            prefix,
        }
    }
}

pub(super) struct QueryKey<'a> {
    data: HashMap<String, DataCell>,
    schema: &'a Table,
}

impl<'a> QueryKey<'a> {
    /// add the column and primary key which you want to query, can only be used on QueryKey
    ///
    /// not sensitive to order, but all keys for an index have to be provided
    pub fn add<T: InputData>(mut self, col: &str, value: T) -> Self {
        self.data.insert(col.to_string(), value.into_cell());
        self
    }
    /// encodes a Query into a key
    ///
    /// will error if primary keys are missing or the data type doesnt match
    pub fn encode(self) -> Result<Key> {
        let schema = self.schema;

        // validating that cells match column data types
        for (title, data) in self.data.iter() {
            if !schema.valid_col(title, data) {
                return Err(
                    TableError::QueryError("data type doesnt match column".to_string()).into(),
                );
            }
        }

        let mut buf = Vec::<u8>::new();

        // encoding table id + prefix
        buf.extend_from_slice(&schema.id.to_le_bytes());
        buf.extend_from_slice(&schema.indices[0].prefix.to_le_bytes());

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
                        }
                        .into());
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
                        }
                        .into());
                    }
                }
                // invalid column name
                None => return Err(TableError::QueryError("invalid column name".to_string()))?,
            }
        }
        Ok(Key::from_encoded_slice(&buf))
    }
}

struct QueryTable {
    tid: u32,
}

impl QueryTable {
    fn encode(self) -> Result<Key> {
        let mut buf = [0u8; TID_LEN];
        buf.write_u32(self.tid);
        Ok(Key::from_encoded_slice(&buf))
    }
}

struct QueryPrefix {
    tid: u32,
    prefix: u16,
}

impl QueryPrefix {
    fn encode(self) -> Result<Key> {
        let mut buf = [0u8; TID_LEN + PREFIX_LEN];
        buf.write_u32(self.tid).write_u16(self.prefix);
        Ok(Key::from_encoded_slice(&buf))
    }
}

/// encodes datacells into Key Value pairs
///
/// delimeter marks the idx where keys and values get seperated, none puts everything into Key leaving Value empty
fn encode_to_kv<'a, I>(tid: u32, prefix: u16, data: I, delim: Option<usize>) -> Result<(Key, Value)>
where
    I: IntoIterator<Item = &'a DataCell>,
{
    let mut iter = data.into_iter().peekable();
    if iter.peek().is_none() {
        return Err(TableError::KeyEncodeError("no data provided".to_string()).into());
    }

    let mut buf = Vec::<u8>::new();
    let mut idx: usize = 0;
    let mut key_delim: usize = 0;

    // table id and prefix
    buf.extend_from_slice(&tid.to_le_bytes());
    buf.extend_from_slice(&prefix.to_le_bytes());
    idx += TID_LEN + PREFIX_LEN;

    // composing byte array by iterating through all columns designated as primary key
    for (i, cell) in iter.enumerate() {
        if let Some(n) = delim {
            if n == 0 {
                return Err(
                    TableError::RecordError("delimiter cant be Some(0)".to_string()).into(),
                );
            } else if n == i {
                // mark the cutoff point between keys and values
                key_delim = idx;
            }
        }

        match cell {
            DataCell::Str(str) => {
                let str = str.encode();
                buf.extend_from_slice(&str);
                idx += str.len();
            }
            DataCell::Int(num) => {
                let num = num.encode();
                buf.extend_from_slice(&num);
                idx += num.len();
            }
        }
    }

    if delim.is_none() {
        // empty value
        key_delim = idx;
    }

    let key_slice = &buf[..key_delim];
    let val_slice = &buf[key_delim..];

    if key_slice.len() > BTREE_MAX_KEY_SIZE {
        return Err(TableError::RecordError("maximum key size exceeded".to_string()).into());
    }
    if val_slice.len() > BTREE_MAX_VAL_SIZE {
        return Err(TableError::RecordError("maximum value size exceeded".to_string()).into());
    }

    Ok((
        Key::from_encoded_slice(key_slice),
        Value::from_encoded_slice(val_slice),
    ))
}

#[cfg(test)]
mod test {
    use crate::database::{pager::mempage_tree, tables::db::Database};

    use super::super::tables::TableBuilder;
    use super::*;
    use test_log::test;
    use tracing::{Level, info, span};

    #[test]
    fn record1() -> Result<()> {
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

        let kv = rec.encode(&table)?.next().unwrap();
        assert_eq!(kv.0.to_string(), "2 0 hello 10");
        assert_eq!(kv.1.to_string(), "world");
        Ok(())
    }

    #[test]
    fn records_test_str() -> Result<()> {
        let key = Key::from_unencoded_type("hello".to_string());
        assert_eq!(key.to_string(), "1 0 hello");

        let key: Key = "hello".into();
        assert_eq!(key.to_string(), "1 0 hello");

        let key = Key::from_unencoded_type("owned hello".to_string());
        assert_eq!(key.to_string(), "1 0 owned hello");

        let val: Value = "world".into();
        assert_eq!(val.to_string(), "world");
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
    fn records_secondary_indicies1() -> Result<()> {
        let pager = mempage_tree();
        let mut db = Database::new(pager);

        let mut table = TableBuilder::new()
            .name("mytable")
            .id(2)
            .pkey(1)
            .add_col("greeter", TypeCol::BYTES)
            .add_col("number", TypeCol::INTEGER)
            .add_col("gretee", TypeCol::BYTES)
            .build(&mut db)
            .unwrap();

        table.add_index("number")?;
        assert_eq!(table.indices.len(), 2);

        let s1 = "hello";
        let i1 = 10;
        let s2 = "world";

        let mut rec = Record::new().add(s1).add(i1).add(s2).encode(&table)?;
        let mut kv = rec.next().unwrap();

        assert_eq!(kv.0.to_string(), "2 0 hello");
        assert_eq!(kv.1.to_string(), "10 world");

        kv = rec.next().unwrap();
        assert_eq!(kv.0.to_string(), "2 1 10 hello");
        assert_eq!(kv.1.to_string(), "");

        Ok(())
    }
}
