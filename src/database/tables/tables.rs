use std::{collections::HashMap, marker::PhantomData, ops::Deref, rc::Rc};

use crate::database::{
    codec::Codec,
    errors::TableError,
    pager::diskpager::KVEngine,
    tables::{
        Key,
        records::{DataCell, Query, Record, Value},
    },
};
use serde::{Deserialize, Serialize};

/*
 * Encoding Layout:
 * |-----------KEY----------|----Value-----|
 * |          [Col1][Col2]..|[Col3][Col4]..|
 * |[TABLE ID][PK1 ][PK2 ]..|[ v1 ][ v2 ]..|
 *
 * Key: 0 Val: Tdef Schema
 *
 * Tdef, id = 1:
 * |-----KEY---|----Val---|
 * |   [ Col1 ]|[  Col2  ]|
 * |[1][ name ]|[  def   ]|
 *
 * Meta, id = 2:
 * |-----KEY---|----Val---|
 * |   [ Col1 ]|[  Col2  ]|
 * |[2][ key  ]|[  val   ]|
 *
 * Data Path:
 * User Input -> DataCell -> Record -> Key, Value
 *
 */

// fixed table which holds all the schemas
const DEF_TABLE_NAME: &'static str = "tdef";
const DEF_TABLE_COL1: &'static str = "name";
const DEF_TABLE_COL2: &'static str = "def";
const DEF_TABLE_ID: u64 = 1;
const DEF_TABLE_PKEYS: u16 = 1;

// fixed meta table holding all the unique table ids
const META_TABLE_NAME: &'static str = "tmeta";
const META_TABLE_COL1: &'static str = "name";
const META_TABLE_COL2: &'static str = "tid";
const META_TABLE_ID: u64 = 2;
const META_TABLE_PKEYS: u16 = 1;

/// wrapper for sentinal value
#[derive(Serialize, Deserialize)]
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
                    data_type: TypeCol::INTEGER,
                },
            ],
            pkeys: META_TABLE_PKEYS,
            _priv: PhantomData,
        })
    }

    fn as_table(self) -> Table {
        self.0
    }
}

/// wrapper for sentinal value
#[derive(Serialize, Deserialize)]
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
            _priv: PhantomData,
        })
    }
}

impl Deref for TDefTable {
    type Target = Table;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub(crate) struct TableBuilder {
    name: Option<String>,
    id: Option<u64>,
    cols: Vec<Column>,
    pkeys: Option<u16>,
}

impl TableBuilder {
    pub fn new() -> Self {
        TableBuilder {
            name: None,
            id: None,
            cols: vec![],
            pkeys: None,
        }
    }

    pub fn name(mut self, name: &str) -> Self {
        self.name = Some(name.to_string());
        self
    }

    /// default 0
    pub fn id(mut self, id: u64) -> Self {
        self.id = Some(id);
        self
    }

    pub fn add_col(mut self, title: &str, data_type: TypeCol) -> Self {
        self.cols.push(Column {
            title: title.to_string(),
            data_type,
        });
        self
    }

    pub fn pkey(mut self, nkeys: u16) -> Self {
        self.pkeys = Some(nkeys);
        self
    }

    // TODO: extend validation
    pub fn build(self) -> Result<Table, TableError> {
        let name = match self.name {
            Some(n) => n,
            None => return Err(TableError::TableError("invalid name".to_string())),
        };
        // TODO: check meta table for unique id
        let id = match self.id {
            Some(id) => id,
            None => return Err(TableError::TableError("invalid id".to_string())),
        };
        let cols = self.cols;
        let pkeys = match self.pkeys {
            Some(pk) => pk,
            None => return Err(TableError::TableError("invalid pkeys".to_string())),
        };
        Ok(Table {
            name,
            id,
            cols,
            pkeys,
            _priv: PhantomData,
        })
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub(crate) struct Table {
    pub(crate) name: String,
    pub(crate) id: u64,
    pub(crate) cols: Vec<Column>,
    pub(crate) pkeys: u16,

    // ensures tables are built through constructor
    _priv: PhantomData<bool>,
}

impl Table {
    /// encodes table to JSON string
    pub fn encode(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }

    /// decodes JSON string into table
    pub fn decode(value: Value) -> Self {
        serde_json::from_str(&value.to_string()).unwrap()
    }

    pub fn cols(&self) -> &[Column] {
        &self.cols[..]
    }
    pub fn id(&self) -> u64 {
        self.id
    }
    pub fn pkeys(&self) -> u16 {
        self.pkeys
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub(crate) struct Column {
    pub title: String,
    pub data_type: TypeCol,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
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
    buffer: HashMap<String, Table>, // table name as key
    kv_engine: KV,
}

impl<KV: KVEngine> Database<KV> {
    pub fn new(pager: KV) -> Self {
        Database {
            tdef: TDefTable::new(),
            buffer: HashMap::new(),
            kv_engine: pager,
        }
    }

    pub fn new_tid(&mut self) -> u64 {
        let key = Query::new()
            .add(META_TABLE_COL1, "id") // we query name column, where pkey = "id"
            .encode(&self.tdef)
            .unwrap();

        match self.kv_engine.get(key).ok() {
            Some(value) => {
                let meta = self.get_meta();
                let res = value.decode();

                if let DataCell::Int(i) = res[0] {
                    // incrementing the ID
                    let (k, v) = Record::new().add("id").add(i + 1).encode(&meta).unwrap();
                    self.kv_engine.set(k, v);

                    i as u64
                } else {
                    // error when types dont match
                    todo!()
                }
            }
            // no id entry yet
            None => {
                let meta = self.get_meta();

                let (k, v) = Record::new().add("id").add(3).encode(&meta).unwrap(); // tid 1 and 2 are taken
                self.kv_engine.set(k, v);

                3
            }
        }
    }

    fn get_meta(&mut self) -> &Table {
        // check buffer
        if self.buffer.contains_key(META_TABLE_NAME) {
            return self.buffer.get(META_TABLE_NAME).unwrap();
        }

        let meta = MetaTable::new().as_table();
        self.buffer.insert(META_TABLE_NAME.to_string(), meta);
        self.buffer.get(META_TABLE_NAME).unwrap()
    }

    /// overwrites table in buffer
    pub fn insert_table(&mut self, table: &Table) {
        let (key, value) = Record::new()
            .add(table.name.clone())
            .add(table.encode())
            .encode(&self.tdef)
            .unwrap();
        let _ = self.kv_engine.set(key, value);

        // updating buffer
        self.buffer.insert(table.name.clone(), table.clone());
    }

    /// gets the schema for a table name, schema is stored inside buffer
    pub fn get_table(&mut self, name: &str) -> Option<&Table> {
        // check buffer
        if self.buffer.contains_key(name) {
            return self.buffer.get(name);
        }
        let key = Query::new().add("name", name).encode(&self.tdef).unwrap();

        if let Ok(t) = self.kv_engine.get(key) {
            self.buffer.insert(name.to_string(), Table::decode(t));
            Some(self.buffer.get(&name.to_string()).unwrap())
        } else {
            None
        }
    }

    fn insert_rec(&mut self, rec: Record, schema: &Table) -> Result<(), TableError> {
        let (key, value) = rec.encode(schema).unwrap();
        let _ = self.kv_engine.set(key, value);

        Ok(())
    }
    fn get_rec(&self, query: Query, schema: &Table) -> Option<Value> {
        self.kv_engine.get(query.encode(schema).unwrap()).ok()
    }
}

// outward API
trait DatabaseAPI {
    fn create_table(&self);
    fn drop_table(&self);

    fn get(&self);
    fn insert(&self);
    fn update(&self);
    fn delete(&self);
}

#[cfg(test)]
mod test {
    use crate::database::{
        pager::{diskpager::Envoy, mempage_tree},
        tables::tables,
    };

    use super::*;
    use crate::database::helper::cleanup_file;
    use test_log::test;

    #[test]
    fn meta_page() {
        let path = "table1.rdb";
        cleanup_file(path);
        let pager = Envoy::new(path);
        let db = Database::new(pager);
        cleanup_file(path);
    }

    #[test]
    fn tables1() {
        let pager = mempage_tree();
        let mut db = Database::new(pager);

        let table = TableBuilder::new()
            .id(3)
            .name("mytable")
            .add_col("name", TypeCol::BYTES)
            .add_col("age", TypeCol::INTEGER)
            .pkey(1)
            .build()
            .unwrap();

        db.insert_table(&table);

        let dec_table = db.get_table("mytable").unwrap();
        assert_eq!(*dec_table, table);
    }

    #[test]
    fn tables2() {
        let pager = mempage_tree();
        let mut db = Database::new(pager);

        let table = TableBuilder::new()
            .id(3)
            .name("mytable")
            .add_col("name", TypeCol::BYTES)
            .add_col("age", TypeCol::INTEGER)
            .pkey(1)
            .build()
            .unwrap();

        db.insert_table(&table);

        let mut entries = vec![];
        entries.push(Record::new().add("Alice").add(20));
        entries.push(Record::new().add("Bob").add(15));
        entries.push(Record::new().add("Charlie").add(25));

        for entry in entries {
            db.insert_rec(entry, &table).unwrap()
        }

        let q1 = Query::new().add("name", "Alice");
        let q2 = Query::new().add("name", "Bob");
        let q3 = Query::new().add("name", "Charlie");

        assert_eq!(db.get_rec(q1, &table).unwrap().to_string(), "20");
        assert_eq!(db.get_rec(q2, &table).unwrap().to_string(), "15");
        assert_eq!(db.get_rec(q3, &table).unwrap().to_string(), "25");
    }
}
