use std::rc::Rc;

use tracing::{debug, error, info, instrument};

use crate::database::{
    BTree,
    api::txdb::TXDB,
    btree::{Compare, ScanIter, ScanMode, SetFlag, SetResponse, Tree},
    codec::*,
    errors::{Error, Result, TXError, TableError},
    pager::{KVEngine, MetaPage, Pager},
    tables::{
        Key, Query, Record, Value,
        tables::{
            DEF_TABLE_COL1, LOWEST_PREMISSIABLE_TID, META_TABLE_COL1, META_TABLE_ID_ROW,
            META_TABLE_NAME, MetaTable, Table,
        },
    },
    types::DataCell,
};

/// Transaction struct, on a per thread basis
pub struct TX {
    pub db: Rc<TXDB>,      // resources
    pub tree: BTree<TXDB>, // snapshot

    pub version: u64,
    pub kind: TXKind,
    pub rollback: MetaPage,

    pub key_range: Option<Vec<Touched>>,
}

pub enum Touched {
    Single(Key),
    Open(Key, Compare),
    Range {
        lo: (Key, Compare),
        hi: (Key, Compare),
    },
}

#[derive(Copy, Clone, Debug, PartialEq, PartialOrd)]
pub enum TXKind {
    Read,
    Write,
}

impl TX {
    /// wrapper function for read_table()
    #[instrument(name = "get table", skip_all)]
    fn get_table(&mut self, name: &str) -> Option<Rc<Table>> {
        info!(name, "getting table");
        self.db.db_link.read_table(name, &self.tree)
    }

    /// clones the value, adds it to the touched array for conflict comparison later
    ///
    /// accepts either combination of single keys or ScanMode
    fn add_touched(&mut self, key: Option<&Key>, scan: Option<&ScanMode>) {
        match self.key_range {
            Some(ref mut v) => {
                if let Some(k) = key {
                    v.push(Touched::Single(k.clone()));
                }
                match scan {
                    Some(scan) => match scan {
                        ScanMode::Open(k, c) => v.push(Touched::Open(k.clone(), c.clone())),
                        ScanMode::Range { lo, hi } => v.push(Touched::Range {
                            lo: lo.clone(),
                            hi: hi.clone(),
                        }),
                    },
                    None => (),
                }
            }
            None => {
                let mut v = vec![];
                if let Some(k) = key {
                    v.push(Touched::Single(k.clone()));
                }
                match scan {
                    Some(scan) => match scan {
                        ScanMode::Open(k, c) => v.push(Touched::Open(k.clone(), c.clone())),
                        ScanMode::Range { lo, hi } => v.push(Touched::Range {
                            lo: lo.clone(),
                            hi: hi.clone(),
                        }),
                    },
                    None => (),
                }
                self.key_range = Some(v);
            }
        }
    }

    #[instrument(name = "new table id", skip_all)]
    pub fn new_tid(&mut self) -> Result<u32> {
        if self.kind == TXKind::Read {
            return Err(TXError::MismatchedKindError.into());
        }

        let meta = self.db.db_link.get_meta(&self.tree);
        let key = Query::with_key(&meta)
            .add(META_TABLE_COL1, META_TABLE_ID_ROW) // we query name column, where pkey = tid
            .encode()?;

        match self.tree.get(key) {
            Some(value) => {
                let res = value.decode();

                if let DataCell::Int(i) = res[0] {
                    // incrementing the ID
                    // WIP FOR TESTING
                    let (k, v) = Record::new()
                        .add(META_TABLE_ID_ROW)
                        .add(i + 1)
                        .encode(&meta)?
                        .next()
                        .unwrap();

                    self.tree.set(k, v, SetFlag::UPDATE).map_err(|e| {
                        error!(?e);
                        TableError::TableIdError("error when retrieving id".to_string())
                    })?;
                    Ok(i as u32 + 1)
                } else {
                    // types dont match
                    return Err(TableError::TableIdError(
                        "id doesnt match expected int".to_string(),
                    ))?;
                }
            }
            // no id entry yet
            None => {
                // WIP FOR TESTING
                let (k, v) = Record::new()
                    .add(META_TABLE_ID_ROW)
                    .add(3)
                    .encode(&meta)?
                    .next()
                    .unwrap();

                self.tree.set(k, v, SetFlag::INSERT).map_err(|e| {
                    error!(?e);
                    TableError::TableIdError("error when retrieving id".to_string())
                })?;

                Ok(LOWEST_PREMISSIABLE_TID)
            }
        }
    }

    /// overwrites table in buffer
    #[instrument(name = "insert table", skip_all)]
    pub fn insert_table(&mut self, table: &Table) -> Result<()> {
        info!(?table, "inserting table");
        if self.kind == TXKind::Read {
            return Err(TXError::MismatchedKindError.into());
        }

        if self.get_table(&table.name).is_some() {
            error!(name = table.name, "table with provided name exists already");
            return Err(TableError::InsertTableError(
                "table with provided name exists already".into(),
            )
            .into());
        }

        // WIP FOR TESTS
        let (k, v) = Record::new()
            .add(table.name.clone())
            .add(table.encode()?)
            .encode(&self.db.db_link.t_def)?
            .next()
            .ok_or(TableError::InsertTableError(
                "record iterator failure".to_string(),
            ))?;

        self.tree.set(k, v, SetFlag::UPSERT).map_err(|e| {
            error!("error when inserting");
            TableError::InsertTableError("error when inserting table".to_string())
        })?;
        Ok(())
    }

    /// TODO: decrement/free up table id
    ///
    /// drops table from schema table
    #[instrument(name = "drop table", skip_all)]
    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        info!(name, "dropping table");
        if self.kind == TXKind::Read {
            return Err(TXError::MismatchedKindError.into());
        }

        let table = match self.get_table(name) {
            Some(t) => t,
            None => {
                return Err(TableError::DeleteTableError("table doesnt exist".to_string()).into());
            }
        };

        // delete keys
        let recs = self.full_table_scan(&table)?.collect_records();
        for rec in recs.into_iter() {
            if let Some(kv) = rec.encode(&table)?.next() {
                self.tree.delete(kv.0)?
            } else {
                return Err(TableError::DeleteTableError("iterator error".to_string()).into());
            }
        }

        // delete from tdef
        let qu = Query::with_key(&self.db.db_link.t_def)
            .add(DEF_TABLE_COL1, name)
            .encode()?;

        self.tree.delete(qu).map_err(|e| {
            error!(%e, "error when dropping table");
            TableError::DeleteTableError("dropping table error when deleting".to_string()).into()
        })
    }

    pub fn full_table_scan(&self, schema: &Table) -> Result<ScanIter<'_, TXDB>> {
        // writing a seek key with TID
        let mut buf = [0u8; TID_LEN];
        let _ = &mut buf[..].write_u32(schema.id);
        let seek_key = Key::from_encoded_slice(&buf);
        let seek_mode = ScanMode::new_open(seek_key, Compare::GT)?;

        self.tree.scan(seek_mode)
    }

    /// inserts a record and potential secondary indicies
    #[instrument(name = "insert rec", skip_all)]
    pub fn insert_rec(&mut self, rec: Record, schema: &Table, flag: SetFlag) -> Result<()> {
        info!(?rec, "inserting record");
        if self.kind == TXKind::Read {
            return Err(TXError::MismatchedKindError.into());
        }

        let mut iter = rec.encode(schema)?.peekable();
        let mut old_rec;
        let old_pk;
        let mut updated = 0;

        // updating the primary key and retrieving the old one
        let primay_key = iter.next().ok_or(Error::InsertError(
            "record failed to generate a primary key".to_string(),
        ))?;

        let res = self.tree.set(primay_key.0, primay_key.1, flag);
        if res.is_ok() {
            updated += 1;
        }
        match res {
            // update found (UPSERT or UPDATE)
            Ok(res) if res.updated => {
                if iter.peek().is_none() {
                    // there are no secondary keys, we are done
                    return Ok(());
                }

                old_pk = res.old.expect("update successful");
                updated += 1;

                // recreating the keys from the update
                old_rec = Record::from_kv(old_pk).encode(schema)?;
                old_rec.next(); // we skip the primary key since we already updated it

                // updating secondary keys
                for (k, v) in iter {
                    if let Some(old_kv) = old_rec.next() {
                        self.tree.delete(old_kv.0)?;
                        let res = self.tree.set(k, v, SetFlag::INSERT)?;

                        if !res.added {
                            return Err(Error::InsertError("couldnt insert record".to_string()));
                        }

                        updated += 1;
                    } else {
                        return Err(Error::InsertError("failed to retrieve old key".to_string()));
                    }
                }
            }
            // INSERT only
            Ok(res) if res.added => {
                for (k, v) in iter {
                    // inserting secondary keys
                    let res = self.tree.set(k, v, SetFlag::INSERT)?;

                    if !res.added {
                        return Err(Error::InsertError("couldnt insert record".to_string()));
                    }

                    updated += 1;
                }
            }
            // Key couldnt be inserted/updated
            Err(e) => {
                return Err(Error::InsertError("couldnt insert record".to_string()));
            }
            _ => unreachable!("we accounted for all cases"),
        }

        debug_assert_eq!(updated, schema.indices.len());
        Ok(())
    }

    /// deletes a record and potential secondary indicies
    fn delete_rec(&mut self, rec: Record, schema: &Table) -> Result<()> {
        info!(?rec, "deleting record");
        if self.kind == TXKind::Read {
            return Err(TXError::MismatchedKindError.into());
        }

        let mut iter = rec.encode(schema)?.peekable();
        let mut updated = 0;

        let primay_key = iter.next().ok_or(Error::DeleteError(
            "record failed to generate a primary key".to_string(),
        ))?;

        // deleting primary key
        if let Ok(()) = self.tree.delete(primay_key.0) {
            updated += 1;
        } else {
            return Err(Error::DeleteError("key doesnt exist".to_string()));
        }

        // checking for secondary keys
        if iter.peek().is_none() {
            return Ok(());
        }

        // deleting secondary keys
        for (k, v) in iter {
            self.tree.delete(k)?;
            updated += 1;
        }

        debug_assert_eq!(updated, schema.indices.len());
        Ok(())
    }

    fn create_index(&self, col: &str, table: &Table) {
        // TODO cant delete from tdef or meta
    }

    fn delete_index(&self, col: &str, table: &Table) {
        // TODO
    }

    fn del_prefix(schema: &Table, prefix: u16) {
        // TODO
        let key = Query::with_prefix(schema, prefix);
        let scan = ScanMode::new_open(key, Compare::GT);
    }
}

// #[cfg(test)]
// mod test {
//     use crate::database::{
//         btree::{Compare, ScanMode, SetFlag},
//         pager::mempage_tree,
//         tables::{Query, Record, TypeCol, tables::TableBuilder},
//         types::DataCell,
//     };

//     use super::*;
//     use crate::database::helper::cleanup_file;
//     use test_log::test;

//     #[test]
//     fn meta_page() {
//         let path = "table1.rdb";
//         cleanup_file(path);
//         let pager = Envoy::new(path);
//         let db = TableKV::new(pager);
//         cleanup_file(path);
//     }

//     #[test]
//     fn tables_encode_decode() {
//         let pager = mempage_tree();
//         let mut db = TableKV::new(pager);

//         let table = TableBuilder::new()
//             .id(3)
//             .name("mytable")
//             .add_col("name", TypeCol::BYTES)
//             .add_col("age", TypeCol::INTEGER)
//             .pkey(1)
//             .build(&mut db)
//             .unwrap();

//         let _ = db.insert_table(&table);

//         let dec_table = db.get_table("mytable").unwrap();
//         assert_eq!(*dec_table, table);

//         // should reject duplicate
//         assert!(db.insert_table(&table).is_err());
//     }

//     #[test]
//     fn records_insert_search() -> Result<()> {
//         let pager = mempage_tree();
//         let mut db = TableKV::new(pager);

//         let table = TableBuilder::new()
//             .id(3)
//             .name("mytable")
//             .add_col("name", TypeCol::BYTES)
//             .add_col("age", TypeCol::INTEGER)
//             .add_col("id", TypeCol::INTEGER)
//             .pkey(1)
//             .build(&mut db)
//             .unwrap();

//         let _ = db.insert_table(&table);
//         let tables = db.get_table("mytable");

//         let mut entries = vec![];
//         entries.push(Record::new().add("Alice").add(20).add(1));
//         entries.push(Record::new().add("Bob").add(15).add(2));
//         entries.push(Record::new().add("Charlie").add(25).add(3));

//         for entry in entries {
//             db.insert_rec(entry, &table, SetFlag::UPSERT).unwrap()
//         }

//         let q1 = Query::with_key(&table).add("name", "Alice").encode()?;
//         let q2 = Query::with_key(&table).add("name", "Bob").encode()?;
//         let q3 = Query::with_key(&table).add("name", "Charlie").encode()?;

//         let q1_res = db.get_rec(q1).unwrap().decode();
//         assert_eq!(q1_res[0], DataCell::Int(20));
//         assert_eq!(q1_res[1], DataCell::Int(1));

//         let q2_res = db.get_rec(q2).unwrap().decode();
//         assert_eq!(q2_res[0], DataCell::Int(15));
//         assert_eq!(q2_res[1], DataCell::Int(2));

//         let q3_res = db.get_rec(q3).unwrap().decode();
//         assert_eq!(q3_res[0], DataCell::Int(25));
//         assert_eq!(q3_res[1], DataCell::Int(3));
//         Ok(())
//     }

//     #[test]
//     fn query_input() {
//         let pager = mempage_tree();
//         let mut db = TableKV::new(pager);

//         let table = TableBuilder::new()
//             .id(3)
//             .name("mytable")
//             .add_col("name", TypeCol::BYTES)
//             .add_col("age", TypeCol::INTEGER)
//             .add_col("id", TypeCol::INTEGER)
//             .pkey(2)
//             .build(&mut db)
//             .unwrap();

//         db.insert_table(&table).unwrap();
//         let tables = db.get_table("mytable");

//         let good_query = Query::with_key(&table).add("name", "Alice").add("age", 10);

//         assert!(good_query.encode().is_ok());

//         let good_query = Query::with_key(&table).add("name", "Alice").add("age", 10);
//         let unordered = Query::with_key(&table).add("age", 10).add("name", "Alice");
//         assert_eq!(good_query.encode().unwrap(), unordered.encode().unwrap());

//         let bad_query = Query::with_key(&table).add("name", "Alice");
//         assert!(bad_query.encode().is_err());

//         let bad_query = Query::with_key(&table).add("dfasdf", "fasdf");
//         assert!(bad_query.encode().is_err());

//         let bad_query = Query::with_key(&table);
//         assert!(bad_query.encode().is_err())
//     }

//     #[test]
//     fn table_ids() {
//         let pager = mempage_tree();
//         let mut db = TableKV::new(pager);
//         assert_eq!(db.new_tid().unwrap(), 3);
//         assert_eq!(db.new_tid().unwrap(), 4);
//         assert_eq!(db.new_tid().unwrap(), 5);
//     }

//     #[test]
//     fn table_builder_validations() {
//         let pager = mempage_tree();
//         let mut db = TableKV::new(pager);

//         // empty name
//         assert!(
//             TableBuilder::new()
//                 .name("")
//                 .id(10)
//                 .add_col("a", TypeCol::BYTES)
//                 .pkey(1)
//                 .build(&mut db)
//                 .is_err()
//         );

//         // zero pkeys
//         assert!(
//             TableBuilder::new()
//                 .name("t")
//                 .id(11)
//                 .add_col("a", TypeCol::BYTES)
//                 .pkey(0)
//                 .build(&mut db)
//                 .is_err()
//         );

//         // more pkeys than cols
//         assert!(
//             TableBuilder::new()
//                 .name("t")
//                 .id(12)
//                 .add_col("a", TypeCol::BYTES)
//                 .pkey(2)
//                 .build(&mut db)
//                 .is_err()
//         );

//         // not enough columns (less than required for your logic)
//         assert!(
//             TableBuilder::new()
//                 .name("t")
//                 .id(13)
//                 .pkey(1)
//                 .build(&mut db)
//                 .is_err()
//         );
//     }

//     #[test]
//     fn duplicate_table_name_rejected() {
//         let pager = mempage_tree();
//         let mut db = TableKV::new(pager);

//         let table = TableBuilder::new()
//             .id(12)
//             .name("dup")
//             .add_col("x", TypeCol::BYTES)
//             .pkey(1)
//             .build(&mut db)
//             .unwrap();

//         assert!(db.insert_table(&table).is_ok());
//         assert!(db.insert_table(&table).is_err());
//     }

//     #[test]
//     fn drop_table_removes_table() {
//         let pager = mempage_tree();
//         let mut db = TableKV::new(pager);

//         let table = TableBuilder::new()
//             .id(13)
//             .name("droppable")
//             .add_col("x", TypeCol::BYTES)
//             .pkey(1)
//             .build(&mut db)
//             .unwrap();

//         db.insert_table(&table).unwrap();
//         assert!(db.get_table("droppable").is_some());
//         assert!(db.drop_table("droppable").is_err());
//     }

//     #[test]
//     fn new_tid_persists_with_envoy() {
//         let path = "test-files/tid_persist.rdb";
//         cleanup_file(path);
//         {
//             let pager = Envoy::new(path);
//             let mut db = TableKV::new(pager);
//             assert_eq!(db.new_tid().unwrap(), 3);
//             assert_eq!(db.new_tid().unwrap(), 4);
//         }
//         // reopen
//         {
//             let pager = Envoy::new(path);
//             let mut db = TableKV::new(pager);
//             // next tid continues
//             assert_eq!(db.new_tid().unwrap(), 5);
//         }
//         cleanup_file(path);
//     }

//     #[test]
//     fn invalid_queries_rejected() {
//         let pager = mempage_tree();
//         let mut db = TableKV::new(pager);

//         let table = TableBuilder::new()
//             .id(16)
//             .name("invalid")
//             .add_col("x", TypeCol::BYTES)
//             .pkey(1)
//             .build(&mut db)
//             .unwrap();

//         db.insert_table(&table).unwrap();

//         // missing primary key
//         assert!(Query::with_key(&table).encode().is_err());

//         // wrong column name
//         assert!(Query::with_key(&table).add("nope", "x").encode().is_err());
//     }

//     #[test]
//     fn scan_open() -> Result<()> {
//         let path = "test-files/scan.rdb";
//         cleanup_file(path);
//         let pager = Envoy::new("test-files/scan.rdb");
//         let mut db = TableKV::new(pager);

//         let table1 = TableBuilder::new()
//             .id(5)
//             .name("table_1")
//             .add_col("name", TypeCol::BYTES)
//             .add_col("age", TypeCol::INTEGER)
//             .pkey(1)
//             .build(&mut db)
//             .unwrap();

//         let table2 = TableBuilder::new()
//             .id(7)
//             .name("table_2")
//             .add_col("id", TypeCol::INTEGER)
//             .add_col("name", TypeCol::BYTES)
//             .add_col("job", TypeCol::BYTES)
//             .pkey(1)
//             .build(&mut db)
//             .unwrap();

//         db.insert_table(&table1)?;
//         db.insert_table(&table2)?;

//         assert!(db.get_table("table_1").is_some());
//         assert!(db.get_table("table_2").is_some());

//         let mut entries_t1 = vec![];
//         entries_t1.push(Record::new().add("Alice").add(20));
//         entries_t1.push(Record::new().add("Bob").add(15));
//         entries_t1.push(Record::new().add("Charlie").add(25));

//         for entry in entries_t1 {
//             db.insert_rec(entry, &table1, SetFlag::UPSERT)?
//         }

//         let mut entries_t2 = vec![];
//         entries_t2.push(Record::new().add(20).add("Alice").add("teacher"));
//         entries_t2.push(Record::new().add(15).add("Bob").add("clerk"));
//         entries_t2.push(Record::new().add(25).add("Charlie").add("fire fighter"));

//         for entry in entries_t2 {
//             db.insert_rec(entry, &table2, SetFlag::UPSERT)?
//         }

//         let open = ScanMode::Open(
//             Query::with_key(&table1).add("name", "Alice").encode()?,
//             Compare::GE,
//         );
//         let res = db.scan(open)?;

//         assert_eq!(res.len(), 3);
//         assert_eq!(res[0].to_string(), "Alice 20");
//         assert_eq!(res[1].to_string(), "Bob 15");
//         assert_eq!(res[2].to_string(), "Charlie 25");

//         let open = ScanMode::Open(
//             Query::with_key(&table2).add("id", 20).encode()?,
//             Compare::GE,
//         );
//         let res = db.scan(open)?;

//         assert_eq!(res.len(), 2);
//         assert_eq!(res[0].to_string(), "20 Alice teacher");
//         assert_eq!(res[1].to_string(), "25 Charlie fire fighter");

//         cleanup_file(path);
//         Ok(())
//     }

//     #[test]
//     fn full_table_scan_seek() -> Result<()> {
//         let path = "test-files/scan.rdb";
//         cleanup_file(path);
//         let pager = Envoy::new("test-files/scan.rdb");
//         let mut db = TableKV::new(pager);

//         let table1 = TableBuilder::new()
//             .id(5)
//             .name("table_1")
//             .add_col("name", TypeCol::BYTES)
//             .add_col("age", TypeCol::INTEGER)
//             .pkey(1)
//             .build(&mut db)
//             .unwrap();

//         let table2 = TableBuilder::new()
//             .id(7)
//             .name("table_2")
//             .add_col("id", TypeCol::INTEGER)
//             .add_col("name", TypeCol::BYTES)
//             .add_col("job", TypeCol::BYTES)
//             .pkey(1)
//             .build(&mut db)
//             .unwrap();

//         db.insert_table(&table1)?;
//         db.insert_table(&table2)?;

//         let mut entries_t1 = vec![];
//         entries_t1.push(Record::new().add("Alice").add(20));
//         entries_t1.push(Record::new().add("Bob").add(15));
//         entries_t1.push(Record::new().add("Charlie").add(25));

//         for entry in entries_t1 {
//             db.insert_rec(entry, &table1, SetFlag::UPSERT)?
//         }

//         let mut entries_t2 = vec![];
//         entries_t2.push(Record::new().add(20).add("Alice").add("teacher"));
//         entries_t2.push(Record::new().add(15).add("Bob").add("clerk"));
//         entries_t2.push(Record::new().add(25).add("Charlie").add("fire fighter"));

//         for entry in entries_t2 {
//             db.insert_rec(entry, &table2, SetFlag::UPSERT)?
//         }

//         let res = db.full_table_scan(&table1)?;
//         assert_eq!(res.len(), 3);

//         let res = db.full_table_scan(&table2)?;
//         assert_eq!(res.len(), 3);

//         cleanup_file(path);
//         Ok(())
//     }
// }

// // these tests were cooked up by claude, good luck!
// #[cfg(test)]
// mod scan {
//     use crate::database::btree::Compare;
//     use crate::database::btree::{ScanMode, SetFlag};
//     use crate::database::tables::{Query, Record};

//     use super::*;
//     use crate::database::helper::cleanup_file;
//     use test_log::test;

//     #[test]
//     fn scan_range_between_keys() -> Result<()> {
//         let path = "test-files/scan_range.rdb";
//         cleanup_file(path);
//         let pager = Envoy::new(path);
//         let mut db = TableKV::new(pager);

//         let table = TableBuilder::new()
//             .id(10)
//             .name("range_table")
//             .add_col("id", TypeCol::INTEGER)
//             .add_col("name", TypeCol::BYTES)
//             .pkey(1)
//             .build(&mut db)
//             .unwrap();

//         db.insert_table(&table)?;

//         // Insert records with ids 1-20
//         for i in 1..=20 {
//             db.insert_rec(
//                 Record::new().add(i as i64).add(format!("name_{}", i)),
//                 &table,
//                 SetFlag::UPSERT,
//             )?;
//         }

//         // Scan from id 5 to 15
//         let lo_key = Query::with_key(&table).add("id", 5i64).encode()?;
//         let hi_key = Query::with_key(&table).add("id", 15i64).encode()?;

//         let range = ScanMode::new_range((lo_key, Compare::GE), (hi_key, Compare::LE))?;

//         let res = db.scan(range)?;

//         assert_eq!(res.len(), 11); // 5 through 15 inclusive
//         assert_eq!(res[0].to_string(), "5 name_5");
//         assert_eq!(res[10].to_string(), "15 name_15");

//         cleanup_file(path);
//         Ok(())
//     }

//     #[test]
//     fn scan_multiple_tables_isolation() -> Result<()> {
//         let path = "test-files/scan_isolation.rdb";
//         cleanup_file(path);
//         let pager = Envoy::new(path);
//         let mut db = TableKV::new(pager);

//         let table1 = TableBuilder::new()
//             .id(20)
//             .name("table_a")
//             .add_col("key", TypeCol::BYTES)
//             .add_col("value", TypeCol::BYTES)
//             .pkey(1)
//             .build(&mut db)
//             .unwrap();

//         let table2 = TableBuilder::new()
//             .id(21)
//             .name("table_b")
//             .add_col("key", TypeCol::BYTES)
//             .add_col("value", TypeCol::BYTES)
//             .pkey(1)
//             .build(&mut db)
//             .unwrap();

//         db.insert_table(&table1)?;
//         db.insert_table(&table2)?;

//         // Insert into table1
//         for i in 1..=10 {
//             db.insert_rec(
//                 Record::new()
//                     .add(format!("key_a_{}", i))
//                     .add(format!("val_a_{}", i)),
//                 &table1,
//                 SetFlag::UPSERT,
//             )?;
//         }

//         // Insert into table2
//         for i in 1..=10 {
//             db.insert_rec(
//                 Record::new()
//                     .add(format!("key_b_{}", i))
//                     .add(format!("val_b_{}", i)),
//                 &table2,
//                 SetFlag::UPSERT,
//             )?;
//         }

//         // Scan table1
//         let open = ScanMode::new_open(
//             Query::with_key(&table1).add("key", "key_a_1").encode()?,
//             Compare::GE,
//         )?;
//         let res1 = db.scan(open)?;

//         // Should only contain table1 records
//         assert!(res1.iter().all(|r| r.to_string().contains("val_a_")));
//         assert_eq!(res1.len(), 10);

//         // Scan table2
//         let open = ScanMode::new_open(
//             Query::with_key(&table2).add("key", "key_b_1").encode()?,
//             Compare::GE,
//         )?;
//         let res2 = db.scan(open)?;

//         // Should only contain table2 records
//         assert!(res2.iter().all(|r| r.to_string().contains("val_b_")));
//         assert_eq!(res2.len(), 10);

//         cleanup_file(path);
//         Ok(())
//     }

//     #[test]
//     fn scan_with_lt_predicate() -> Result<()> {
//         let path = "test-files/scan_lt.rdb";
//         cleanup_file(path);
//         let pager = Envoy::new(path);
//         let mut db = TableKV::new(pager);

//         let table = TableBuilder::new()
//             .id(30)
//             .name("lt_table")
//             .add_col("score", TypeCol::INTEGER)
//             .add_col("player", TypeCol::BYTES)
//             .pkey(1)
//             .build(&mut db)
//             .unwrap();

//         db.insert_table(&table)?;

//         for i in 1..=10 {
//             db.insert_rec(
//                 Record::new()
//                     .add(i as i64 * 10)
//                     .add(format!("player_{}", i)),
//                 &table,
//                 SetFlag::UPSERT,
//             )?;
//         }

//         // Scan backwards from score 50
//         let open = ScanMode::new_open(
//             Query::with_key(&table).add("score", 50i64).encode()?,
//             Compare::LT,
//         )?;

//         let res = db.scan(open)?;

//         // Should return scores 40, 30, 20, 10
//         assert_eq!(res.len(), 4);
//         assert_eq!(res[0].to_string(), "40 player_4");
//         assert_eq!(res[3].to_string(), "10 player_1");

//         cleanup_file(path);
//         Ok(())
//     }

//     #[test]
//     fn scan_empty_result_set() -> Result<()> {
//         let path = "test-files/scan_empty.rdb";
//         cleanup_file(path);
//         let pager = Envoy::new(path);
//         let mut db = TableKV::new(pager);

//         let table = TableBuilder::new()
//             .id(40)
//             .name("empty_table")
//             .add_col("id", TypeCol::INTEGER)
//             .add_col("data", TypeCol::BYTES)
//             .pkey(1)
//             .build(&mut db)
//             .unwrap();

//         db.insert_table(&table)?;

//         for i in 1..=5 {
//             db.insert_rec(
//                 Record::new().add(i as i64).add(format!("data_{}", i)),
//                 &table,
//                 SetFlag::UPSERT,
//             )?;
//         }

//         // Scan for values greater than max
//         let open = ScanMode::new_open(
//             Query::with_key(&table).add("id", 100i64).encode()?,
//             Compare::GT,
//         )?;

//         let result = db.scan(open);
//         assert!(result.is_err()); // Should error on empty result

//         cleanup_file(path);
//         Ok(())
//     }

//     #[test]
//     fn scan_single_record_result() -> Result<()> {
//         let path = "test-files/scan_single.rdb";
//         cleanup_file(path);
//         let pager = Envoy::new(path);
//         let mut db = TableKV::new(pager);

//         let table = TableBuilder::new()
//             .id(50)
//             .name("single_table")
//             .add_col("code", TypeCol::BYTES)
//             .add_col("value", TypeCol::INTEGER)
//             .pkey(1)
//             .build(&mut db)
//             .unwrap();

//         db.insert_table(&table)?;

//         db.insert_rec(
//             Record::new().add("CODE_001").add(42i64),
//             &table,
//             SetFlag::UPSERT,
//         )?;

//         let open = ScanMode::new_open(
//             Query::with_key(&table).add("code", "CODE_001").encode()?,
//             Compare::EQ,
//         );
//         // EQ is not allowed in open scans, should error
//         assert!(open.is_err());

//         cleanup_file(path);
//         Ok(())
//     }

//     #[test]
//     fn scan_with_le_predicate() -> Result<()> {
//         let path = "test-files/scan_le.rdb";
//         cleanup_file(path);
//         let pager = Envoy::new(path);
//         let mut db = TableKV::new(pager);

//         let table = TableBuilder::new()
//             .id(60)
//             .name("le_table")
//             .add_col("value", TypeCol::INTEGER)
//             .add_col("label", TypeCol::BYTES)
//             .pkey(1)
//             .build(&mut db)
//             .unwrap();

//         db.insert_table(&table)?;

//         for i in 10..=50 {
//             db.insert_rec(
//                 Record::new().add(i as i64).add(format!("label_{}", i)),
//                 &table,
//                 SetFlag::UPSERT,
//             )?;
//         }

//         // Scan from 35 backwards (LE)
//         let open = ScanMode::new_open(
//             Query::with_key(&table).add("value", 35i64).encode()?,
//             Compare::LE,
//         )?;

//         let res = db.scan(open)?;

//         // Should include 35 and go backwards to 10
//         assert_eq!(res.len(), 26); // 35 down to 10
//         assert_eq!(res[0].to_string(), "35 label_35");

//         cleanup_file(path);
//         Ok(())
//     }

//     #[test]
//     fn scan_large_dataset() -> Result<()> {
//         let path = "test-files/scan_large.rdb";
//         cleanup_file(path);
//         let pager = Envoy::new(path);
//         let mut db = TableKV::new(pager);

//         let table = TableBuilder::new()
//             .id(70)
//             .name("large_table")
//             .add_col("id", TypeCol::INTEGER)
//             .add_col("name", TypeCol::BYTES)
//             .pkey(1)
//             .build(&mut db)
//             .unwrap();

//         db.insert_table(&table)?;

//         // Insert 500 records
//         for i in 1..=500 {
//             db.insert_rec(
//                 Record::new().add(i as i64).add(format!("name_{:04}", i)),
//                 &table,
//                 SetFlag::UPSERT,
//             )?;
//         }

//         // Scan from id 100 onwards
//         let open = ScanMode::new_open(
//             Query::with_key(&table).add("id", 100i64).encode()?,
//             Compare::GT,
//         )?;

//         let res = db.scan(open)?;

//         assert_eq!(res.len(), 400); // 101 to 500
//         assert_eq!(res[0].to_string(), "101 name_0101");
//         assert_eq!(res[399].to_string(), "500 name_0500");

//         cleanup_file(path);
//         Ok(())
//     }

//     #[test]
//     fn scan_byte_string_keys() -> Result<()> {
//         let path = "test-files/scan_bytes.rdb";
//         cleanup_file(path);
//         let pager = Envoy::new(path);
//         let mut db = TableKV::new(pager);

//         let table = TableBuilder::new()
//             .id(80)
//             .name("byte_table")
//             .add_col("name", TypeCol::BYTES)
//             .add_col("data", TypeCol::BYTES)
//             .pkey(1)
//             .build(&mut db)
//             .unwrap();

//         db.insert_table(&table)?;

//         let names = vec!["alice", "bob", "charlie", "david", "eve"];
//         for name in &names {
//             db.insert_rec(
//                 Record::new().add(*name).add(format!("data_{}", name)),
//                 &table,
//                 SetFlag::UPSERT,
//             )?;
//         }

//         // Scan from "bob" onwards
//         let open = ScanMode::new_open(
//             Query::with_key(&table).add("name", "bob").encode()?,
//             Compare::GE,
//         )?;

//         let res = db.scan(open)?;

//         assert_eq!(res.len(), 4); // bob, charlie, david, eve
//         assert_eq!(res[0].to_string(), "bob data_bob");

//         cleanup_file(path);
//         Ok(())
//     }

//     #[test]
//     fn scan_after_deletes() -> Result<()> {
//         let path = "test-files/scan_after_delete.rdb";
//         cleanup_file(path);
//         let pager = Envoy::new(path);
//         let mut db = TableKV::new(pager);

//         let table = TableBuilder::new()
//             .id(90)
//             .name("delete_table")
//             .add_col("id", TypeCol::INTEGER)
//             .add_col("value", TypeCol::BYTES)
//             .pkey(1)
//             .build(&mut db)
//             .unwrap();

//         db.insert_table(&table)?;

//         // Insert 10 records
//         for i in 1..=10 {
//             db.insert_rec(
//                 Record::new().add(i as i64).add(format!("val_{}", i)),
//                 &table,
//                 SetFlag::UPSERT,
//             )?;
//         }

//         // Delete some records
//         for i in [3, 5, 7].iter() {
//             let key = Query::with_key(&table).add("id", *i as i64).encode()?;
//             db.kve.delete(key)?;
//         }

//         // Scan from beginning
//         let open = ScanMode::new_open(
//             Query::with_key(&table).add("id", 1i64).encode()?,
//             Compare::GE,
//         )?;

//         let res = db.scan(open)?;

//         // Should have 7 records (10 - 3 deleted)
//         assert_eq!(res.len(), 7);
//         assert!(!res.iter().any(|r| r.to_string().contains("val_3")));
//         assert!(!res.iter().any(|r| r.to_string().contains("val_5")));
//         assert!(!res.iter().any(|r| r.to_string().contains("val_7")));

//         cleanup_file(path);
//         Ok(())
//     }
// }

#[cfg(test)]
mod test {
    use crate::database::{
        api::{kvdb::KVDB, tx::TXKind},
        btree::{Compare, ScanMode, SetFlag},
        pager::transaction::Transaction,
        pager::{DiskPager, mempage_tree},
        tables::{Query, Record, TypeCol, tables::TableBuilder},
        types::DataCell,
    };
    use std::sync::Arc;

    use super::*;
    use crate::database::helper::cleanup_file;
    use test_log::test;

    #[test]
    fn meta_page() {
        let path = "test-files/meta_page.rdb";
        cleanup_file(path);
        let _db = Arc::new(KVDB::new(path));
        cleanup_file(path);
    }

    #[test]
    fn tables_encode_decode() {
        let path = "test-files/tables_encode_decode.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let table = TableBuilder::new()
            .id(3)
            .name("mytable")
            .add_col("name", TypeCol::BYTES)
            .add_col("age", TypeCol::INTEGER)
            .pkey(1)
            .build(&mut tx)
            .unwrap();

        let _ = tx.insert_table(&table);

        let dec_table = tx.get_table("mytable").unwrap();
        assert_eq!(*dec_table, table);

        // should reject duplicate
        assert!(tx.insert_table(&table).is_err());

        db.commit(tx).unwrap();
        cleanup_file(path);
    }

    #[test]
    fn records_insert_search() -> Result<()> {
        let path = "test-files/records_insert_search.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let table = TableBuilder::new()
            .id(3)
            .name("mytable")
            .add_col("name", TypeCol::BYTES)
            .add_col("age", TypeCol::INTEGER)
            .add_col("id", TypeCol::INTEGER)
            .pkey(1)
            .build(&mut tx)?;

        tx.insert_table(&table)?;

        let mut entries = vec![];
        entries.push(Record::new().add("Alice").add(20).add(1));
        entries.push(Record::new().add("Bob").add(15).add(2));
        entries.push(Record::new().add("Charlie").add(25).add(3));

        for entry in entries {
            tx.insert_rec(entry, &table, SetFlag::UPSERT)?;
        }

        let q1 = Query::with_key(&table).add("name", "Alice").encode()?;
        let q2 = Query::with_key(&table).add("name", "Bob").encode()?;
        let q3 = Query::with_key(&table).add("name", "Charlie").encode()?;

        let q1_res = tx.tree.get(q1).unwrap().decode();
        assert_eq!(q1_res[0], DataCell::Int(20));
        assert_eq!(q1_res[1], DataCell::Int(1));

        let q2_res = tx.tree.get(q2).unwrap().decode();
        assert_eq!(q2_res[0], DataCell::Int(15));
        assert_eq!(q2_res[1], DataCell::Int(2));

        let q3_res = tx.tree.get(q3).unwrap().decode();
        assert_eq!(q3_res[0], DataCell::Int(25));
        assert_eq!(q3_res[1], DataCell::Int(3));

        db.commit(tx)?;
        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn query_input() {
        let path = "test-files/query_input.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let table = TableBuilder::new()
            .id(3)
            .name("mytable")
            .add_col("name", TypeCol::BYTES)
            .add_col("age", TypeCol::INTEGER)
            .add_col("id", TypeCol::INTEGER)
            .pkey(2)
            .build(&mut tx)
            .unwrap();

        tx.insert_table(&table).unwrap();

        let good_query = Query::with_key(&table).add("name", "Alice").add("age", 10);
        assert!(good_query.encode().is_ok());

        let good_query = Query::with_key(&table).add("name", "Alice").add("age", 10);
        let unordered = Query::with_key(&table).add("age", 10).add("name", "Alice");
        assert_eq!(good_query.encode().unwrap(), unordered.encode().unwrap());

        let bad_query = Query::with_key(&table).add("name", "Alice");
        assert!(bad_query.encode().is_err());

        let bad_query = Query::with_key(&table).add("dfasdf", "fasdf");
        assert!(bad_query.encode().is_err());

        let bad_query = Query::with_key(&table);
        assert!(bad_query.encode().is_err());

        db.commit(tx).unwrap();
        cleanup_file(path);
    }

    #[test]
    fn table_ids() {
        let path = "test-files/table_ids.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);
        assert_eq!(tx.new_tid().unwrap(), 3);
        assert_eq!(tx.new_tid().unwrap(), 4);
        assert_eq!(tx.new_tid().unwrap(), 5);
        db.commit(tx).unwrap();
        cleanup_file(path);
    }

    #[test]
    fn table_builder_validations() {
        let path = "test-files/table_builder_validations.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        // empty name
        assert!(
            TableBuilder::new()
                .name("")
                .id(10)
                .add_col("a", TypeCol::BYTES)
                .pkey(1)
                .build(&mut tx)
                .is_err()
        );

        // zero pkeys
        assert!(
            TableBuilder::new()
                .name("t")
                .id(11)
                .add_col("a", TypeCol::BYTES)
                .pkey(0)
                .build(&mut tx)
                .is_err()
        );

        // more pkeys than cols
        assert!(
            TableBuilder::new()
                .name("t")
                .id(12)
                .add_col("a", TypeCol::BYTES)
                .pkey(2)
                .build(&mut tx)
                .is_err()
        );

        // not enough columns (less than required for your logic)
        assert!(
            TableBuilder::new()
                .name("t")
                .id(13)
                .pkey(1)
                .build(&mut tx)
                .is_err()
        );

        db.commit(tx).unwrap();
        cleanup_file(path);
    }

    #[test]
    fn duplicate_table_name_rejected() {
        let path = "test-files/duplicate_table_name_rejected.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let table = TableBuilder::new()
            .id(12)
            .name("dup")
            .add_col("x", TypeCol::BYTES)
            .pkey(1)
            .build(&mut tx)
            .unwrap();

        assert!(tx.insert_table(&table).is_ok());
        assert!(tx.insert_table(&table).is_err());

        db.commit(tx).unwrap();
        cleanup_file(path);
    }

    #[test]
    fn drop_table_removes_table() {
        let path = "test-files/drop_table_removes_table.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let table = TableBuilder::new()
            .id(13)
            .name("droppable")
            .add_col("x", TypeCol::BYTES)
            .pkey(1)
            .build(&mut tx)
            .unwrap();

        tx.insert_table(&table).unwrap();
        assert!(tx.get_table("droppable").is_some());
        assert!(tx.drop_table("droppable").is_err());

        db.commit(tx).unwrap();
        cleanup_file(path);
    }

    #[test]
    fn new_tid_persists() {
        let path = "test-files/tid_persist.rdb";
        cleanup_file(path);
        {
            let db = Arc::new(KVDB::new(path));
            let mut tx = db.begin(&db, TXKind::Write);
            assert_eq!(tx.new_tid().unwrap(), 3);
            assert_eq!(tx.new_tid().unwrap(), 4);
            db.commit(tx).unwrap();
        }
        // reopen
        {
            let db = Arc::new(KVDB::new(path));
            let mut tx = db.begin(&db, TXKind::Write);
            // next tid continues
            assert_eq!(tx.new_tid().unwrap(), 5);
            db.commit(tx).unwrap();
        }
        cleanup_file(path);
    }

    #[test]
    fn invalid_queries_rejected() {
        let path = "test-files/invalid_queries_rejected.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let table = TableBuilder::new()
            .id(16)
            .name("invalid")
            .add_col("x", TypeCol::BYTES)
            .pkey(1)
            .build(&mut tx)
            .unwrap();

        tx.insert_table(&table).unwrap();

        // missing primary key
        assert!(Query::with_key(&table).encode().is_err());

        // wrong column name
        assert!(Query::with_key(&table).add("nope", "x").encode().is_err());

        db.commit(tx).unwrap();
        cleanup_file(path);
    }

    #[test]
    fn scan_open() -> Result<()> {
        let path = "test-files/scan_open.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let table1 = TableBuilder::new()
            .id(5)
            .name("table_1")
            .add_col("name", TypeCol::BYTES)
            .add_col("age", TypeCol::INTEGER)
            .pkey(1)
            .build(&mut tx)?;

        let table2 = TableBuilder::new()
            .id(7)
            .name("table_2")
            .add_col("id", TypeCol::INTEGER)
            .add_col("name", TypeCol::BYTES)
            .add_col("job", TypeCol::BYTES)
            .pkey(1)
            .build(&mut tx)?;

        tx.insert_table(&table1)?;
        tx.insert_table(&table2)?;

        assert!(tx.get_table("table_1").is_some());
        assert!(tx.get_table("table_2").is_some());

        let mut entries_t1 = vec![];
        entries_t1.push(Record::new().add("Alice").add(20));
        entries_t1.push(Record::new().add("Bob").add(15));
        entries_t1.push(Record::new().add("Charlie").add(25));

        for entry in entries_t1 {
            tx.insert_rec(entry, &table1, SetFlag::UPSERT)?;
        }

        let mut entries_t2 = vec![];
        entries_t2.push(Record::new().add(20).add("Alice").add("teacher"));
        entries_t2.push(Record::new().add(15).add("Bob").add("clerk"));
        entries_t2.push(Record::new().add(25).add("Charlie").add("fire fighter"));

        for entry in entries_t2 {
            tx.insert_rec(entry, &table2, SetFlag::UPSERT)?;
        }

        let open = ScanMode::new_open(
            Query::with_key(&table1).add("name", "Alice").encode()?,
            Compare::GE,
        )?;
        let res: Vec<_> = tx.tree.scan(open)?.collect_records();

        assert_eq!(res.len(), 3);
        assert_eq!(res[0].to_string(), "Alice 20");
        assert_eq!(res[1].to_string(), "Bob 15");
        assert_eq!(res[2].to_string(), "Charlie 25");

        let open = ScanMode::new_open(
            Query::with_key(&table2).add("id", 20).encode()?,
            Compare::GE,
        )?;
        let res: Vec<_> = tx.tree.scan(open)?.collect_records();

        assert_eq!(res.len(), 2);
        assert_eq!(res[0].to_string(), "20 Alice teacher");
        assert_eq!(res[1].to_string(), "25 Charlie fire fighter");

        db.commit(tx)?;
        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn full_table_scan_seek() -> Result<()> {
        let path = "test-files/full_table_scan_seek.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let table1 = TableBuilder::new()
            .id(5)
            .name("table_1")
            .add_col("name", TypeCol::BYTES)
            .add_col("age", TypeCol::INTEGER)
            .pkey(1)
            .build(&mut tx)?;

        let table2 = TableBuilder::new()
            .id(7)
            .name("table_2")
            .add_col("id", TypeCol::INTEGER)
            .add_col("name", TypeCol::BYTES)
            .add_col("job", TypeCol::BYTES)
            .pkey(1)
            .build(&mut tx)?;

        tx.insert_table(&table1)?;
        tx.insert_table(&table2)?;

        let mut entries_t1 = vec![];
        entries_t1.push(Record::new().add("Alice").add(20));
        entries_t1.push(Record::new().add("Bob").add(15));
        entries_t1.push(Record::new().add("Charlie").add(25));

        for entry in entries_t1 {
            tx.insert_rec(entry, &table1, SetFlag::UPSERT)?;
        }

        let mut entries_t2 = vec![];
        entries_t2.push(Record::new().add(20).add("Alice").add("teacher"));
        entries_t2.push(Record::new().add(15).add("Bob").add("clerk"));
        entries_t2.push(Record::new().add(25).add("Charlie").add("fire fighter"));

        for entry in entries_t2 {
            tx.insert_rec(entry, &table2, SetFlag::UPSERT)?;
        }

        let res = tx.full_table_scan(&table1)?;
        let records: Vec<_> = res.collect_records();
        assert_eq!(records.len(), 3);

        let res = tx.full_table_scan(&table2)?;
        let records: Vec<_> = res.collect_records();
        assert_eq!(records.len(), 3);

        db.commit(tx)?;
        cleanup_file(path);
        Ok(())
    }
}

// these tests were cooked up by claude, good luck!
#[cfg(test)]
mod scan {
    use crate::database::api::{kvdb::KVDB, tx::TXKind};
    use crate::database::btree::Compare;
    use crate::database::btree::{ScanMode, SetFlag};
    use crate::database::pager::transaction::Transaction;
    use crate::database::tables::{Query, Record, TypeCol, tables::TableBuilder};
    use std::sync::Arc;

    use super::*;
    use crate::database::helper::cleanup_file;
    use test_log::test;

    #[test]
    fn scan_range_between_keys() -> Result<()> {
        let path = "test-files/scan_range.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let table = TableBuilder::new()
            .id(10)
            .name("range_table")
            .add_col("id", TypeCol::INTEGER)
            .add_col("name", TypeCol::BYTES)
            .pkey(1)
            .build(&mut tx)?;

        tx.insert_table(&table)?;

        // Insert records with ids 1-20
        for i in 1..=20 {
            tx.insert_rec(
                Record::new().add(i as i64).add(format!("name_{}", i)),
                &table,
                SetFlag::UPSERT,
            )?;
        }

        // Scan from id 5 to 15
        let lo_key = Query::with_key(&table).add("id", 5i64).encode()?;
        let hi_key = Query::with_key(&table).add("id", 15i64).encode()?;

        let range = ScanMode::new_range((lo_key, Compare::GE), (hi_key, Compare::LE))?;

        let res: Vec<_> = tx.tree.scan(range)?.collect_records();

        assert_eq!(res.len(), 11); // 5 through 15 inclusive
        assert_eq!(res[0].to_string(), "5 name_5");
        assert_eq!(res[10].to_string(), "15 name_15");

        db.commit(tx)?;
        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn scan_multiple_tables_isolation() -> Result<()> {
        let path = "test-files/scan_isolation.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let table1 = TableBuilder::new()
            .id(20)
            .name("table_a")
            .add_col("key", TypeCol::BYTES)
            .add_col("value", TypeCol::BYTES)
            .pkey(1)
            .build(&mut tx)?;

        let table2 = TableBuilder::new()
            .id(21)
            .name("table_b")
            .add_col("key", TypeCol::BYTES)
            .add_col("value", TypeCol::BYTES)
            .pkey(1)
            .build(&mut tx)?;

        tx.insert_table(&table1)?;
        tx.insert_table(&table2)?;

        // Insert into table1
        for i in 1..=10 {
            tx.insert_rec(
                Record::new()
                    .add(format!("key_a_{}", i))
                    .add(format!("val_a_{}", i)),
                &table1,
                SetFlag::UPSERT,
            )?;
        }

        // Insert into table2
        for i in 1..=10 {
            tx.insert_rec(
                Record::new()
                    .add(format!("key_b_{}", i))
                    .add(format!("val_b_{}", i)),
                &table2,
                SetFlag::UPSERT,
            )?;
        }

        // Scan table1
        let open = ScanMode::new_open(
            Query::with_key(&table1).add("key", "key_a_1").encode()?,
            Compare::GE,
        )?;
        let res1: Vec<_> = tx.tree.scan(open)?.collect_records();

        // Should only contain table1 records
        assert!(res1.iter().all(|r| r.to_string().contains("val_a_")));
        assert_eq!(res1.len(), 10);

        // Scan table2
        let open = ScanMode::new_open(
            Query::with_key(&table2).add("key", "key_b_1").encode()?,
            Compare::GE,
        )?;
        let res2: Vec<_> = tx.tree.scan(open)?.collect_records();

        // Should only contain table2 records
        assert!(res2.iter().all(|r| r.to_string().contains("val_b_")));
        assert_eq!(res2.len(), 10);

        db.commit(tx)?;
        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn scan_with_lt_predicate() -> Result<()> {
        let path = "test-files/scan_lt.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let table = TableBuilder::new()
            .id(30)
            .name("lt_table")
            .add_col("score", TypeCol::INTEGER)
            .add_col("player", TypeCol::BYTES)
            .pkey(1)
            .build(&mut tx)?;

        tx.insert_table(&table)?;

        for i in 1..=10 {
            tx.insert_rec(
                Record::new()
                    .add(i as i64 * 10)
                    .add(format!("player_{}", i)),
                &table,
                SetFlag::UPSERT,
            )?;
        }

        // Scan backwards from score 50
        let open = ScanMode::new_open(
            Query::with_key(&table).add("score", 50i64).encode()?,
            Compare::LT,
        )?;

        let res: Vec<_> = tx.tree.scan(open)?.collect_records();

        // Should return scores 40, 30, 20, 10
        assert_eq!(res.len(), 4);
        assert_eq!(res[0].to_string(), "40 player_4");
        assert_eq!(res[3].to_string(), "10 player_1");

        db.commit(tx)?;
        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn scan_empty_result_set() -> Result<()> {
        let path = "test-files/scan_empty.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let table = TableBuilder::new()
            .id(40)
            .name("empty_table")
            .add_col("id", TypeCol::INTEGER)
            .add_col("data", TypeCol::BYTES)
            .pkey(1)
            .build(&mut tx)?;

        tx.insert_table(&table)?;

        for i in 1..=5 {
            tx.insert_rec(
                Record::new().add(i as i64).add(format!("data_{}", i)),
                &table,
                SetFlag::UPSERT,
            )?;
        }

        // Scan for values greater than max
        let open = ScanMode::new_open(
            Query::with_key(&table).add("id", 100i64).encode()?,
            Compare::GT,
        )?;

        let result = tx.tree.scan(open);
        assert!(result.is_err()); // Should error on empty result

        db.commit(tx)?;
        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn scan_single_record_result() -> Result<()> {
        let path = "test-files/scan_single.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let table = TableBuilder::new()
            .id(50)
            .name("single_table")
            .add_col("code", TypeCol::BYTES)
            .add_col("value", TypeCol::INTEGER)
            .pkey(1)
            .build(&mut tx)?;

        tx.insert_table(&table)?;

        tx.insert_rec(
            Record::new().add("CODE_001").add(42i64),
            &table,
            SetFlag::UPSERT,
        )?;

        let open = ScanMode::new_open(
            Query::with_key(&table).add("code", "CODE_001").encode()?,
            Compare::EQ,
        );
        // EQ is not allowed in open scans, should error
        assert!(open.is_err());

        db.commit(tx)?;
        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn scan_with_le_predicate() -> Result<()> {
        let path = "test-files/scan_le.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let table = TableBuilder::new()
            .id(60)
            .name("le_table")
            .add_col("value", TypeCol::INTEGER)
            .add_col("label", TypeCol::BYTES)
            .pkey(1)
            .build(&mut tx)?;

        tx.insert_table(&table)?;

        for i in 10..=50 {
            tx.insert_rec(
                Record::new().add(i as i64).add(format!("label_{}", i)),
                &table,
                SetFlag::UPSERT,
            )?;
        }

        // Scan from 35 backwards (LE)
        let open = ScanMode::new_open(
            Query::with_key(&table).add("value", 35i64).encode()?,
            Compare::LE,
        )?;

        let res: Vec<_> = tx.tree.scan(open)?.collect_records();

        // Should include 35 and go backwards to 10
        assert_eq!(res.len(), 26); // 35 down to 10
        assert_eq!(res[0].to_string(), "35 label_35");

        db.commit(tx)?;
        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn tx_scan_large_dataset() -> Result<()> {
        let path = "test-files/tx_scan_large.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let table = TableBuilder::new()
            .id(70)
            .name("large_table")
            .add_col("id", TypeCol::INTEGER)
            .add_col("name", TypeCol::BYTES)
            .pkey(1)
            .build(&mut tx)?;

        tx.insert_table(&table)?;

        // Insert 500 records
        for i in 1..=500 {
            tx.insert_rec(
                Record::new().add(i as i64).add(format!("name_{:04}", i)),
                &table,
                SetFlag::UPSERT,
            )?;
        }

        // Scan from id 100 onwards
        let open = ScanMode::new_open(
            Query::with_key(&table).add("id", 100i64).encode()?,
            Compare::GT,
        )?;

        let res: Vec<_> = tx.tree.scan(open)?.collect_records();

        assert_eq!(res.len(), 400); // 101 to 500
        assert_eq!(res[0].to_string(), "101 name_0101");
        assert_eq!(res[399].to_string(), "500 name_0500");

        db.commit(tx)?;
        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn scan_byte_string_keys() -> Result<()> {
        let path = "test-files/scan_bytes.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let table = TableBuilder::new()
            .id(80)
            .name("byte_table")
            .add_col("name", TypeCol::BYTES)
            .add_col("data", TypeCol::BYTES)
            .pkey(1)
            .build(&mut tx)?;

        tx.insert_table(&table)?;

        let names = vec!["alice", "bob", "charlie", "david", "eve"];
        for name in &names {
            tx.insert_rec(
                Record::new().add(*name).add(format!("data_{}", name)),
                &table,
                SetFlag::UPSERT,
            )?;
        }

        // Scan from "bob" onwards
        let open = ScanMode::new_open(
            Query::with_key(&table).add("name", "bob").encode()?,
            Compare::GE,
        )?;

        let res: Vec<_> = tx.tree.scan(open)?.collect_records();

        assert_eq!(res.len(), 4); // bob, charlie, david, eve
        assert_eq!(res[0].to_string(), "bob data_bob");

        db.commit(tx)?;
        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn scan_after_deletes() -> Result<()> {
        let path = "test-files/scan_after_delete.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let table = TableBuilder::new()
            .id(90)
            .name("delete_table")
            .add_col("id", TypeCol::INTEGER)
            .add_col("value", TypeCol::BYTES)
            .pkey(1)
            .build(&mut tx)?;

        tx.insert_table(&table)?;

        // Insert 10 records
        for i in 1..=10 {
            tx.insert_rec(
                Record::new().add(i as i64).add(format!("val_{}", i)),
                &table,
                SetFlag::UPSERT,
            )?;
        }

        // Delete some records
        for i in [3, 5, 7].iter() {
            let key = Query::with_key(&table).add("id", *i as i64).encode()?;
            tx.tree.delete(key)?;
        }

        // Scan from beginning
        let open = ScanMode::new_open(
            Query::with_key(&table).add("id", 1i64).encode()?,
            Compare::GE,
        )?;

        let res: Vec<_> = tx.tree.scan(open)?.collect_records();

        // Should have 7 records (10 - 3 deleted)
        assert_eq!(res.len(), 7);
        assert!(!res.iter().any(|r| r.to_string().contains("val_3")));
        assert!(!res.iter().any(|r| r.to_string().contains("val_5")));
        assert!(!res.iter().any(|r| r.to_string().contains("val_7")));

        db.commit(tx)?;
        cleanup_file(path);
        Ok(())
    }
}
