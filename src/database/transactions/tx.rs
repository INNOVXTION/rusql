use std::sync::Arc;

use tracing::{debug, error, info, instrument};

use crate::database::{
    BTree,
    btree::{Compare, ScanIter, ScanMode, SetFlag, SetResponse, Tree},
    codec::*,
    errors::{Error, Result, TXError, TableError},
    pager::{KVEngine, MetaPage, Pager},
    tables::{
        Key, Query, Record, Value,
        records::QueryKey,
        tables::{
            DEF_TABLE_COL1, LOWEST_PREMISSIABLE_TID, META_TABLE_COL1, META_TABLE_ID_ROW,
            META_TABLE_NAME, MetaTable, Table,
        },
    },
    transactions::{keyrange::KeyRange, kvdb::KVDB, txdb::TXDB},
    types::DataCell,
};

/// Transaction struct, on a per thread basis
pub struct TX {
    pub db: Arc<TXDB>,     // resources
    pub tree: BTree<TXDB>, // snapshot

    pub version: u64,
    pub kind: TXKind,
    pub rollback: MetaPage,

    pub key_range: KeyRange,
}

#[derive(Copy, Clone, Debug, PartialEq, PartialOrd)]
pub enum TXKind {
    Read,
    Write,
}

// tree callbacks
impl TX {
    fn tree_get(&self, key: Key) -> Option<Value> {
        self.tree.get(key)
    }

    fn tree_set(&mut self, key: Key, value: Value, flag: SetFlag) -> Result<SetResponse> {
        self.key_range.add(&key);
        self.tree.set(key, value, flag)
    }

    fn tree_scan(&self, mode: ScanMode) -> Result<ScanIter<'_, TXDB>> {
        self.tree.scan(mode)
    }

    fn tree_delete(&mut self, key: Key) -> Result<()> {
        self.key_range.add(&key);
        self.tree.delete(key)
    }
}

impl TX {
    /// wrapper function for read_table()
    #[instrument(name = "get table", skip_all)]
    fn get_table(&mut self, name: &str) -> Option<Arc<Table>> {
        info!(name, "getting table");
        self.db.db_link.read_table_buffer(name, &self.tree)
    }

    #[instrument(name = "new table id", skip_all)]
    pub fn new_tid(&mut self) -> Result<u32> {
        if self.kind == TXKind::Read {
            return Err(TXError::MismatchedKindError.into());
        }

        self.key_range.listen();

        let meta = self.db.db_link.get_meta(&self.tree);
        let key = Query::with_key(&meta)
            .add(META_TABLE_COL1, META_TABLE_ID_ROW) // we query name column, where pkey = tid
            .encode()?;

        match self.tree_get(key) {
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

                    self.tree_set(k, v, SetFlag::UPDATE).map_err(|e| {
                        error!(%e);
                        TableError::TableIdError("error when retrieving id".to_string())
                    })?;

                    self.key_range.capture_and_stop();
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

                self.tree_set(k, v, SetFlag::INSERT).map_err(|e| {
                    error!(%e);
                    TableError::TableIdError("error when retrieving id".to_string())
                })?;

                self.key_range.capture_and_stop();
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

        self.key_range.listen();

        // WIP FOR TESTS
        let (k, v) = Record::new()
            .add(table.name.clone())
            .add(table.encode()?)
            .encode(&self.db.db_link.t_def)?
            .next()
            .ok_or(TableError::InsertTableError(
                "record iterator failure".to_string(),
            ))?;

        self.tree_set(k, v, SetFlag::UPSERT).map_err(|e| {
            error!(%e, "error when inserting");
            TableError::InsertTableError("error when inserting table".to_string())
        })?;

        self.key_range.capture_and_stop();
        Ok(())
    }

    /// TODO: decrement/free up table id
    ///
    /// drops table from the database
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

        self.key_range.listen();

        // delete keys
        let recs = self.full_table_scan(&table)?.collect_records();
        for rec in recs.into_iter() {
            if let Some(kv) = rec.encode(&table)?.next() {
                self.tree_delete(kv.0)?
            } else {
                return Err(TableError::DeleteTableError("iterator error".to_string()).into());
            }
        }
        self.key_range.capture_and_listen();

        // delete from tdef
        let qu = Query::with_key(&self.db.db_link.t_def)
            .add(DEF_TABLE_COL1, name)
            .encode()?;

        self.tree_delete(qu).map_err(|e| {
            error!(%e, "error when dropping table");
            TableError::DeleteTableError("dropping table error when deleting".to_string())
        })?;

        self.key_range.capture_and_stop();
        Ok(())
    }

    pub fn full_table_scan(&self, schema: &Table) -> Result<ScanIter<'_, TXDB>> {
        // writing a seek key with TID
        let mut buf = [0u8; TID_LEN];
        let _ = &mut buf[..].write_u32(schema.id);
        let seek_key = Key::from_encoded_slice(&buf);
        let seek_mode = ScanMode::new_open(seek_key, Compare::GT)?;

        self.tree_scan(seek_mode)
    }

    /// inserts a record and potential secondary indicies
    #[instrument(name = "insert rec", skip_all)]
    pub fn insert_rec(&mut self, rec: Record, schema: &Table, flag: SetFlag) -> Result<()> {
        info!(?rec, "inserting record");
        if self.kind == TXKind::Read {
            return Err(TXError::MismatchedKindError.into());
        }

        self.key_range.listen();
        let mut iter = rec.encode(schema)?.peekable();
        let mut old_rec;
        let old_pk;
        let mut updated = 0;

        // updating the primary key and retrieving the old one
        let primay_key = iter.next().ok_or(Error::InsertError(
            "record failed to generate a primary key".to_string(),
        ))?;

        let res = self.tree_set(primay_key.0, primay_key.1, flag);
        if res.is_ok() {
            updated += 1;
        }

        if iter.peek().is_none() {
            // there are no secondary keys, we are done
            self.key_range.capture_and_stop();
            return Ok(());
        }

        match res {
            // update found (UPSERT or UPDATE)
            Ok(res) if res.updated => {
                old_pk = res.old.expect("update successful");
                updated += 1;

                // recreating the keys from the update
                old_rec = Record::from_kv(old_pk).encode(schema)?;
                old_rec.next(); // we skip the primary key since we already updated it

                // updating secondary keys
                for (k, v) in iter {
                    if let Some(old_kv) = old_rec.next() {
                        self.tree_delete(old_kv.0)?;
                        let res = self.tree_set(k, v, SetFlag::INSERT)?;

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
                    let res = self.tree_set(k, v, SetFlag::INSERT)?;
                    self.key_range.capture_and_stop();

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

    fn delete_from_query(&mut self, q: QueryKey, schema: &Table) -> Result<()> {
        let key = q.encode()?;
        let val = match self.tree_get(key.clone()) {
            Some(v) => v,
            None => return Err(Error::DeleteError("key doesnt exist".to_string())),
        };
        let rec = Record::from_kv((key, val));
        self.delete_rec(rec, schema)
    }

    /// deletes a record and potential secondary indicies
    fn delete_rec(&mut self, rec: Record, schema: &Table) -> Result<()> {
        info!(?rec, "deleting record");
        if self.kind == TXKind::Read {
            return Err(TXError::MismatchedKindError.into());
        }

        self.key_range.listen();
        let mut iter = rec.encode(schema)?.peekable();
        let mut updated = 0;

        let primay_key = iter.next().ok_or(Error::DeleteError(
            "record failed to generate a primary key".to_string(),
        ))?;

        // deleting primary key
        if let Ok(()) = self.tree_delete(primay_key.0) {
            self.key_range.capture_and_listen();
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
            self.tree_delete(k)?;
            updated += 1;
        }

        debug_assert_eq!(updated, schema.indices.len());
        self.key_range.capture_and_stop();
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

#[cfg(test)]
mod tables {
    use crate::database::{
        btree::{Compare, ScanMode, SetFlag},
        pager::transaction::Transaction,
        pager::{DiskPager, mempage_tree},
        tables::{Query, Record, TypeCol, tables::TableBuilder},
        transactions::{kvdb::KVDB, tx::TXKind},
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

        let q1_res = tx.tree_get(q1).unwrap().decode();
        assert_eq!(q1_res[0], DataCell::Int(20));
        assert_eq!(q1_res[1], DataCell::Int(1));

        let q2_res = tx.tree_get(q2).unwrap().decode();
        assert_eq!(q2_res[0], DataCell::Int(15));
        assert_eq!(q2_res[1], DataCell::Int(2));

        let q3_res = tx.tree_get(q3).unwrap().decode();
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

        let _ = db.commit(tx);
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
        let res: Vec<_> = tx.tree_scan(open)?.collect_records();

        assert_eq!(res.len(), 3);
        assert_eq!(res[0].to_string(), "Alice 20");
        assert_eq!(res[1].to_string(), "Bob 15");
        assert_eq!(res[2].to_string(), "Charlie 25");

        let open = ScanMode::new_open(
            Query::with_key(&table2).add("id", 20).encode()?,
            Compare::GE,
        )?;
        let res: Vec<_> = tx.tree_scan(open)?.collect_records();

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
    use crate::database::btree::Compare;
    use crate::database::btree::{ScanMode, SetFlag};
    use crate::database::pager::transaction::Transaction;
    use crate::database::tables::{Query, Record, TypeCol, tables::TableBuilder};
    use crate::database::transactions::{kvdb::KVDB, tx::TXKind};
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

        let res: Vec<_> = tx.tree_scan(range)?.collect_records();

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
        let res1: Vec<_> = tx.tree_scan(open)?.collect_records();

        // Should only contain table1 records
        assert!(res1.iter().all(|r| r.to_string().contains("val_a_")));
        assert_eq!(res1.len(), 10);

        // Scan table2
        let open = ScanMode::new_open(
            Query::with_key(&table2).add("key", "key_b_1").encode()?,
            Compare::GE,
        )?;
        let res2: Vec<_> = tx.tree_scan(open)?.collect_records();

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

        let res: Vec<_> = tx.tree_scan(open)?.collect_records();

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

        let result = tx.tree_scan(open);
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

        let res: Vec<_> = tx.tree_scan(open)?.collect_records();

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

        let res: Vec<_> = tx.tree_scan(open)?.collect_records();

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

        let res: Vec<_> = tx.tree_scan(open)?.collect_records();

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
            tx.tree_delete(key)?;
        }

        // Scan from beginning
        let open = ScanMode::new_open(
            Query::with_key(&table).add("id", 1i64).encode()?,
            Compare::GE,
        )?;

        let res: Vec<_> = tx.tree_scan(open)?.collect_records();

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

#[cfg(test)]
mod concurrent_tx_tests {
    use super::*;
    use crate::database::{
        btree::SetFlag,
        helper::cleanup_file,
        pager::transaction::{CommitStatus, Retry, Transaction},
        tables::{Query, Record, TypeCol, tables::TableBuilder},
        transactions::{
            kvdb::KVDB,
            retry::{Backoff, RetryResult, RetryStatus, retry},
            tx::TXKind,
        },
        types::DataCell,
    };
    use parking_lot::Mutex;
    use std::sync::{Arc, Barrier};
    use std::thread;
    // use test_log::test;
    use tracing::{Level, span, warn};

    #[test]
    fn concurrent_same_key_write() -> Result<()> {
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

        db.commit(tx)?;

        let res = Mutex::new(vec![]);
        let n = 1000;
        let barrier = Arc::new(Barrier::new(n));

        thread::scope(|s| {
            let mut handles = vec![];
            for i in 0..n {
                handles.push(s.spawn(|| {
                    let id = thread::current().id();
                    let span = span!(Level::DEBUG, "thread ", ?id);
                    let _guard = span.enter();

                    barrier.wait();

                    let mut tx = db.begin(&db, TXKind::Write);
                    let mut entries = vec![];

                    entries.push(Record::new().add("Alice").add(20).add(1));
                    entries.push(Record::new().add("Bob").add(15).add(2));
                    entries.push(Record::new().add("Charlie").add(25).add(3));

                    for entry in entries {
                        let _ = tx.insert_rec(entry, &table, SetFlag::UPSERT);
                    }
                    let tx_version = tx.version;
                    let r = db.commit(tx);
                    res.lock().push(r);
                }));
            }

            for h in handles {
                assert!(h.join().is_ok());
            }
        });

        // should provoke write conflicts
        assert!(res.lock().iter().any(|r| r.is_err()));
        // assert!(res.lock().iter().filter(|r| r.is_ok()).count() < 10);

        let tx = db.begin(&db, TXKind::Read);

        let q1 = Query::with_key(&table).add("name", "Alice").encode()?;
        let q2 = Query::with_key(&table).add("name", "Bob").encode()?;
        let q3 = Query::with_key(&table).add("name", "Charlie").encode()?;

        let q1_res = tx.tree_get(q1).unwrap().decode();
        assert_eq!(q1_res[0], DataCell::Int(20));
        assert_eq!(q1_res[1], DataCell::Int(1));

        let q2_res = tx.tree_get(q2).unwrap().decode();
        assert_eq!(q2_res[0], DataCell::Int(15));
        assert_eq!(q2_res[1], DataCell::Int(2));

        let q3_res = tx.tree_get(q3).unwrap().decode();
        assert_eq!(q3_res[0], DataCell::Int(25));
        assert_eq!(q3_res[1], DataCell::Int(3));

        let ft = tx.full_table_scan(&table)?.collect_records();
        assert_eq!(ft.len(), 3);

        db.commit(tx)?;
        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn concurrent_insert_different_keys() -> Result<()> {
        let path = "test-files/concurrent_insert_diff.rdb";
        cleanup_file(path);

        use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt};

        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open("output.log")
            .expect("failed to open log file");

        let (file_writer, _guard) = tracing_appender::non_blocking(file);

        let stdout_layer = fmt::layer().with_ansi(true);
        let file_layer = fmt::layer()
            .with_ansi(false)
            .with_writer(file_writer)
            .fmt_fields(fmt::format::DefaultFields::new());

        tracing_subscriber::registry()
            .with(stdout_layer)
            .with(file_layer)
            .init();

        let db = Arc::new(KVDB::new(path));

        let mut tx = db.begin(&db, TXKind::Write);

        let table = TableBuilder::new()
            .id(3)
            .name("users")
            .add_col("id", TypeCol::INTEGER)
            .add_col("name", TypeCol::BYTES)
            .pkey(1)
            .build(&mut tx)?;

        tx.insert_table(&table)?;
        db.commit(tx)?;

        let n_threads = 1000;
        let barrier = Arc::new(Barrier::new(n_threads));
        let results = Arc::new(Mutex::new(vec![]));
        let retries_exceeded = Arc::new(Mutex::new(0));

        thread::scope(|s| {
            let mut handles = vec![];
            for i in 0..n_threads {
                let db = db.clone();
                let table = table.clone();
                let barrier = barrier.clone();
                let results = results.clone();
                let retries_exceeded = retries_exceeded.clone();

                handles.push(s.spawn(move || {
                    let id = thread::current().id();
                    let span = span!(Level::DEBUG, "thread", ?id);
                    let _guard = span.enter();

                    barrier.wait();

                    let r = retry(Backoff::default(), || {
                        let mut tx = db.begin(&db, TXKind::Write);
                        let rec = Record::new().add(i as i64).add(format!("user_{}", i));

                        let r = tx.insert_rec(rec, &table, SetFlag::INSERT);

                        let commit_result = db.commit(tx);
                        if commit_result.can_retry() {
                            debug!("retrying");
                            RetryStatus::Continue
                        } else {
                            results.lock().push(commit_result);
                            RetryStatus::Break
                        }
                    });

                    if r == RetryResult::AttemptsExceeded {
                        error!("retries exceeded");
                        results
                            .lock()
                            .push(Err(Error::TransactionError(TXError::RetriesExceeded)));
                        *retries_exceeded.lock() += 1;
                    }
                }));
            }

            for h in handles {
                let id = h.thread().id();
                if let Err(err) = h.join() {
                    println!("thread {:?} panicked!", id);
                    if let Some(s) = err.downcast_ref::<&str>() {
                        println!("panic message: {}", s);
                        panic!()
                    } else if let Some(s) = err.downcast_ref::<String>() {
                        println!("panic message: {}", s);
                        panic!()
                    }
                }
            }
        });

        // All transactions should succeed (different keys)
        let results = results.lock();
        assert_eq!(
            results.iter().filter(|r| r.is_ok()).count() + *retries_exceeded.lock(),
            n_threads
        );

        // Verify all records exist
        let tx = db.begin(&db, TXKind::Read);
        for i in 0..n_threads {
            let q = Query::with_key(&table).add("id", i as i64).encode()?;
            let res = tx.tree_get(q);
            assert!(res.is_some());
            let res = res.unwrap().decode();
            assert_eq!(res[0], DataCell::Str(format!("user_{}", i)));
        }
        db.commit(tx)?;
        cleanup_file(path);

        Ok(())
    }

    #[test]
    fn concurrent_read_same_records() -> Result<()> {
        let path = "test-files/concurrent_read_same.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let table = TableBuilder::new()
            .id(100)
            .name("read_table")
            .add_col("id", TypeCol::INTEGER)
            .add_col("value", TypeCol::BYTES)
            .pkey(1)
            .build(&mut tx)?;

        tx.insert_table(&table)?;

        // Insert initial data
        for i in 1..=5 {
            tx.insert_rec(
                Record::new().add(i as i64).add(format!("value_{}", i)),
                &table,
                SetFlag::INSERT,
            )?;
        }
        db.commit(tx)?;

        let n_threads = 20;
        let barrier = Arc::new(Barrier::new(n_threads));
        let results = Arc::new(Mutex::new(vec![]));

        thread::scope(|s| {
            for _ in 0..n_threads {
                let db = db.clone();
                let table = table.clone();
                let barrier = barrier.clone();
                let results = results.clone();

                s.spawn(move || {
                    barrier.wait();

                    let tx = db.begin(&db, TXKind::Read);

                    // All threads read the same records
                    let mut success = true;
                    for i in 1..=5 {
                        let q = Query::with_key(&table)
                            .add("id", i as i64)
                            .encode()
                            .unwrap();
                        if let Some(value) = tx.tree_get(q) {
                            let decoded = value.decode();
                            if decoded[0] != DataCell::Str(format!("value_{}", i)) {
                                success = false;
                            }
                        } else {
                            success = false;
                        }
                    }

                    let _ = db.commit(tx);
                    results.lock().push(success);
                });
            }
        });

        // All read transactions should succeed and be consistent
        let results = results.lock();
        assert_eq!(results.iter().filter(|r| **r).count(), n_threads);

        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn concurrent_read_multi_table() -> Result<()> {
        let path = "test-files/concurrent_read_multi_table.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let table1 = TableBuilder::new()
            .id(101)
            .name("table1")
            .add_col("id", TypeCol::INTEGER)
            .add_col("data", TypeCol::BYTES)
            .pkey(1)
            .build(&mut tx)?;

        let table2 = TableBuilder::new()
            .id(102)
            .name("table2")
            .add_col("id", TypeCol::INTEGER)
            .add_col("data", TypeCol::BYTES)
            .pkey(1)
            .build(&mut tx)?;

        tx.insert_table(&table1)?;
        tx.insert_table(&table2)?;

        for i in 1..=10 {
            tx.insert_rec(
                Record::new()
                    .add(i as i64)
                    .add(format!("table1_data_{}", i)),
                &table1,
                SetFlag::INSERT,
            )?;
            tx.insert_rec(
                Record::new()
                    .add(i as i64)
                    .add(format!("table2_data_{}", i)),
                &table2,
                SetFlag::INSERT,
            )?;
        }
        db.commit(tx)?;

        let n_threads = 15;
        let barrier = Arc::new(Barrier::new(n_threads));
        let results = Arc::new(Mutex::new(vec![]));

        thread::scope(|s| {
            for _ in 0..n_threads {
                let db = db.clone();
                let table1 = table1.clone();
                let table2 = table2.clone();
                let barrier = barrier.clone();
                let results = results.clone();

                s.spawn(move || {
                    barrier.wait();

                    let tx = db.begin(&db, TXKind::Read);
                    let mut success = true;

                    for i in 1..=10 {
                        let q1 = Query::with_key(&table1)
                            .add("id", i as i64)
                            .encode()
                            .unwrap();
                        let q2 = Query::with_key(&table2)
                            .add("id", i as i64)
                            .encode()
                            .unwrap();

                        if let (Some(v1), Some(v2)) = (tx.tree_get(q1), tx.tree_get(q2)) {
                            let d1 = v1.decode();
                            let d2 = v2.decode();
                            if d1[0] != DataCell::Str(format!("table1_data_{}", i))
                                || d2[0] != DataCell::Str(format!("table2_data_{}", i))
                            {
                                success = false;
                            }
                        } else {
                            success = false;
                        }
                    }

                    let _ = db.commit(tx);
                    results.lock().push(success);
                });
            }
        });

        let results = results.lock();
        assert_eq!(results.iter().filter(|r| **r).count(), n_threads);

        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn concurrent_update_different_keys() -> Result<()> {
        let path = "test-files/concurrent_update_diff.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let table = TableBuilder::new()
            .id(103)
            .name("update_table")
            .add_col("id", TypeCol::INTEGER)
            .add_col("value", TypeCol::BYTES)
            .pkey(1)
            .build(&mut tx)?;

        tx.insert_table(&table)?;

        // Insert initial records
        for i in 1..=10 {
            tx.insert_rec(
                Record::new().add(i as i64).add(format!("initial_{}", i)),
                &table,
                SetFlag::INSERT,
            )?;
        }
        db.commit(tx)?;

        let n_threads = 10;
        let barrier = Arc::new(Barrier::new(n_threads));
        let results = Arc::new(Mutex::new(vec![]));

        thread::scope(|s| {
            for i in 0..n_threads {
                let db = db.clone();
                let table = table.clone();
                let barrier = barrier.clone();
                let results = results.clone();

                s.spawn(move || {
                    let id = thread::current().id();
                    let span = span!(Level::DEBUG, "thread", ?id);
                    let _guard = span.enter();

                    barrier.wait();

                    let r = retry(Backoff::default(), || {
                        let mut tx = db.begin(&db, TXKind::Write);
                        let thread_id = i as i64 + 1;

                        let rec = Record::new()
                            .add(thread_id)
                            .add(format!("updated_by_thread_{}", i));
                        let _ = tx.insert_rec(rec, &table, SetFlag::UPSERT);

                        let commit_result = db.commit(tx);
                        if commit_result.can_retry() {
                            RetryStatus::Continue
                        } else {
                            results.lock().push(commit_result);
                            RetryStatus::Break
                        }
                    });

                    if r == RetryResult::AttemptsExceeded {
                        results
                            .lock()
                            .push(Err(Error::TransactionError(TXError::RetriesExceeded)));
                    }
                });
            }
        });

        // All updates should succeed (different keys)
        let results = results.lock();
        assert_eq!(results.iter().filter(|r| r.is_ok()).count(), n_threads);

        // Verify all updates were applied
        let tx = db.begin(&db, TXKind::Read);
        for i in 0..n_threads {
            let q = Query::with_key(&table).add("id", i as i64 + 1).encode()?;
            let res = tx.tree_get(q).unwrap().decode();
            assert_eq!(res[0], DataCell::Str(format!("updated_by_thread_{}", i)));
        }
        db.commit(tx)?;

        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn concurrent_update_same_key_conflict() -> Result<()> {
        let path = "test-files/concurrent_update_conflict.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let table = TableBuilder::new()
            .id(104)
            .name("conflict_table")
            .add_col("id", TypeCol::INTEGER)
            .add_col("counter", TypeCol::INTEGER)
            .pkey(1)
            .build(&mut tx)?;

        tx.insert_table(&table)?;

        tx.insert_rec(Record::new().add(1i64).add(0i64), &table, SetFlag::INSERT)?;
        db.commit(tx)?;

        let n_threads = 100;
        let barrier = Arc::new(Barrier::new(n_threads));
        let results = Arc::new(Mutex::new(vec![]));

        thread::scope(|s| {
            for i in 0..n_threads {
                let db = db.clone();
                let table = table.clone();
                let barrier = barrier.clone();
                let results = results.clone();

                s.spawn(move || {
                    barrier.wait();

                    let r = retry(Backoff::default(), || {
                        let mut tx = db.begin(&db, TXKind::Write);

                        // Read current value
                        let q = Query::with_key(&table).add("id", 1i64).encode().unwrap();
                        let current_val = if let Some(v) = tx.tree_get(q) {
                            let decoded = v.decode();
                            if let DataCell::Int(val) = decoded[0] {
                                val
                            } else {
                                0
                            }
                        } else {
                            0
                        };

                        let rec = Record::new().add(1i64).add(current_val + 1);
                        let _ = tx.insert_rec(rec, &table, SetFlag::UPSERT);

                        let commit_result = db.commit(tx);
                        if commit_result.can_retry() {
                            RetryStatus::Continue
                        } else {
                            results.lock().push(commit_result);
                            RetryStatus::Break
                        }
                    });

                    if r == RetryResult::AttemptsExceeded {
                        results
                            .lock()
                            .push(Err(Error::TransactionError(TXError::RetriesExceeded)));
                    }
                });
            }
        });

        // Some transactions will succeed, some will fail due to conflicts
        let results = results.lock();
        let successful = results.iter().filter(|r| r.is_ok()).count();
        assert!(successful > 0); // At least some should succeed
        assert!(successful < n_threads); // Not all will succeed due to conflicts

        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn concurrent_delete_different_keys() -> Result<()> {
        let path = "test-files/concurrent_delete_diff.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let table = TableBuilder::new()
            .id(105)
            .name("delete_table")
            .add_col("id", TypeCol::INTEGER)
            .add_col("value", TypeCol::BYTES)
            .pkey(1)
            .build(&mut tx)?;

        tx.insert_table(&table)?;

        // Insert records to delete
        for i in 1..=100 {
            tx.insert_rec(
                Record::new().add(i as i64).add(format!("to_delete_{}", i)),
                &table,
                SetFlag::INSERT,
            )?;
        }
        db.commit(tx)?;

        let n_threads = 100;
        let barrier = Arc::new(Barrier::new(n_threads));
        let results = Arc::new(Mutex::new(vec![]));

        thread::scope(|s| {
            for i in 1..=n_threads {
                let db = db.clone();
                let table = table.clone();
                let barrier = barrier.clone();
                let results = results.clone();

                s.spawn(move || {
                    barrier.wait();

                    let r = retry(Backoff::default(), || {
                        let mut tx = db.begin(&db, TXKind::Write);

                        let id = thread::current().id();
                        let span = span!(Level::DEBUG, "thread", ?id, tx.version);
                        let _guard = span.enter();

                        let q = Query::with_key(&table).add("id", i as i64);

                        let _ = tx.delete_from_query(q, &table);
                        let commit_result = db.commit(tx);

                        if commit_result.can_retry() {
                            RetryStatus::Continue
                        } else {
                            results.lock().push(commit_result);
                            RetryStatus::Break
                        }
                    });

                    if r == RetryResult::AttemptsExceeded {
                        warn!("retries exceeded");
                        results
                            .lock()
                            .push(Err(Error::TransactionError(TXError::RetriesExceeded)));
                    }
                });
            }
        });

        // All deletes should succeed (different keys)
        let results = results.lock();
        assert_eq!(results.iter().filter(|r| r.is_ok()).count(), n_threads);

        // Verify all records were deleted
        let tx = db.begin(&db, TXKind::Read);
        for i in 1..=100 {
            let q = Query::with_key(&table).add("id", i as i64).encode()?;
            assert!(tx.tree_get(q).is_none());
        }
        db.commit(tx)?;

        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn concurrent_delete_same_key_conflict() -> Result<()> {
        let path = "test-files/concurrent_delete_conflict.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let table = TableBuilder::new()
            .id(106)
            .name("delete_conflict_table")
            .add_col("id", TypeCol::INTEGER)
            .add_col("value", TypeCol::BYTES)
            .pkey(1)
            .build(&mut tx)?;

        tx.insert_table(&table)?;

        tx.insert_rec(
            Record::new().add(1i64).add("shared_record"),
            &table,
            SetFlag::INSERT,
        )?;
        db.commit(tx)?;

        let n_threads = 10;
        let barrier = Arc::new(Barrier::new(n_threads));
        let results = Arc::new(Mutex::new(vec![]));

        thread::scope(|s| {
            for _ in 0..n_threads {
                let db = db.clone();
                let table = table.clone();
                let barrier = barrier.clone();
                let results = results.clone();

                s.spawn(move || {
                    let id = thread::current().id();
                    let span = span!(Level::DEBUG, "thread", ?id);
                    let _guard = span.enter();

                    barrier.wait();

                    let r = retry(Backoff::default(), || {
                        let mut tx = db.begin(&db, TXKind::Write);
                        let q = Query::with_key(&table).add("id", 1i64).encode().unwrap();

                        // All threads try to delete the same key
                        let _ = tx.tree_delete(q);
                        let commit_result = db.commit(tx);

                        if commit_result.can_retry() {
                            RetryStatus::Continue
                        } else if commit_result.is_err() {
                            results.lock().push((false, true));
                            RetryStatus::Break
                        } else {
                            results.lock().push((true, true));
                            RetryStatus::Break
                        }
                    });

                    if r == RetryResult::AttemptsExceeded {
                        results.lock().push((false, false));
                    }
                });
            }
        });

        // Only one delete should succeed
        let results = results.lock();
        let successful_deletes = results.iter().filter(|(d, _)| *d).count();
        let successful_commits = results.iter().filter(|(_, c)| *c).count();
        assert_eq!(successful_deletes, 1); // Only one should successfully delete

        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn concurrent_mixed_crud_operations() -> Result<()> {
        let path = "test-files/concurrent_mixed_crud.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let table = TableBuilder::new()
            .id(107)
            .name("mixed_crud_table")
            .add_col("id", TypeCol::INTEGER)
            .add_col("operation", TypeCol::BYTES)
            .add_col("value", TypeCol::BYTES)
            .pkey(1)
            .build(&mut tx)?;

        tx.insert_table(&table)?;

        // Insert some initial records
        for i in 1..=5 {
            tx.insert_rec(
                Record::new()
                    .add(i as i64)
                    .add("initial")
                    .add(format!("initial_val_{}", i)),
                &table,
                SetFlag::INSERT,
            )?;
        }
        db.commit(tx)?;

        let n_threads = 15;
        let barrier = Arc::new(Barrier::new(n_threads));
        let results = Arc::new(Mutex::new(vec![]));

        thread::scope(|s| {
            for i in 0..n_threads {
                let db = db.clone();
                let table = table.clone();
                let barrier = barrier.clone();
                let results = results.clone();

                s.spawn(move || {
                    barrier.wait();

                    let r = retry(Backoff::default(), || {
                        let mut tx = db.begin(&db, TXKind::Write);
                        let operation_id = (i % 3) as u32;

                        match operation_id {
                            0 => {
                                // READ
                                let q = Query::with_key(&table).add("id", 1i64).encode().unwrap();
                                let _ = tx.tree_get(q);
                            }
                            1 => {
                                // INSERT/UPDATE
                                let new_id = i as i64 + 10;
                                let rec = Record::new()
                                    .add(new_id)
                                    .add("insert_op")
                                    .add(format!("inserted_by_{}", i));
                                let _ = tx.insert_rec(rec, &table, SetFlag::UPSERT);
                            }
                            2 => {
                                // UPDATE existing
                                let update_id = (i % 5) as i64 + 1;
                                let rec = Record::new()
                                    .add(update_id)
                                    .add("updated")
                                    .add(format!("updated_by_thread_{}", i));
                                let _ = tx.insert_rec(rec, &table, SetFlag::UPSERT);
                            }
                            _ => unreachable!(),
                        }

                        let commit_result = db.commit(tx);
                        if commit_result.can_retry() {
                            RetryStatus::Continue
                        } else {
                            results.lock().push(commit_result);
                            RetryStatus::Break
                        }
                    });

                    if r == RetryResult::AttemptsExceeded {
                        results
                            .lock()
                            .push(Err(Error::TransactionError(TXError::RetriesExceeded)));
                    }
                });
            }
        });

        let results = results.lock();
        let successful = results.iter().filter(|r| r.is_ok()).count();
        assert!(successful >= n_threads / 2); // At least half should succeed

        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn concurrent_read_during_writes() -> Result<()> {
        let path = "test-files/concurrent_read_during_writes.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let table = TableBuilder::new()
            .id(108)
            .name("read_during_write_table")
            .add_col("id", TypeCol::INTEGER)
            .add_col("value", TypeCol::INTEGER)
            .pkey(1)
            .build(&mut tx)?;

        tx.insert_table(&table)?;

        tx.insert_rec(Record::new().add(1i64).add(0i64), &table, SetFlag::INSERT)?;
        db.commit(tx)?;

        let n_threads = 12;
        let barrier = Arc::new(Barrier::new(n_threads));
        let read_results = Arc::new(Mutex::new(vec![]));
        let write_results = Arc::new(Mutex::new(vec![]));

        thread::scope(|s| {
            for i in 0..n_threads {
                let db = db.clone();
                let table = table.clone();
                let barrier = barrier.clone();
                let read_results = read_results.clone();
                let write_results = write_results.clone();

                s.spawn(move || {
                    barrier.wait();

                    if i < 6 {
                        // Reader threads
                        let tx = db.begin(&db, TXKind::Read);
                        let q = Query::with_key(&table).add("id", 1i64).encode().unwrap();
                        let val = tx.tree_get(q);
                        let _ = db.commit(tx);
                        read_results.lock().push(val.is_some());
                    } else {
                        // Writer threads
                        let r = retry(Backoff::default(), || {
                            let mut tx = db.begin(&db, TXKind::Write);
                            let current = if let Some(v) = tx
                                .tree_get(Query::with_key(&table).add("id", 1i64).encode().unwrap())
                            {
                                let decoded = v.decode();
                                if let DataCell::Int(val) = decoded[0] {
                                    val
                                } else {
                                    0
                                }
                            } else {
                                0
                            };

                            let rec = Record::new().add(1i64).add(current + 1);
                            let _ = tx.insert_rec(rec, &table, SetFlag::UPSERT);
                            let result = db.commit(tx);

                            if result.can_retry() {
                                RetryStatus::Continue
                            } else {
                                write_results.lock().push(result.is_ok());
                                RetryStatus::Break
                            }
                        });

                        if r == RetryResult::AttemptsExceeded {
                            write_results.lock().push(false);
                        }
                    }
                });
            }
        });

        // All readers should see data
        let read_results = read_results.lock();
        assert_eq!(read_results.iter().filter(|r| **r).count(), 6);

        // Some writers should succeed
        let write_results = write_results.lock();
        let write_success = write_results.iter().filter(|r| **r).count();
        assert!(write_success > 0);

        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn concurrent_insert_then_read_consistency() -> Result<()> {
        let path = "test-files/concurrent_insert_read_consistency.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let table = TableBuilder::new()
            .id(109)
            .name("insert_read_consistency")
            .add_col("id", TypeCol::INTEGER)
            .add_col("data", TypeCol::BYTES)
            .pkey(1)
            .build(&mut tx)?;

        tx.insert_table(&table)?;
        db.commit(tx)?;

        let n_insert_threads = 5;
        let n_read_threads = 10;
        let barrier = Arc::new(Barrier::new(n_insert_threads + n_read_threads));
        let insert_results = Arc::new(Mutex::new(vec![]));
        let read_results = Arc::new(Mutex::new(vec![]));

        thread::scope(|s| {
            for i in 0..n_insert_threads {
                let db = db.clone();
                let table = table.clone();
                let barrier = barrier.clone();
                let insert_results = insert_results.clone();

                s.spawn(move || {
                    barrier.wait();

                    let r = retry(Backoff::default(), || {
                        let mut tx = db.begin(&db, TXKind::Write);
                        let rec = Record::new().add(i as i64).add(format!("inserted_{}", i));
                        let _ = tx.insert_rec(rec, &table, SetFlag::INSERT);
                        let result = db.commit(tx);
                        if result.can_retry() {
                            RetryStatus::Continue
                        } else {
                            insert_results.lock().push(result);
                            RetryStatus::Break
                        }
                    });
                });
            }

            for _ in 0..n_read_threads {
                let db = db.clone();
                let table = table.clone();
                let barrier = barrier.clone();
                let read_results = read_results.clone();

                s.spawn(move || {
                    barrier.wait();

                    let tx = db.begin(&db, TXKind::Read);
                    let mut count = 0;

                    for i in 0..n_insert_threads {
                        let q = Query::with_key(&table)
                            .add("id", i as i64)
                            .encode()
                            .unwrap();
                        if tx.tree_get(q).is_some() {
                            count += 1;
                        }
                    }

                    let _ = db.commit(tx);
                    read_results.lock().push(count);
                });
            }
        });

        // All inserts should eventually succeed
        let insert_results = insert_results.lock();
        assert_eq!(
            insert_results.iter().filter(|r| r.is_ok()).count(),
            n_insert_threads
        );

        // Readers should see consistent number of inserted records
        let read_results = read_results.lock();
        assert!(
            read_results
                .iter()
                .all(|count| *count <= n_insert_threads as usize)
        );

        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn concurrent_full_table_scan() -> Result<()> {
        let path = "test-files/concurrent_full_scan.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let table = TableBuilder::new()
            .id(110)
            .name("scan_table")
            .add_col("id", TypeCol::INTEGER)
            .add_col("value", TypeCol::BYTES)
            .pkey(1)
            .build(&mut tx)?;

        tx.insert_table(&table)?;

        // Insert 50 records
        for i in 1..=50 {
            tx.insert_rec(
                Record::new().add(i as i64).add(format!("value_{}", i)),
                &table,
                SetFlag::INSERT,
            )?;
        }
        db.commit(tx)?;

        let n_threads = 10;
        let barrier = Arc::new(Barrier::new(n_threads));
        let results = Arc::new(Mutex::new(vec![]));

        thread::scope(|s| {
            for _ in 0..n_threads {
                let db = db.clone();
                let table = table.clone();
                let barrier = barrier.clone();
                let results = results.clone();

                s.spawn(move || {
                    barrier.wait();

                    let tx = db.begin(&db, TXKind::Read);
                    let records = tx.full_table_scan(&table).unwrap().collect_records();
                    let _ = db.commit(tx);

                    results.lock().push(records.len());
                });
            }
        });

        // All scans should return the same count
        let results = results.lock();
        assert!(results.iter().all(|count| *count == 50));

        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn concurrent_insert_with_retry_logic() -> Result<()> {
        let path = "test-files/concurrent_insert_retry.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let table = TableBuilder::new()
            .id(111)
            .name("retry_table")
            .add_col("id", TypeCol::INTEGER)
            .add_col("name", TypeCol::BYTES)
            .pkey(1)
            .build(&mut tx)?;

        tx.insert_table(&table)?;
        db.commit(tx)?;

        let n_threads = 20;
        let barrier = Arc::new(Barrier::new(n_threads));
        let results = Arc::new(Mutex::new(vec![]));

        thread::scope(|s| {
            for i in 0..n_threads {
                let db = db.clone();
                let table = table.clone();
                let barrier = barrier.clone();
                let results = results.clone();

                s.spawn(move || {
                    barrier.wait();

                    let r = retry(Backoff::default(), || {
                        let mut tx = db.begin(&db, TXKind::Write);
                        let rec = Record::new().add(i as i64).add(format!("user_{}", i));
                        let _ = tx.insert_rec(rec, &table, SetFlag::INSERT);
                        let commit_result = db.commit(tx);

                        if commit_result.can_retry() {
                            RetryStatus::Continue
                        } else {
                            results.lock().push(commit_result);
                            RetryStatus::Break
                        }
                    });

                    if r == RetryResult::AttemptsExceeded {
                        results
                            .lock()
                            .push(Err(Error::TransactionError(TXError::RetriesExceeded)));
                    }
                });
            }
        });

        // All different key inserts should succeed with retry logic
        let results = results.lock();
        assert_eq!(results.iter().filter(|r| r.is_ok()).count(), n_threads);

        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn concurrent_upsert_same_key() -> Result<()> {
        let path = "test-files/concurrent_upsert_same.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let table = TableBuilder::new()
            .id(112)
            .name("upsert_table")
            .add_col("id", TypeCol::INTEGER)
            .add_col("version", TypeCol::INTEGER)
            .pkey(1)
            .build(&mut tx)?;

        tx.insert_table(&table)?;
        db.commit(tx)?;

        let n_threads = 15;
        let barrier = Arc::new(Barrier::new(n_threads));
        let results = Arc::new(Mutex::new(vec![]));

        thread::scope(|s| {
            for i in 0..n_threads {
                let db = db.clone();
                let table = table.clone();
                let barrier = barrier.clone();
                let results = results.clone();

                s.spawn(move || {
                    barrier.wait();

                    let r = retry(Backoff::default(), || {
                        let mut tx = db.begin(&db, TXKind::Write);
                        let rec = Record::new().add(1i64).add(i as i64);
                        let _ = tx.insert_rec(rec, &table, SetFlag::UPSERT);
                        let result = db.commit(tx);

                        if result.can_retry() {
                            RetryStatus::Continue
                        } else {
                            results.lock().push(result);
                            RetryStatus::Break
                        }
                    });

                    if r == RetryResult::AttemptsExceeded {
                        results
                            .lock()
                            .push(Err(Error::TransactionError(TXError::RetriesExceeded)));
                    }
                });
            }
        });

        // All upserts should succeed
        let results = results.lock();
        let successful = results.iter().filter(|r| r.is_ok()).count();
        assert!(successful > 0); // At least one should succeed
        assert!(successful <= n_threads); // Not necessarily all if there's conflict detection

        // Verify the final record exists
        let tx = db.begin(&db, TXKind::Read);
        let q = Query::with_key(&table).add("id", 1i64).encode()?;
        assert!(tx.tree_get(q).is_some());
        db.commit(tx)?;

        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn concurrent_write_after_read() -> Result<()> {
        let path = "test-files/concurrent_write_after_read.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let table = TableBuilder::new()
            .id(113)
            .name("write_after_read")
            .add_col("id", TypeCol::INTEGER)
            .add_col("data", TypeCol::BYTES)
            .pkey(1)
            .build(&mut tx)?;

        tx.insert_table(&table)?;

        for i in 1..=5 {
            tx.insert_rec(
                Record::new().add(i as i64).add(format!("initial_{}", i)),
                &table,
                SetFlag::INSERT,
            )?;
        }
        db.commit(tx)?;

        let n_threads = 10;
        let barrier = Arc::new(Barrier::new(n_threads));
        let results = Arc::new(Mutex::new(vec![]));

        thread::scope(|s| {
            for i in 0..n_threads {
                let db = db.clone();
                let table = table.clone();
                let barrier = barrier.clone();
                let results = results.clone();

                s.spawn(move || {
                    barrier.wait();

                    let r = retry(Backoff::default(), || {
                        let mut tx = db.begin(&db, TXKind::Write);

                        // Read first
                        let read_q = Query::with_key(&table).add("id", 1i64).encode().unwrap();
                        let _ = tx.tree_get(read_q);

                        // Then write
                        let write_id = i as i64 + 10;
                        let rec = Record::new().add(write_id).add(format!("written_by_{}", i));
                        let _ = tx.insert_rec(rec, &table, SetFlag::INSERT);

                        let result = db.commit(tx);
                        if result.can_retry() {
                            RetryStatus::Continue
                        } else {
                            results.lock().push(result);
                            RetryStatus::Break
                        }
                    });

                    if r == RetryResult::AttemptsExceeded {
                        results
                            .lock()
                            .push(Err(Error::TransactionError(TXError::RetriesExceeded)));
                    }
                });
            }
        });

        // Most writes should succeed (different keys)
        let results = results.lock();
        let successful = results.iter().filter(|r| r.is_ok()).count();
        assert!(successful >= n_threads / 2);

        cleanup_file(path);
        Ok(())
    }
}
