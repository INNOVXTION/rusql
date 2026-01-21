use std::sync::Arc;

use tracing::{debug, error, info, instrument};

use crate::database::{
    BTree,
    btree::{
        Compare, DeleteResponse, PrefixScanIter, ScanIter, ScanMode, SetFlag, SetResponse, Tree,
    },
    codec::*,
    errors::{Error, Result, TXError, TableError},
    pager::{KVEngine, MetaPage, Pager},
    tables::{
        Key, Query, Record, Value,
        records::QueryCol,
        tables::{
            DEF_TABLE_COL1, LOWEST_PREMISSIABLE_TID, META_TABLE_COL1, META_TABLE_ID_ROW, MetaTable,
            PKEY_PREFIX, Table,
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

    fn tree_delete(&mut self, key: Key) -> Result<DeleteResponse> {
        self.key_range.add(&key);
        self.tree.delete(key)
    }
}

impl TX {
    /// wrapper function for read_table()
    #[instrument(name = "get table", skip_all)]
    fn get_table(&mut self, name: &str) -> Option<Arc<Table>> {
        info!(name, "getting table");
        self.db
            .db_link
            .read_table_buffer(name, self.version, &self.tree)
    }

    #[instrument(name = "new table id", skip_all)]
    pub fn new_tid(&mut self) -> Result<u32> {
        if self.kind == TXKind::Read {
            return Err(TXError::MismatchedKindError.into());
        }

        self.key_range.listen();

        let meta = self.db.db_link.get_meta(&self.tree);
        let key = Query::by_col(&meta)
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

                    assert!(i >= LOWEST_PREMISSIABLE_TID as i64);

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
            .ok_or_else(|| TableError::InsertTableError("record iterator failure".to_string()))?;

        self.tree_set(k, v, SetFlag::UPSERT).map_err(|e| {
            error!(%e, "error when inserting");
            TableError::InsertTableError("error when inserting table".to_string())
        })?;

        self.key_range.capture_and_stop();
        Ok(())
    }

    #[instrument(name = "update table", skip_all)]
    pub fn update_table(&mut self, table: &Table) -> Result<()> {
        info!(?table, "updating table");
        if self.kind == TXKind::Read {
            return Err(TXError::MismatchedKindError.into());
        }
        self.db.db_link.evict_table(&table.name)?;

        self.key_range.listen();

        // WIP FOR TESTS
        let (k, v) = Record::new()
            .add(table.name.clone())
            .add(table.encode()?)
            .encode(&self.db.db_link.t_def)?
            .next()
            .ok_or_else(|| TableError::InsertTableError("record iterator failure".to_string()))?;

        self.tree_set(k, v, SetFlag::UPDATE).map_err(|e| {
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

        // delete keys
        let recs = self.full_table_scan(&table)?.collect_records();
        for rec in recs.into_iter() {
            self.delete_rec(rec, &table)?;
        }

        self.key_range.listen();

        // delete from tdef
        let qu = Query::by_col(&self.db.db_link.t_def)
            .add(DEF_TABLE_COL1, name)
            .encode()?;

        self.tree_delete(qu).map_err(|e| {
            error!(%e, "error when dropping table");
            TableError::DeleteTableError("dropping table error when deleting".to_string())
        })?;

        self.key_range.capture_and_stop();

        // evict from buffer
        self.db.db_link.evict_table(name)?;

        Ok(())
    }

    /// scans all primary keys in a table
    pub fn full_table_scan(&self, schema: &Table) -> Result<PrefixScanIter<'_, TXDB>> {
        // writing a seek key with TID and Prefix 0
        let key = Query::by_tid_prefix(schema, PKEY_PREFIX);
        debug!(%key, "full table scan key");
        ScanMode::prefix(key, &self.tree)
    }

    /// counts all unique rows inside a table
    pub fn count_rows(&self, schema: &Table) -> Result<u32> {
        let scan = self.full_table_scan(schema)?;
        Ok(scan.into_iter().count() as u32)
    }

    /// counts all entries inside a table, this includes secondary indices
    pub fn count_entries(&self, schema: &Table) -> Result<u32> {
        let key = Query::by_tid(schema);
        let scan = ScanMode::prefix(key, &self.tree)?;
        Ok(scan.count() as u32)
    }

    /// inserts a record and potentially secondary indicies
    #[instrument(name = "insert rec", skip_all)]
    pub fn insert_rec(&mut self, rec: Record, schema: &Table, flag: SetFlag) -> Result<u32> {
        info!(?rec, "inserting record");
        if self.kind == TXKind::Read {
            return Err(TXError::MismatchedKindError.into());
        }

        if schema.id < LOWEST_PREMISSIABLE_TID {
            return Err(Error::InsertError("invalid table id".to_string()).into());
        }

        self.key_range.listen();
        let mut iter = rec
            .encode(schema)
            .map_err(|e| {
                error!(?e, "record failed to encode");
                Error::InsertError("record failed to encode".to_string())
            })?
            .peekable();
        let mut old_rec;
        let old_pk;
        let mut modified = 0;

        // updating the primary key and retrieving the old one
        let primay_key = iter.next().ok_or_else(|| {
            error!("record failed to generate a primary key");
            Error::InsertError("record failed to generate a primary key".to_string())
        })?;

        let res = self.tree_set(primay_key.0, primay_key.1, flag);
        if let Ok(ref r) = res {
            if r.updated || r.added {
                self.key_range.capture_and_listen();
                modified += 1;
                debug!("modified primary key");
            }
        }

        if iter.peek().is_none() {
            // there are no secondary keys, we are done
            return Ok(modified);
        }

        match res {
            // update found (UPSERT or UPDATE)
            Ok(res) if res.updated => {
                old_pk = res.old.expect("update successful");

                // recreating the keys from the update
                old_rec = Record::from_kv(old_pk).encode(schema)?;
                old_rec.next(); // we skip the primary key since we already updated it

                // updating secondary keys
                for (k, v) in iter {
                    if let Some(old_kv) = old_rec.next() {
                        let res = self.tree_delete(old_kv.0)?;
                        if res.deleted {
                            self.key_range.capture_and_listen();
                        }

                        let res = self.tree_set(k, v, SetFlag::INSERT)?;
                        debug!("added secondary key UPSERT/UPDATE");
                        self.key_range.capture_and_listen();

                        if !res.added {
                            return Err(Error::InsertError("couldnt insert record".to_string()));
                        }

                        modified += 1;
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
                    self.key_range.capture_and_listen();
                    debug!("added secondary key INSERT");
                    if !res.added {
                        return Err(Error::InsertError("couldnt insert record".to_string()));
                    }

                    modified += 1;
                }
            }
            // Key couldnt be inserted/updated
            Err(e) => {
                error!("couldnt insert record");
                return Err(Error::InsertError("couldnt insert record".to_string()));
            }
            _ => unreachable!("we accounted for all cases"),
        }

        debug!("modified {modified} keys");
        debug_assert_eq!(modified as usize, schema.indices.len());
        Ok(modified)
    }

    fn delete_from_query(&mut self, q: QueryCol, schema: &Table) -> Result<()> {
        let key = q.encode()?;

        // getting full record
        let val = match self.tree_get(key.clone()) {
            Some(v) => v,
            None => return Err(Error::DeleteError("key doesnt exist".to_string())),
        };
        let rec = Record::from_kv((key, val));

        let _ = self.delete_rec(rec, schema)?;
        Ok(())
    }

    /// deletes a record and potential secondary indicies
    fn delete_rec(&mut self, rec: Record, schema: &Table) -> Result<u32> {
        info!(?rec, "deleting record");
        if self.kind == TXKind::Read {
            return Err(TXError::MismatchedKindError.into());
        }
        if schema.id < LOWEST_PREMISSIABLE_TID {
            return Err(Error::DeleteError("invalid table id".to_string()).into());
        }

        self.key_range.listen();
        let mut iter = rec.encode(schema)?.peekable();
        let mut updated = 0;

        let primay_key = iter.next().ok_or_else(|| {
            Error::DeleteError("record failed to generate a primary key".to_string())
        })?;

        // deleting primary key
        if let Ok(r) = self.tree_delete(primay_key.0)
            && let true = r.deleted
        {
            self.key_range.capture_and_listen();
            updated += 1;
        } else {
            return Err(Error::DeleteError("key doesnt exist".to_string()));
        }

        // checking for secondary keys
        if iter.peek().is_none() {
            return Ok(updated);
        }

        // deleting secondary keys
        for (k, v) in iter {
            self.tree_delete(k)?;
            updated += 1;
        }

        debug_assert_eq!(updated as usize, schema.indices.len());
        self.key_range.capture_and_stop();
        Ok(updated)
    }

    /// adds a new index for a table. If the index doesnt exist, it will be created, otherwise the column will be added to an existing index. Updates the table as well.
    ///
    /// returns the number of modified keys
    #[instrument(name = "create index", skip(self, table))]
    fn create_index(&mut self, idx_name: &str, col: &str, table: &mut Table) -> Result<u32> {
        if let None = table.idx_exists(idx_name) {
            table.create_index(idx_name)?;
        }
        let idx = table.add_col_to_index(idx_name, col)?;

        // get all primary rows
        let recs = self.full_table_scan(table)?.collect_records();
        let nrecs = recs.len();
        assert!(nrecs > 0);

        // insert new keys
        self.key_range.listen();
        let mut modified = 0;

        for rec in recs {
            debug!(%rec, "trying to encode");
            let mut iter = rec.encode(&table)?;

            for c in 0..idx {
                iter.next(); // skipping keys
            }

            for (k, v) in iter {
                debug!(%k, "inserting...");
                let r = self.tree_set(k, v, SetFlag::INSERT)?;
                assert!(r.added, "key doesnt exist therefore this shouldnt fail");
                self.key_range.capture_and_listen();
                modified += 1;
            }
        }

        // update or insert
        if let Some(_) = self.get_table(&table.name) {
            self.update_table(table)?;
        } else {
            self.insert_table(table)?;
        }

        assert_eq!(nrecs, modified as usize);
        Ok(modified)
    }

    /// deletes an index for a table and all associated keys. Updates the table as well.
    ///
    /// returns number of modified keys
    #[instrument(name = "delete index", skip(self, table))]
    fn delete_index(&mut self, idx_name: &str, table: &mut Table) -> Result<u32> {
        // get prefix
        let idx = match table.idx_exists(idx_name) {
            Some(idx) => idx,
            None => {
                return Err(TableError::IndexCreateError("index doesnt exists".to_string()).into());
            }
        };

        let prefix = table.indices[idx].prefix;
        let q = Query::by_tid_prefix(table, prefix);
        let kvs: Vec<(Key, Value)> = ScanMode::prefix(q, &self.tree)?.collect();
        assert!(kvs.len() > 0);
        assert!(prefix != 0);

        // delete keys
        self.key_range.listen();
        let mut modified = 0;

        for (k, _) in kvs {
            let r = self.tree_delete(k)?;
            assert!(r.deleted, "key should exist therefore this shouldnt fail");
            self.key_range.capture_and_listen();
            modified += 1;
        }

        table.remove_index(idx_name)?;

        // update or insert
        if let Some(_) = self.get_table(&table.name) {
            self.update_table(table)?;
        } else {
            self.insert_table(table)?;
        }

        Ok(modified)
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

        let q1 = Query::by_col(&table).add("name", "Alice").encode()?;
        let q2 = Query::by_col(&table).add("name", "Bob").encode()?;
        let q3 = Query::by_col(&table).add("name", "Charlie").encode()?;

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

        let good_query = Query::by_col(&table).add("name", "Alice").add("age", 10);
        assert!(good_query.encode().is_ok());

        let good_query = Query::by_col(&table).add("name", "Alice").add("age", 10);
        let unordered = Query::by_col(&table).add("age", 10).add("name", "Alice");
        assert_eq!(good_query.encode().unwrap(), unordered.encode().unwrap());

        let bad_query = Query::by_col(&table).add("name", "Alice");
        assert!(bad_query.encode().is_err());

        let bad_query = Query::by_col(&table).add("dfasdf", "fasdf");
        assert!(bad_query.encode().is_err());

        let bad_query = Query::by_col(&table);
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

        let rec = Record::new().add("test");

        assert!(tx.insert_rec(rec, &table, SetFlag::INSERT).is_ok());

        assert!(tx.get_table("droppable").is_some());
        assert!(tx.drop_table("droppable").is_ok());

        db.commit(tx).unwrap();
        let mut tx = db.begin(&db, TXKind::Read);

        assert!(tx.get_table("droppable").is_none());

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
        assert!(Query::by_col(&table).encode().is_err());

        // wrong column name
        assert!(Query::by_col(&table).add("nope", "x").encode().is_err());

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

        let open = ScanMode::open(
            Query::by_col(&table1).add("name", "Alice").encode()?,
            Compare::GE,
        )?;
        let res: Vec<_> = tx.tree_scan(open)?.collect_records();

        assert_eq!(res.len(), 3);
        assert_eq!(res[0].to_string(), "Alice 20");
        assert_eq!(res[1].to_string(), "Bob 15");
        assert_eq!(res[2].to_string(), "Charlie 25");

        let open = ScanMode::open(Query::by_col(&table2).add("id", 20).encode()?, Compare::GE)?;
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
        entries_t2.push(Record::new().add(15).add("Bob").add("clerk"));
        entries_t2.push(Record::new().add(20).add("Alice").add("teacher"));
        entries_t2.push(Record::new().add(25).add("Charlie").add("fire fighter"));

        for entry in entries_t2 {
            tx.insert_rec(entry, &table2, SetFlag::UPSERT)?;
        }

        let res = tx.full_table_scan(&table1)?;
        let records: Vec<_> = res.collect_records();
        assert_eq!(records.len(), 3);

        let mut iter = records.into_iter();
        assert_eq!(iter.next().unwrap().to_string(), "Alice 20");
        assert_eq!(iter.next().unwrap().to_string(), "Bob 15");
        assert_eq!(iter.next().unwrap().to_string(), "Charlie 25");
        assert!(iter.next().is_none());

        let res = tx.full_table_scan(&table2)?;
        let records: Vec<_> = res.collect_records();
        assert_eq!(records.len(), 3);

        let mut iter = records.into_iter();
        assert_eq!(iter.next().unwrap().to_string(), "15 Bob clerk");
        assert_eq!(iter.next().unwrap().to_string(), "20 Alice teacher");
        assert_eq!(iter.next().unwrap().to_string(), "25 Charlie fire fighter");
        assert!(iter.next().is_none());

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
        let lo_key = Query::by_col(&table).add("id", 5i64).encode()?;
        let hi_key = Query::by_col(&table).add("id", 15i64).encode()?;

        let range = ScanMode::range((lo_key, Compare::GE), (hi_key, Compare::GT))?;

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
        let open = ScanMode::open(
            Query::by_col(&table1).add("key", "key_a_1").encode()?,
            Compare::GE,
        )?;
        let res1: Vec<_> = tx.tree_scan(open)?.collect_records();

        // Should only contain table1 records
        assert!(res1.iter().all(|r| r.to_string().contains("val_a_")));
        assert_eq!(res1.len(), 10);

        // Scan table2
        let open = ScanMode::open(
            Query::by_col(&table2).add("key", "key_b_1").encode()?,
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
        let open = ScanMode::open(
            Query::by_col(&table).add("score", 50i64).encode()?,
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
        let open = ScanMode::open(
            Query::by_col(&table).add("id", 100i64).encode()?,
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

        let open = ScanMode::open(
            Query::by_col(&table).add("code", "CODE_001").encode()?,
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
        let open = ScanMode::open(
            Query::by_col(&table).add("value", 35i64).encode()?,
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
        let open = ScanMode::open(
            Query::by_col(&table).add("id", 100i64).encode()?,
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
        let open = ScanMode::open(
            Query::by_col(&table).add("name", "bob").encode()?,
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
            let key = Query::by_col(&table).add("id", *i as i64).encode()?;
            tx.tree_delete(key)?;
        }

        // Scan from beginning
        let open = ScanMode::open(Query::by_col(&table).add("id", 1i64).encode()?, Compare::GE)?;

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
    use test_log::test;
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
                let id = h.thread().id();
                if let Err(err) = h.join() {
                    if let Some(s) = err.downcast_ref::<&str>() {
                        error!("thread {:?} panicked: {}", id, s);
                        panic!()
                    } else if let Some(s) = err.downcast_ref::<String>() {
                        error!("thread {:?} panicked: {}", id, s);
                        panic!()
                    }
                }
            }
        });

        // should provoke write conflicts
        assert!(res.lock().iter().any(|r| r.is_err()));
        // assert!(res.lock().iter().filter(|r| r.is_ok()).count() < 10);

        let tx = db.begin(&db, TXKind::Read);

        let q1 = Query::by_col(&table).add("name", "Alice").encode()?;
        let q2 = Query::by_col(&table).add("name", "Bob").encode()?;
        let q3 = Query::by_col(&table).add("name", "Charlie").encode()?;

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

                        let _ = tx.insert_rec(rec, &table, SetFlag::INSERT);

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
                    if let Some(s) = err.downcast_ref::<&str>() {
                        error!("thread {:?} panicked: {}", id, s);
                        panic!()
                    } else if let Some(s) = err.downcast_ref::<String>() {
                        error!("thread {:?} panicked: {}", id, s);
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
            let q = Query::by_col(&table).add("id", i as i64).encode()?;
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
                        let q = Query::by_col(&table).add("id", i as i64).encode().unwrap();
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

        for i in 1..=100 {
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

        let n_threads = 1000;
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

                    for i in 1..=100 {
                        let q1 = Query::by_col(&table1).add("id", i as i64).encode().unwrap();
                        let q2 = Query::by_col(&table2).add("id", i as i64).encode().unwrap();

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
        for i in 1..=1000 {
            tx.insert_rec(
                Record::new().add(i as i64).add(format!("initial_{}", i)),
                &table,
                SetFlag::INSERT,
            )?;
        }
        db.commit(tx)?;

        let n_threads = 1000;
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
            let q = Query::by_col(&table).add("id", i as i64 + 1).encode()?;
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

        let n_threads = 1000;
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
                        let q = Query::by_col(&table).add("id", 1i64).encode().unwrap();
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
        for i in 1..=1000 {
            tx.insert_rec(
                Record::new().add(i as i64).add(format!("to_delete_{}", i)),
                &table,
                SetFlag::INSERT,
            )?;
        }
        db.commit(tx)?;

        let n_threads = 1000;
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

                        let q = Query::by_col(&table).add("id", i as i64);

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
            let q = Query::by_col(&table).add("id", i as i64).encode()?;
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

        let n_threads = 1000;
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
                        let q = Query::by_col(&table).add("id", 1i64).encode().unwrap();

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
                                let q = Query::by_col(&table).add("id", 1i64).encode().unwrap();
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

        let n_threads = 1000;
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
                        let q = Query::by_col(&table).add("id", 1i64).encode().unwrap();
                        let val = tx.tree_get(q);
                        let _ = db.commit(tx);
                        read_results.lock().push(val.is_some());
                    } else {
                        // Writer threads
                        let r = retry(Backoff::default(), || {
                            let mut tx = db.begin(&db, TXKind::Write);
                            let current = if let Some(v) =
                                tx.tree_get(Query::by_col(&table).add("id", 1i64).encode().unwrap())
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
                        let q = Query::by_col(&table).add("id", i as i64).encode().unwrap();
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
        let q = Query::by_col(&table).add("id", 1i64).encode()?;
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
                        let read_q = Query::by_col(&table).add("id", 1i64).encode().unwrap();
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

#[cfg(test)]
mod secondary_index_ops {
    use super::*;
    use crate::database::{
        btree::{Compare, ScanMode, SetFlag},
        helper::cleanup_file,
        pager::transaction::{Retry, Transaction},
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
    use test_log::test;

    #[test]
    fn insert_record_with_single_secondary_index() -> Result<()> {
        let path = "test-files/insert_single_secondary.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let mut table = TableBuilder::new()
            .id(50)
            .name("users")
            .add_col("id", TypeCol::INTEGER)
            .add_col("email", TypeCol::BYTES)
            .add_col("name", TypeCol::BYTES)
            .pkey(1)
            .build(&mut tx)?;

        table.create_index("email")?;
        table.add_col_to_index("email", "email")?;
        tx.insert_table(&table)?;

        // Insert record
        let rec = Record::new()
            .add(1i64)
            .add("alice@example.com")
            .add("Alice");

        tx.insert_rec(rec, &table, SetFlag::INSERT)?;

        // Query by primary key
        let pk_query = Query::by_col(&table).add("id", 1i64).encode()?;
        let result = tx.tree_get(pk_query).unwrap().decode();

        assert_eq!(result[0], DataCell::Str("alice@example.com".to_string()));
        assert_eq!(result[1], DataCell::Str("Alice".to_string()));

        // Query by secondary index (email)
        let email_query = Query::by_col(&table)
            .add("email", "alice@example.com")
            .encode()?;

        let mut scan = ScanMode::open(email_query, Compare::GE)
            .unwrap()
            .into_iter(&tx.tree)
            .unwrap();

        let result = scan.next().unwrap();
        let key = result.0.decode();
        let val = result.1.decode();

        assert_eq!(key[0], DataCell::Str("alice@example.com".to_string()));
        assert_eq!(key[1], DataCell::Int(1));
        assert_eq!(val[0], DataCell::Str("Alice".to_string()));

        assert!(scan.next().is_none());

        db.commit(tx)?;
        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn insert_multiple_records_with_secondary_index() -> Result<()> {
        let path = "test-files/insert_multi_secondary.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let mut table = TableBuilder::new()
            .id(51)
            .name("employees")
            .add_col("id", TypeCol::INTEGER)
            .add_col("department", TypeCol::BYTES)
            .add_col("name", TypeCol::BYTES)
            .pkey(1)
            .build(&mut tx)?;

        table.create_index("department")?;
        table.add_col_to_index("department", "department")?;
        tx.insert_table(&table)?;

        // Insert multiple records
        for i in 1..=5 {
            let dept = if i % 2 == 0 { "Engineering" } else { "Sales" };
            let rec = Record::new()
                .add(i as i64)
                .add(dept)
                .add(format!("Employee_{}", i));
            tx.insert_rec(rec, &table, SetFlag::INSERT)?;
        }

        // Verify records by primary key
        for i in 1..=5 {
            let pk_query = Query::by_col(&table).add("id", i as i64).encode()?;
            let result = tx.tree_get(pk_query).unwrap().decode();
            assert_eq!(result[1], DataCell::Str(format!("Employee_{}", i)));
        }

        // Verify records by secondary index
        let eng_query = Query::by_col(&table)
            .add("department", "Engineering")
            .encode()?;
        let mut scan = ScanMode::open(eng_query, Compare::GE)
            .unwrap()
            .into_iter(&tx.tree)
            .unwrap();
        let mut count = 0;
        while let Some(_) = scan.next() {
            count += 1;
        }
        assert!(count > 0); // Verify we got at least one Engineering result

        db.commit(tx)?;
        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn insert_record_with_multiple_secondary_indices() -> Result<()> {
        let path = "test-files/insert_multi_indices.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let mut table = TableBuilder::new()
            .id(52)
            .name("products")
            .add_col("id", TypeCol::INTEGER)
            .add_col("name", TypeCol::BYTES)
            .add_col("category", TypeCol::BYTES)
            .add_col("price", TypeCol::INTEGER)
            .pkey(1)
            .build(&mut tx)?;

        table.create_index("name")?;
        table.add_col_to_index("name", "name")?;
        table.create_index("category")?;
        table.add_col_to_index("category", "category")?;
        table.create_index("price")?;
        table.add_col_to_index("price", "price")?;
        tx.insert_table(&table)?;

        // Insert record
        let rec = Record::new()
            .add(1i64)
            .add("Laptop")
            .add("Electronics")
            .add(1500i64);

        tx.insert_rec(rec, &table, SetFlag::INSERT)?;

        // Query by each secondary index
        let name_query = Query::by_col(&table).add("name", "Laptop").encode()?;
        let mut scan = ScanMode::open(name_query, Compare::GE)
            .unwrap()
            .into_iter(&tx.tree)
            .unwrap();
        assert!(scan.next().is_some()); // Found by name

        let category_query = Query::by_col(&table)
            .add("category", "Electronics")
            .encode()?;
        let mut scan = ScanMode::open(category_query, Compare::GE)
            .unwrap()
            .into_iter(&tx.tree)
            .unwrap();
        assert!(scan.next().is_some()); // Found by category

        let price_query = Query::by_col(&table).add("price", 1500i64).encode()?;
        let mut scan = ScanMode::open(price_query, Compare::GE)
            .unwrap()
            .into_iter(&tx.tree)
            .unwrap();
        assert!(scan.next().is_some()); // Found by price

        db.commit(tx)?;
        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn delete_record_with_secondary_index() -> Result<()> {
        let path = "test-files/delete_secondary_index.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let mut table = TableBuilder::new()
            .id(53)
            .name("deletable")
            .add_col("id", TypeCol::INTEGER)
            .add_col("status", TypeCol::BYTES)
            .add_col("data", TypeCol::BYTES)
            .pkey(1)
            .build(&mut tx)?;

        table.create_index("status")?;
        table.add_col_to_index("status", "status")?;
        tx.insert_table(&table)?;

        // Insert record
        let rec = Record::new().add(1i64).add("active").add("test data");
        tx.insert_rec(rec, &table, SetFlag::INSERT)?;

        // Verify it exists via primary key
        let pk_query = Query::by_col(&table).add("id", 1i64).encode()?;
        assert!(tx.tree_get(pk_query.clone()).is_some());

        // Verify it exists via secondary index
        let status_query = Query::by_col(&table).add("status", "active").encode()?;
        let mut scan = ScanMode::open(status_query.clone(), Compare::GE)
            .unwrap()
            .into_iter(&tx.tree)
            .unwrap();
        assert!(scan.next().is_some());

        // Delete the record
        let q = Query::by_col(&table).add("id", 1i64);
        tx.delete_from_query(q, &table)?;

        // Verify primary key is gone
        assert!(tx.tree_get(pk_query).is_none());

        // Verify secondary index entry is also gone (should be handled by record deletion)
        let scan = ScanMode::open(status_query, Compare::GE)
            .unwrap()
            .into_iter(&tx.tree);

        assert!(scan.is_none());

        db.commit(tx)?;
        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn delete_multiple_records_with_secondary_index() -> Result<()> {
        let path = "test-files/delete_multi_secondary.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let mut table = TableBuilder::new()
            .id(54)
            .name("deletable_multi")
            .add_col("id", TypeCol::INTEGER)
            .add_col("category", TypeCol::BYTES)
            .add_col("name", TypeCol::BYTES)
            .pkey(1)
            .build(&mut tx)?;

        table.create_index("category")?;
        table.add_col_to_index("category", "category")?;
        tx.insert_table(&table)?;

        // Insert records
        for i in 1..=10 {
            let rec = Record::new()
                .add(i as i64)
                .add("category_a")
                .add(format!("item_{}", i));
            tx.insert_rec(rec, &table, SetFlag::INSERT)?;
        }

        // Delete some records
        for i in [1, 3, 5, 7, 9].iter() {
            let pk_query = Query::by_col(&table).add("id", *i as i64);
            tx.delete_from_query(pk_query, &table)?;
        }

        // Verify remaining records
        for i in [2, 4, 6, 8, 10].iter() {
            let pk_query = Query::by_col(&table).add("id", *i as i64).encode()?;
            assert!(tx.tree_get(pk_query).is_some());
        }

        // Verify deleted records are gone
        for i in [1, 3, 5, 7, 9].iter() {
            let pk_query = Query::by_col(&table).add("id", *i as i64).encode()?;
            assert!(tx.tree_get(pk_query).is_none());
        }

        db.commit(tx)?;
        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn read_record_via_secondary_index() -> Result<()> {
        let path = "test-files/read_secondary_index.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let mut table = TableBuilder::new()
            .id(55)
            .name("readers")
            .add_col("id", TypeCol::INTEGER)
            .add_col("username", TypeCol::BYTES)
            .add_col("email", TypeCol::BYTES)
            .add_col("role", TypeCol::BYTES)
            .pkey(1)
            .build(&mut tx)?;

        table.create_index("username")?;
        table.add_col_to_index("username", "username")?;
        table.create_index("email")?;
        table.add_col_to_index("email", "email")?;
        tx.insert_table(&table)?;

        // Insert records
        let records = vec![
            (1i64, "alice", "alice@example.com", "admin"),
            (2i64, "bob", "bob@example.com", "user"),
            (3i64, "charlie", "charlie@example.com", "user"),
        ];

        for (id, username, email, role) in &records {
            let rec = Record::new().add(*id).add(*username).add(*email).add(*role);
            tx.insert_rec(rec, &table, SetFlag::INSERT)?;
        }

        // Read via username secondary index
        let username_query = Query::by_col(&table).add("username", "alice").encode()?;
        let mut scan = ScanMode::open(username_query, Compare::GE)
            .unwrap()
            .into_iter(&tx.tree)
            .unwrap();
        let result = scan.next().unwrap();
        let val = result.1.decode();
        assert_eq!(val[0], DataCell::Str("alice@example.com".to_string()));
        assert_eq!(val[1], DataCell::Str("admin".to_string()));

        // Read via email secondary index
        let email_query = Query::by_col(&table)
            .add("email", "bob@example.com")
            .encode()?;
        let mut scan = ScanMode::open(email_query, Compare::GE)
            .unwrap()
            .into_iter(&tx.tree)
            .unwrap();

        let result = scan.next().unwrap();

        let key = result.0;
        assert_eq!(key.get_tid(), 55);
        assert_eq!(key.get_prefix(), 2);

        let key = key.decode();
        assert_eq!(key[0], DataCell::Str("bob@example.com".to_string()));
        assert_eq!(key[1], DataCell::Int(2));

        let val = result.1.decode();
        assert_eq!(val[0], DataCell::Str("bob".to_string()));
        assert_eq!(val[1], DataCell::Str("user".to_string()));

        db.commit(tx)?;
        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn upsert_record_with_secondary_index() -> Result<()> {
        let path = "test-files/upsert_secondary_index.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let mut table = TableBuilder::new()
            .id(56)
            .name("upsertable")
            .add_col("id", TypeCol::INTEGER)
            .add_col("status", TypeCol::BYTES)
            .add_col("value", TypeCol::INTEGER)
            .pkey(1)
            .build(&mut tx)?;

        table.create_index("status")?;
        table.add_col_to_index("status", "status")?;
        tx.insert_table(&table)?;

        // Insert initial record
        let rec = Record::new().add(1i64).add("pending").add(100i64);
        tx.insert_rec(rec, &table, SetFlag::UPSERT)?;

        // Verify initial state
        let status_query = Query::by_col(&table).add("status", "pending").encode()?;
        let mut scan = ScanMode::open(status_query, Compare::GE)
            .unwrap()
            .into_iter(&tx.tree)
            .unwrap();
        let result = scan.next().unwrap();
        let val = result.1.decode();
        assert_eq!(val[0], DataCell::Int(100));

        // Upsert with updated values
        let rec = Record::new().add(1i64).add("completed").add(200i64);
        tx.insert_rec(rec, &table, SetFlag::UPSERT)?;

        // Verify updated state via primary key
        let pk_query = Query::by_col(&table).add("id", 1i64).encode()?;
        let result = tx.tree_get(pk_query).unwrap().decode();
        assert_eq!(result[0], DataCell::Str("completed".to_string()));
        assert_eq!(result[1], DataCell::Int(200));

        // Verify new secondary index entry exists
        let new_status_query = Query::by_col(&table).add("status", "completed").encode()?;
        let mut scan = ScanMode::open(new_status_query, Compare::GE)
            .unwrap()
            .into_iter(&tx.tree)
            .unwrap();
        assert!(scan.next().is_some());

        db.commit(tx)?;
        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn scan_with_secondary_index_key() -> Result<()> {
        let path = "test-files/scan_secondary_index.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let mut table = TableBuilder::new()
            .id(57)
            .name("scannables")
            .add_col("id", TypeCol::INTEGER)
            .add_col("score", TypeCol::INTEGER)
            .add_col("name", TypeCol::BYTES)
            .pkey(1)
            .build(&mut tx)?;

        table.create_index("score")?;
        table.add_col_to_index("score", "score")?;
        tx.insert_table(&table)?;

        // Insert records with various scores
        for i in 1..=20 {
            let score = (i * 10) as i64;
            let rec = Record::new()
                .add(i as i64)
                .add(score)
                .add(format!("player_{}", i));
            tx.insert_rec(rec, &table, SetFlag::INSERT)?;
        }

        // Scan from score 100 onwards
        let start_key = Query::by_col(&table).add("score", 100i64).encode()?;
        let scan_mode = ScanMode::open(start_key, Compare::GE)?;
        let results: Vec<_> = tx.tree_scan(scan_mode)?.collect_records();

        // Should get records with scores from 100 to 200 (11 records)
        assert_eq!(results.len(), 11);

        db.commit(tx)?;
        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn concurrent_inserts_different_secondary_values() -> Result<()> {
        let path = "test-files/concurrent_secondary_inserts.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let mut table = TableBuilder::new()
            .id(58)
            .name("concurrent_secondary")
            .add_col("id", TypeCol::INTEGER)
            .add_col("tag", TypeCol::BYTES)
            .add_col("data", TypeCol::BYTES)
            .pkey(1)
            .build(&mut tx)?;

        table.create_index("tag")?;
        table.add_col_to_index("tag", "tag")?;
        tx.insert_table(&table)?;
        db.commit(tx)?;

        let n_threads = 50;
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

                        let tag = format!("tag_0");
                        let rec = Record::new()
                            .add(i as i64)
                            .add(tag.clone())
                            .add(format!("data_{}", i));

                        let result = tx.insert_rec(rec, &table, SetFlag::INSERT);
                        if result.is_err() {
                            return RetryStatus::Break;
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

        // All inserts should succeed (different primary keys)
        let results = results.lock();
        assert_eq!(results.iter().filter(|r| r.is_ok()).count(), n_threads);

        // Verify records via secondary index
        let tx = db.begin(&db, TXKind::Read);

        let tag = format!("tag_0");
        let tag_query = Query::by_col(&table).add("tag", tag).encode()?;

        let scan = ScanMode::prefix(tag_query, &tx.tree);
        assert_eq!(scan.unwrap().count(), n_threads);

        db.commit(tx)?;

        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn secondary_index_isolation_across_transactions() -> Result<()> {
        let path = "test-files/secondary_isolation.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let mut table = TableBuilder::new()
            .id(59)
            .name("isolation_test")
            .add_col("id", TypeCol::INTEGER)
            .add_col("status", TypeCol::BYTES)
            .add_col("value", TypeCol::BYTES)
            .pkey(1)
            .build(&mut tx)?;

        table.create_index("status")?;
        table.add_col_to_index("status", "status")?;
        tx.insert_table(&table)?;
        db.commit(tx)?;

        // Transaction 1: Insert with status "draft"
        let mut tx1 = db.begin(&db, TXKind::Write);
        let rec = Record::new().add(1i64).add("draft").add("content_1");
        tx1.insert_rec(rec, &table, SetFlag::INSERT)?;
        db.commit(tx1)?;

        // Transaction 2: Read via secondary index
        let tx2 = db.begin(&db, TXKind::Read);
        let status_query = Query::by_col(&table).add("status", "draft").encode()?;
        let mut scan = ScanMode::open(status_query, Compare::GE)
            .unwrap()
            .into_iter(&tx2.tree)
            .unwrap();
        let result = scan.next().unwrap();
        let val = result.1.decode();
        assert_eq!(val[0], DataCell::Str("content_1".to_string()));
        db.commit(tx2)?;

        // Transaction 3: Update record
        let mut tx3 = db.begin(&db, TXKind::Write);
        let rec = Record::new()
            .add(1i64)
            .add("published")
            .add("updated_content");
        tx3.insert_rec(rec, &table, SetFlag::UPSERT)?;
        db.commit(tx3)?;

        // Transaction 4: Verify update via secondary index
        let tx4 = db.begin(&db, TXKind::Read);
        let new_status_query = Query::by_col(&table).add("status", "published").encode()?;
        let mut scan = ScanMode::open(new_status_query, Compare::GE)
            .unwrap()
            .into_iter(&tx4.tree)
            .unwrap();
        let result = scan.next().unwrap();
        let val = result.1.decode();
        assert_eq!(val[0], DataCell::Str("updated_content".to_string()));
        db.commit(tx4)?;

        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn create_sec_idx_existing_keys() -> Result<()> {
        let path = "create_sec_idx.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let mut table = TableBuilder::new()
            .id(58)
            .name("create_secondary")
            .add_col("id", TypeCol::INTEGER)
            .add_col("tag", TypeCol::BYTES)
            .add_col("data", TypeCol::BYTES)
            .pkey(1)
            .build(&mut tx)?;

        let n_inserts = 10;
        let mut count = 0;

        for i in 0..n_inserts {
            let rec = Record::new().add(i).add(format!("tag_{i}")).add("data");
            tx.insert_rec(rec, &table, SetFlag::INSERT)?;
            count += 1;
        }

        assert_eq!(n_inserts, count);

        // creating idx in table that houses rows already
        tx.create_index("tag", "tag", &mut table)?;

        // query secondary index
        count = 0;
        for i in 0..n_inserts {
            let q = Query::by_col(&table)
                .add("tag", format!("tag_{}", i))
                .encode()?;

            assert_eq!(q.get_prefix(), 1);
            assert_eq!(q.get_tid(), 58);

            let scan = ScanMode::prefix(q, &tx.tree)?.next().unwrap();

            assert_eq!(scan.0.to_string(), format!("58 1 tag_{i} {i}"));
            assert_eq!(scan.1.to_string(), format!("data"));
            count += 1;
        }

        assert_eq!(n_inserts, count);

        cleanup_file(path);
        Ok(())
    }

    #[test]
    fn delete_sec_idx_existing_keys() -> Result<()> {
        let path = "create_sec_idx.rdb";
        cleanup_file(path);
        let db = Arc::new(KVDB::new(path));
        let mut tx = db.begin(&db, TXKind::Write);

        let mut table = TableBuilder::new()
            .id(58)
            .name("create_secondary")
            .add_col("id", TypeCol::INTEGER)
            .add_col("tag", TypeCol::BYTES)
            .add_col("data", TypeCol::BYTES)
            .pkey(1)
            .build(&mut tx)?;

        let n_inserts = 10;
        let mut count = 0;

        for i in 0..n_inserts {
            let rec = Record::new()
                .add(i)
                .add(format!("tag_{i}"))
                .add(format!("data_{i}"));
            tx.insert_rec(rec, &table, SetFlag::INSERT)?;
            count += 1;
        }

        assert_eq!(n_inserts, count);
        assert_eq!(tx.count_entries(&table)?, n_inserts);

        // creating idx in table that houses rows already
        tx.create_index("tag", "tag", &mut table)?;
        assert_eq!(tx.count_entries(&table)?, n_inserts * 2);
        tx.create_index("data", "data", &mut table)?;
        assert_eq!(tx.count_entries(&table)?, n_inserts * 3);

        // query secondary index
        count = 0;
        for i in 0..n_inserts {
            let q = Query::by_col(&table)
                .add("tag", format!("tag_{i}"))
                .encode()?;

            assert_eq!(q.get_prefix(), 1);
            assert_eq!(q.get_tid(), 58);

            let scan = ScanMode::prefix(q, &tx.tree)?.next().unwrap();

            assert_eq!(scan.0.to_string(), format!("58 1 tag_{i} {i}"));
            assert_eq!(scan.1.to_string(), format!("data_{i}"));
            count += 1;
        }
        assert_eq!(n_inserts, count);

        count = 0;
        for i in 0..n_inserts {
            let q = Query::by_col(&table)
                .add("data", format!("data_{i}"))
                .encode()?;

            assert_eq!(q.get_prefix(), 2);
            assert_eq!(q.get_tid(), 58);

            let scan = ScanMode::prefix(q, &tx.tree)?.next().unwrap();

            assert_eq!(scan.0.to_string(), format!("58 2 data_{i} {i}"));
            assert_eq!(scan.1.to_string(), format!("tag_{i}"));
            count += 1;
        }
        assert_eq!(n_inserts, count);

        tx.delete_index("tag", &mut table)?;
        assert_eq!(tx.count_entries(&table)?, n_inserts * 2);
        tx.delete_index("data", &mut table)?;
        assert_eq!(tx.count_entries(&table)?, n_inserts);

        for i in 0..n_inserts {
            let r = Query::by_col(&table)
                .add("tag", format!("tag_{}", i))
                .encode();
            assert!(r.is_err()) // the index doesnt exist anymore!
        }

        for i in 0..n_inserts {
            let r = Query::by_col(&table)
                .add("data", format!("data_{}", i))
                .encode();
            assert!(r.is_err()) // the index doesnt exist anymore!
        }

        cleanup_file(path);
        Ok(())
    }
}
