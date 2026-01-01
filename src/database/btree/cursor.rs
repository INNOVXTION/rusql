use tracing::{debug, instrument};

use crate::database::{
    BTree,
    btree::{Tree, TreeNode, node::NodeType},
    errors::{Result, ScanError},
    pager::diskpager::Pager,
    tables::{Key, Record, Value},
};

pub(crate) enum ScanMode {
    // scanning the entire table starting from key
    Open(Key, Compare),
    // scans table within range
    Range {
        lo: (Key, Compare),
        hi: (Key, Compare),
    },
}

impl ScanMode {
    /// scans the entire table starting from key
    pub fn new_open(key: Key, cmp: Compare) -> Result<Self> {
        if cmp == Compare::EQ {
            return Err(ScanError::ScanCreateError(
                "invalid input: EQ predicate cant be used for open scans".to_string(),
            )
            .into());
        }
        Ok(ScanMode::Open(key, cmp))
    }
    /// scans a range between two keys, predicates dictate starting position for lo and hi
    pub fn new_range(lo: (Key, Compare), hi: (Key, Compare)) -> Result<Self> {
        let tid = lo.0.get_tid();
        if tid != hi.0.get_tid() {
            return Err(ScanError::ScanCreateError(
                "invalid input: keys from different tables provided".to_string(),
            )
            .into());
        }
        if lo.0 >= hi.0 {
            return Err(ScanError::ScanCreateError(
                "invalid input: low point exceeds high point".to_string(),
            )
            .into());
        }
        Ok(ScanMode::Range { lo, hi })
    }

    /// turns scanmode into iterator by performing tree read operations
    pub fn into_iter<'a, P: Pager>(self, tree: &'a BTree<P>) -> Option<ScanIter<'a, P>> {
        match self {
            ScanMode::Open(key, compare) => {
                let dir = match compare {
                    Compare::LT | Compare::LE => CursorDir::Prev,
                    Compare::GT | Compare::GE => CursorDir::Next,
                    Compare::EQ => unreachable!(), // we validate scanmode creation
                };
                Some(ScanIter {
                    cursor: seek(tree, &key, compare)?,
                    tid: key.get_tid(),
                    dir,
                    range: None,
                    finished: false,
                })
            }
            ScanMode::Range { lo, hi } => {
                let tid = lo.0.get_tid();
                Some(ScanIter {
                    cursor: seek(tree, &lo.0, lo.1)?,
                    tid,
                    dir: CursorDir::Next,
                    range: Some(hi),
                    finished: false,
                })
            }
        }
    }
}

pub(super) fn scan_single<P: Pager>(tree: &BTree<P>, key: &Key) -> Option<Vec<(Key, Value)>> {
    let mut res: Vec<(Key, Value)> = vec![];
    let cursor = seek(tree, &key, Compare::EQ)?;
    res.push(cursor.deref());
    Some(res)
}

pub(crate) struct ScanIter<'a, P: Pager> {
    cursor: Cursor<'a, P>,
    tid: u32,
    dir: CursorDir,
    range: Option<(Key, Compare)>,
    finished: bool,
}

impl<'a, P: Pager> ScanIter<'a, P> {
    pub fn collect_records(self) -> Vec<Record> {
        let v: Vec<Record> = self.into_iter().map(|kv| Record::from_kv(kv)).collect();
        v
    }
}

enum CursorDir {
    Next,
    Prev,
}

impl<'a, P: Pager> Iterator for ScanIter<'a, P> {
    type Item = (Key, Value);

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        match self.range {
            // range scan
            Some(ref hi) => {
                let (k, v) = self.cursor.next()?;

                if !key_cmp(&k, &hi.0, hi.1) {
                    // return as soon as key doesnt match range key predicate
                    self.finished = true;
                    return None;
                }
                Some((k, v))
            }
            // open scan
            None => {
                match self.dir {
                    CursorDir::Next => {
                        let (k, v) = self.cursor.next()?;

                        if k.get_tid() != self.tid {
                            // return as soon as we see a key belonging to a different table
                            self.finished = true;
                            return None;
                        };
                        Some((k, v))
                    }
                    CursorDir::Prev => {
                        let (k, v) = self.cursor.prev()?;

                        if k.get_tid() != self.tid {
                            // return as soon as we see a key belonging to a different table
                            self.finished = true;
                            return None;
                        };
                        Some((k, v))
                    }
                }
            }
        }
    }
}

/*
path = [R, N2, L2] Nodes to be read
pos  = [1, 1 , 2]   indices correspond to an idx to a given key

in this case, the key would be located at:
idx 1 in root, index 1 in Node 2 and index 2 in Leaf 2
*/

#[derive(Debug)]
pub(crate) struct Cursor<'a, P: Pager> {
    tree: &'a BTree<P>,
    path: Vec<TreeNode>, // from root to leaf
    pos: Vec<u16>,       // indices
    empty: bool,
}

impl<'a, P: Pager> Cursor<'a, P> {
    pub fn new(tree: &'a BTree<P>) -> Self {
        Cursor {
            tree: tree,
            path: vec![],
            pos: vec![],
            empty: false,
        }
    }

    // retrieves the key value pair at current position
    pub fn deref(&self) -> (Key, Value) {
        let node = &self.path[self.path.len() - 1];
        let idx = self.pos[self.path.len() - 1];

        let key = node.get_key(idx).unwrap();
        let val = node.get_val(idx).unwrap();
        (key, val)
    }

    // moves the path one idx forward
    pub fn next(&mut self) -> Option<(Key, Value)> {
        if self.empty {
            return None;
        }
        let res = self.deref();
        self.iter_next(self.path.len() - 1);
        Some(res)
    }

    fn iter_next(&mut self, level: usize) {
        if self.pos[level] + 1 < self.path[level].get_nkeys() {
            // move within node
            self.pos[level] += 1;
        } else if level > 0 {
            // we reached the last key of the node, so we go up one level to access the sibling
            self.iter_next(level - 1);
        } else {
            // past last key
            self.empty = true;
            return;
        }
        if level + 1 < self.pos.len() {
            // we are in a non leaf node and need to retrieve the next sibling
            let node = &self.path[level];
            let kid = self.tree.decode(node.get_ptr(self.pos[level]));

            self.path[level + 1] = kid;
            self.pos[level + 1] = 0;
        }
    }

    // moves the path one idx backwards
    pub fn prev(&mut self) -> Option<(Key, Value)> {
        if self.empty {
            return None;
        }
        let res = self.deref();
        if res.0.is_empty() {
            // empty key edge case!
            self.empty = true;
            return None;
        }
        self.iter_prev(self.path.len() - 1);
        Some(res)
    }

    fn iter_prev(&mut self, level: usize) {
        if self.pos[level] > 0 {
            // move within node
            self.pos[level] -= 1;
        } else if level > 0 {
            // we reached the last key of the node, so we go up one level to access the sibling
            self.iter_prev(level - 1);
        } else {
            // past last key
            self.empty = true;
            return;
        }
        if level + 1 < self.pos.len() {
            // we are in a non leaf node and need to retrieve the next sibling
            let node = &self.path[level];
            let kid = self.tree.decode(node.get_ptr(self.pos[level]));

            self.pos[level + 1] = kid.get_nkeys() - 1;
            self.path[level + 1] = kid;
        }
    }
}

// creates a new cursor
#[instrument(skip_all)]
fn seek<'a, P: Pager>(tree: &'a BTree<P>, key: &Key, flag: Compare) -> Option<Cursor<'a, P>> {
    let mut cursor = Cursor::new(tree);
    let mut ptr = tree.get_root();

    while let Some(p) = ptr {
        let node = tree.decode(p);

        ptr = match node.get_type() {
            NodeType::Node => {
                let idx = node_lookup(&node, &key, &Compare::LE)?; // navigating nodes
                let ptr = node.get_ptr(idx);

                cursor.path.push(node);
                cursor.pos.push(idx);

                Some(ptr)
            }
            NodeType::Leaf => {
                let idx = node_lookup(&node, &key, &flag)?;
                debug!(idx, "seek idx after lookup");
                cursor.path.push(node);
                cursor.pos.push(idx);

                None
            }
        }
    }
    debug!("creating cursor, pos: {:?}", cursor.pos);
    if cursor.pos.is_empty() || cursor.path.is_empty() {
        return None;
    }
    // accounting for empty key edge case
    if cursor.deref().0.is_empty() {
        return None;
    }
    assert_eq!(cursor.pos.len(), cursor.path.len());

    Some(cursor)
}

fn key_cmp(k1: &Key, k2: &Key, pred: Compare) -> bool {
    match pred {
        Compare::LT => k1 < k2,
        Compare::LE => k1 <= k2,
        Compare::GT => k1 > k2,
        Compare::GE => k1 >= k2,
        Compare::EQ => k1 == k2,
    }
}

fn node_lookup(node: &TreeNode, key: &Key, flag: &Compare) -> Option<u16> {
    if node.get_nkeys() == 0 {
        return None;
    }
    match flag {
        Compare::LT => cmp_lt(node, key),
        Compare::LE => cmp_le(node, key),
        Compare::GT => cmp_gt(node, key),
        Compare::GE => cmp_ge(node, key),
        Compare::EQ => cmp_eq(node, key),
    }
}

#[derive(Clone, Copy, PartialEq, PartialOrd)]
pub(crate) enum Compare {
    LT, // <
    LE, // <=
    GT, // >
    GE, // >=
    EQ, // ==
}

fn cmp_lt(node: &TreeNode, key: &Key) -> Option<u16> {
    let nkeys = node.get_nkeys();
    let mut lo: u16 = 0;
    let mut hi: u16 = nkeys;

    debug!(
        "cmp_lt, key: {} in {:?} nkeys {}",
        key,
        node.get_type(),
        nkeys
    );
    while hi > lo {
        let m = (hi + lo) / 2;
        let v = node.get_key(m).ok()?;
        debug!(%v, %key, "comparing v against key");
        // if v == *key {
        //     return None; // key already exists
        // };
        if v >= *key {
            // changed to larger equal
            hi = m;
        } else {
            lo = m + 1;
        }
    }
    if lo == 0 { None } else { Some(lo - 1) }
}

pub(super) fn cmp_le(node: &TreeNode, key: &Key) -> Option<u16> {
    let nkeys = node.get_nkeys();
    let mut lo: u16 = 0;
    let mut hi: u16 = nkeys;

    debug!("cmp_le in {:?} nkeys {}", node.get_type(), nkeys);
    while hi > lo {
        let m = (hi + lo) / 2; // mid point
        let v = node.get_key(m).ok()?; // key at m

        if v == *key {
            return Some(m as u16);
        };
        if v > *key {
            hi = m;
        } else {
            lo = m + 1;
        }
    }
    if lo == 0 { Some(0) } else { Some(lo - 1) }
}

fn cmp_gt(node: &TreeNode, key: &Key) -> Option<u16> {
    let nkeys = node.get_nkeys();
    let mut lo: u16 = 0;
    let mut hi: u16 = nkeys;

    debug!("cmp_gt in {:?} nkeys {}", node.get_type(), nkeys);
    while hi > lo {
        let m = (hi + lo) / 2; // mid point
        let v = node.get_key(m).ok()?; // key at m

        if v > *key {
            hi = m;
        } else {
            lo = m + 1;
        }
    }
    if lo == nkeys { None } else { Some(lo) }
}

fn cmp_ge(node: &TreeNode, key: &Key) -> Option<u16> {
    let nkeys = node.get_nkeys();
    let mut lo: u16 = 0;
    let mut hi: u16 = nkeys;

    debug!("cmp_ge in {:?} nkeys {}", node.get_type(), nkeys);
    while hi > lo {
        let m = (hi + lo) / 2; // mid point
        let v = node.get_key(m).ok()?; // key at m

        if v == *key {
            return Some(m as u16);
        };
        if v > *key {
            hi = m;
        } else {
            lo = m + 1;
        }
    }
    if lo == nkeys { None } else { Some(lo) }
}

fn cmp_eq(node: &TreeNode, key: &Key) -> Option<u16> {
    let nkeys = node.get_nkeys();
    let mut lo: u16 = 0;
    let mut hi: u16 = nkeys;

    debug!("cmp_eq in {:?} nkeys {}", node.get_type(), nkeys);
    while hi > lo {
        let m = (hi + lo) / 2; // mid point
        let v = node.get_key(m).ok()?; // key at m

        if v == *key {
            return Some(m as u16);
        };
        if v > *key {
            hi = m;
        } else {
            lo = m + 1;
        }
    }
    None
}

#[cfg(test)]
mod test {
    use super::*;
    use test_log::test;

    use crate::database::{
        btree::{
            SetFlag, Tree,
            cursor::{Compare, ScanMode},
        },
        pager::{diskpager::KVEngine, mempage_tree},
        tables::Record,
    };

    #[test]
    fn scan_single1() -> Result<()> {
        let tree = mempage_tree();

        for i in 1u16..=100u16 {
            tree.set(format!("{i}").into(), "value".into(), SetFlag::UPSERT)
                .unwrap()
        }

        for i in 1u16..=100u16 {
            let key = format!("{}", i).into();
            let tree_ref = tree.pager.btree.borrow();
            let res = scan_single(&tree_ref, &key);

            assert!(res.is_some());
            let res: Vec<Record> = res.unwrap().into_iter().map(Record::from_kv).collect();

            assert_eq!(res[0].to_string(), format!("{i} value"));
        }

        Ok(())
    }

    #[test]
    fn scan_open() -> Result<()> {
        let tree = mempage_tree();

        for i in 1i64..=10i64 {
            tree.set(i.into(), "value".into(), SetFlag::UPSERT).unwrap()
        }

        let key = 5i64.into();
        let q = ScanMode::Open(key, Compare::GT);
        let tree_ref = tree.pager.btree.borrow();
        let res = tree_ref.scan(q);

        assert!(res.is_ok());

        let mut recs = res.unwrap().map(Record::from_kv).into_iter();

        assert_eq!(recs.next().unwrap().to_string(), "6 value");
        assert_eq!(recs.next().unwrap().to_string(), "7 value");
        assert_eq!(recs.next().unwrap().to_string(), "8 value");
        assert_eq!(recs.next().unwrap().to_string(), "9 value");
        assert_eq!(recs.next().unwrap().to_string(), "10 value");
        Ok(())
    }

    #[test]
    fn cursor_next_navigation() -> Result<()> {
        let tree = mempage_tree();

        for i in 1i64..=10i64 {
            tree.set(i.into(), format!("val{}", i).into(), SetFlag::UPSERT)
                .unwrap();
        }

        let key = 1i64.into();
        let btree = tree.pager.btree.borrow();
        let mut cursor = seek(&btree, &key, Compare::EQ).unwrap();

        // Navigate through all elements using next()
        for i in 1i64..=10i64 {
            let (k, v) = cursor.next().unwrap();
            assert_eq!(k.to_string(), format!("1 0 {}", i));
            assert_eq!(v.to_string(), format!("val{}", i));
        }

        // Should return None after last element
        assert!(cursor.next().is_none());

        Ok(())
    }

    #[test]
    fn cursor_prev_navigation() -> Result<()> {
        let tree = mempage_tree();

        for i in 1i64..=10i64 {
            tree.set(i.into(), format!("val{}", i).into(), SetFlag::UPSERT)
                .unwrap();
        }

        let key = 10i64.into();
        let btree = tree.pager.btree.borrow();
        let mut cursor = seek(&btree, &key, Compare::EQ).unwrap();

        // Navigate backwards using prev()
        for i in (1i64..=10i64).rev() {
            let (k, v) = cursor.prev().unwrap();
            assert_eq!(k.to_string(), format!("1 0 {}", i));
            assert_eq!(v.to_string(), format!("val{}", i));
        }

        // Should return None before first element
        assert!(cursor.prev().is_none());

        Ok(())
    }

    #[test]
    fn scan_single_existing_keys() -> Result<()> {
        let tree = mempage_tree();

        for i in 1i64..=50i64 {
            tree.set(
                format!("{}", i).into(),
                format!("value{}", i).into(),
                SetFlag::UPSERT,
            )
            .unwrap();
        }

        // Test random keys
        for i in &[1i64, 10, 25, 40, 50] {
            let key = format!("{}", i).into();
            let tree_ref = tree.pager.btree.borrow();
            let res = scan_single(&tree_ref, &key);

            assert!(res.is_some());
            let records: Vec<Record> = res.unwrap().into_iter().map(Record::from_kv).collect();
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].to_string(), format!("{} value{}", i, i));
        }

        Ok(())
    }

    #[test]
    fn scan_single_nonexistent_key() -> Result<()> {
        let tree = mempage_tree();

        for i in 1i64..=10i64 {
            tree.set(i.into(), "value".into(), SetFlag::UPSERT).unwrap();
        }

        let key = 999i64.into();
        let tree_ref = tree.pager.btree.borrow();
        let res = scan_single(&tree_ref, &key);

        assert!(res.is_none());
        Ok(())
    }

    // Test scan_open with GT (greater than)
    #[test]
    fn scan_open_gt() -> Result<()> {
        let tree = mempage_tree();

        for i in 1i64..=10i64 {
            tree.set(i.into(), format!("val{}", i).into(), SetFlag::UPSERT)
                .unwrap();
        }

        let key = 5i64.into();
        let q = ScanMode::Open(key, Compare::GT);
        let tree_ref = tree.pager.btree.borrow();
        let res = tree_ref.scan(q);

        assert!(res.is_ok());
        let records: Vec<Record> = res.unwrap().into_iter().map(Record::from_kv).collect();

        assert_eq!(records.len(), 5);
        assert_eq!(records[0].to_string(), "6 val6");
        assert_eq!(records[4].to_string(), "10 val10");

        Ok(())
    }

    #[test]
    fn scan_open_ge() -> Result<()> {
        let tree = mempage_tree();

        for i in 1i64..=10i64 {
            tree.set(i.into(), format!("val{}", i).into(), SetFlag::UPSERT)
                .unwrap();
        }

        let key = 5i64.into();
        let q = ScanMode::Open(key, Compare::GE);
        let tree_ref = tree.pager.btree.borrow();
        let res = tree_ref.scan(q);

        assert!(res.is_ok());
        let records: Vec<Record> = res.unwrap().into_iter().map(Record::from_kv).collect();

        assert_eq!(records.len(), 6);
        assert_eq!(records[0].to_string(), "5 val5");
        assert_eq!(records[5].to_string(), "10 val10");

        Ok(())
    }
    #[test]
    fn scan_open_lt() -> Result<()> {
        let tree = mempage_tree();

        for i in 1i64..=10i64 {
            tree.set(i.into(), format!("val{}", i).into(), SetFlag::UPSERT)
                .unwrap();
        }

        let key = 6i64.into();
        let q = ScanMode::Open(key, Compare::LT);
        let tree_ref = tree.pager.btree.borrow();
        let res = tree_ref.scan(q);

        assert!(res.is_ok());
        let records: Vec<Record> = res.unwrap().into_iter().map(Record::from_kv).collect();

        assert_eq!(records.len(), 5);
        assert_eq!(records[0].to_string(), "5 val5");
        assert_eq!(records[4].to_string(), "1 val1");

        Ok(())
    }

    #[test]
    fn scan_open_le() -> Result<()> {
        let tree = mempage_tree();

        for i in 1i64..=10i64 {
            tree.set(i.into(), format!("val{}", i).into(), SetFlag::UPSERT)
                .unwrap();
        }

        let key = 6i64.into();
        let q = ScanMode::Open(key, Compare::LE);
        let tree_ref = tree.pager.btree.borrow();
        let res = tree_ref.scan(q);

        assert!(res.is_ok());
        let records: Vec<Record> = res.unwrap().into_iter().map(Record::from_kv).collect();

        assert_eq!(records.len(), 6);
        assert_eq!(records[0].to_string(), "6 val6");
        assert_eq!(records[5].to_string(), "1 val1");

        Ok(())
    }

    #[test]
    fn scan_open_from_first_element() -> Result<()> {
        let tree = mempage_tree();

        for i in 1i64..=10i64 {
            tree.set(i.into(), "value".into(), SetFlag::UPSERT).unwrap();
        }

        let key = 1i64.into();
        let q = ScanMode::Open(key, Compare::GE);
        let tree_ref = tree.pager.btree.borrow();
        let res = tree_ref.scan(q);

        assert!(res.is_ok());
        assert_eq!(res.unwrap().count(), 10);

        Ok(())
    }

    #[test]
    fn scan_open_from_last_element() -> Result<()> {
        let tree = mempage_tree();

        for i in 1i64..=10i64 {
            tree.set(i.into(), "value".into(), SetFlag::UPSERT).unwrap();
        }

        let key = 10i64.into();
        let q = ScanMode::Open(key, Compare::LE);
        let tree_ref = tree.pager.btree.borrow();
        let res = tree_ref.scan(q);

        assert!(res.is_ok());
        assert_eq!(res.unwrap().count(), 10);

        Ok(())
    }

    #[test]
    fn scan_open_beyond_range() -> Result<()> {
        let tree = mempage_tree();

        for i in 1i64..=10i64 {
            tree.set(i.into(), "value".into(), SetFlag::UPSERT).unwrap();
        }

        let tree_ref = tree.pager.btree.borrow();

        // GT from last element
        let key = 10i64.into();
        let q = ScanMode::Open(key, Compare::GT);
        let res = tree_ref.scan(q);

        assert!(res.is_err());

        // LT from first element
        let key = 1i64.into();
        let q = ScanMode::Open(key, Compare::LT);
        let res = tree_ref.scan(q);

        assert!(res.is_err());

        Ok(())
    }
    // Test with large dataset
    #[test]
    fn scan_large_dataset() -> Result<()> {
        let tree = mempage_tree();

        for i in 1i64..=1000i64 {
            tree.set(i.into(), format!("val{}", i).into(), SetFlag::UPSERT)
                .unwrap();
        }

        let key = 500i64.into();
        let q = ScanMode::Open(key, Compare::GT);
        let tree_ref = tree.pager.btree.borrow();
        let res = tree_ref.scan(q);

        assert!(res.is_ok());

        let records: Vec<Record> = res.unwrap().map(Record::from_kv).collect();

        assert_eq!(records.len(), 500);
        assert_eq!(records[0].to_string(), "501 val501");
        assert_eq!(records[499].to_string(), "1000 val1000");

        Ok(())
    }

    // Test seek with different Compare flags
    #[test]
    fn seek_with_different_compares() -> Result<()> {
        let tree = mempage_tree();

        for i in 1i64..=10i64 {
            tree.set(i.into(), format!("val{}", i).into(), SetFlag::UPSERT)
                .unwrap();
        }

        let btree = tree.pager.btree.borrow();

        // Test EQ - deref should return the exact match
        let key = 5i64.into();
        let cursor = seek(&btree, &key, Compare::EQ).unwrap();
        let (k, _) = cursor.deref();
        assert_eq!(k.to_string(), "1 0 5");

        // Test GE - deref should return the exact match or next greater
        let cursor = seek(&btree, &key, Compare::GE).unwrap();
        let (k, _) = cursor.deref();
        assert_eq!(k.to_string(), "1 0 5");

        // Test GT - deref should return the next value after key
        let cursor = seek(&btree, &key, Compare::GT).unwrap();
        let (k, _) = cursor.deref();
        assert_eq!(k.to_string(), "1 0 6");

        // Test LE - deref should return the exact match or next smaller
        let cursor = seek(&btree, &key, Compare::LE).unwrap();
        let (k, _) = cursor.deref();
        assert_eq!(k.to_string(), "1 0 5");

        // Test LT - deref should return the value before key
        let cursor = seek(&btree, &key, Compare::LT).unwrap();
        let (k, _) = cursor.deref();
        assert_eq!(k.to_string(), "1 0 4");

        Ok(())
    }

    #[test]
    fn empty_tree_scan() -> Result<()> {
        let tree = mempage_tree();

        let key = 1i64.into();
        let tree_ref = tree.pager.btree.borrow();
        let res = scan_single(&tree_ref, &key);

        assert!(res.is_none());

        let q = ScanMode::Open(1i64.into(), Compare::GT);
        let res = tree_ref.scan(q);

        assert!(res.is_err());

        Ok(())
    }

    #[test]
    fn single_element_tree() -> Result<()> {
        let tree = mempage_tree();
        tree.set(1i64.into(), "value".into(), SetFlag::UPSERT)
            .unwrap();
        let tree_ref = tree.pager.btree.borrow();

        // Scan single
        let res = scan_single(&tree_ref, &1i64.into());
        assert!(res.is_some());
        assert_eq!(res.unwrap().len(), 1);

        // Scan GT (should return none)
        let q = ScanMode::Open(1i64.into(), Compare::GT);
        let res = tree_ref.scan(q);
        assert!(res.is_err());

        // Scan GE (should return the element)
        let q = ScanMode::Open(1i64.into(), Compare::GE);
        let res = tree_ref.scan(q);
        assert!(res.is_ok());
        assert_eq!(res.unwrap().count(), 1);

        Ok(())
    }
}
