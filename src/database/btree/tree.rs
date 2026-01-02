use std::cell::RefCell;
use std::fmt::Debug;
use std::rc::Rc;
use std::rc::Weak;

use tracing::debug;
use tracing::info;
use tracing::instrument;

use crate::database::btree::cursor::ScanIter;
use crate::database::errors::ScanError;
use crate::database::{
    btree::{cursor::ScanMode, node::*},
    errors::{Error, Result},
    helper::debug_print_tree,
    pager::{
        EnvoyV1,
        diskpager::{KVEngine, NodeFlag, Pager},
    },
    tables::{Key, Record, Value},
    types::*,
};

pub(crate) struct BTree<P: Pager> {
    root_ptr: Option<Pointer>,
    pager: Weak<P>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum SetFlag {
    /// only add new rows
    INSERT,
    /// only modifies existing
    UPDATE,
    /// add or modify
    UPSERT,
}

pub(crate) struct SetResponse {
    old: Option<(Key, Value)>,
    added: bool,
    updated: bool,
}

pub(crate) trait Tree {
    type Codec: Pager;

    fn search(&self, key: Key) -> Option<Value>;
    fn set(&mut self, key: Key, value: Value, flag: SetFlag) -> Result<()>;
    fn scan(&self, mode: ScanMode) -> Result<ScanIter<'_, Self::Codec>>;
    fn delete(&mut self, key: Key) -> Result<()>;

    fn set_root(&mut self, ptr: Option<Pointer>);
    fn get_root(&self) -> Option<Pointer>;
}

impl<P: Pager> Tree for BTree<P> {
    type Codec = P;

    #[instrument(name = "tree insert", skip_all)]
    fn set(&mut self, key: Key, val: Value, flag: SetFlag) -> Result<()> {
        info!("inserting key: {key}, val: {val} flag: {flag:?}",);

        // get root node
        let root = match self.root_ptr {
            Some(ptr) => {
                debug!(?ptr, "getting root ptr at:");
                self.decode(ptr)
            }
            None => {
                if flag == SetFlag::UPDATE {
                    // edge case: updating in empty tree
                    return Err(Error::InsertError("cant update in empty tree".to_string()));
                }
                debug!("no root found, creating new root");

                let mut new_root = TreeNode::new();

                new_root.set_header(NodeType::Leaf, 2);
                new_root.kvptr_append(0, Pointer::from(0), Key::new_empty(), "".into())?; // empty key to remove edge case
                new_root.kvptr_append(1, Pointer::from(0), key, val)?;
                self.root_ptr = Some(self.encode(new_root));

                return Ok(());
            }
        };

        // recursively insert kv
        let updated_root = self
            .tree_insert(root.borrow().as_tn(), key, val, flag)
            .ok_or(Error::InsertError(
                "couldnt fulfill set request".to_string(),
            ))?;

        let mut split = updated_root.split()?;

        // deleting old root and creating a new one
        self.dealloc(self.root_ptr.unwrap());
        if split.0 == 1 {
            // no split, update root
            self.root_ptr = Some(self.encode(split.1.remove(0)));
            debug!(
                "inserted without root split, root ptr {}",
                self.root_ptr.unwrap()
            );
            return Ok(());
        }

        // in case of split tree grows in height
        let mut new_root = TreeNode::new();
        new_root.set_header(NodeType::Node, split.0);

        // iterate through node array from split to create new root node
        for (i, node) in split.1.into_iter().enumerate() {
            let key = node.get_key(0)?;
            new_root.kvptr_append(i as u16, self.encode(node), key, "".into())?;
        }

        // encoding new root and updating tree ptr
        self.root_ptr = Some(self.encode(new_root));
        debug!(
            "inserted with root split, new root ptr {}",
            self.root_ptr.unwrap()
        );
        Ok(())
    }

    #[instrument(name = "tree delete", skip_all)]
    fn delete(&mut self, key: Key) -> Result<()> {
        info!("deleting kv: {key}",);
        let root_ptr = match self.root_ptr {
            Some(n) => n,
            None => {
                return Err(Error::DeleteError(
                    "cant delete from empty tree!".to_string(),
                ));
            }
        };

        let updated = self
            .tree_delete(self.decode(root_ptr).borrow().as_tn(), &key)
            .ok_or(Error::DeleteError("key not found!".to_string()))?;
        self.dealloc(root_ptr);

        match updated.get_nkeys() {
            // check if tree needs to shrink in height
            1 if updated.get_type() == NodeType::Node => {
                debug!(
                    "tree shrunk, root updated to: {:?} nkeys: {}",
                    updated.get_type(),
                    updated.get_nkeys()
                );
                self.root_ptr = Some(updated.get_ptr(0));
            }
            // delete tree if node is empty
            0 => {
                debug!("tree is now empty!");
                self.root_ptr = None;
            }
            _ => {
                debug!(
                    "root updated to: {:?} nkeys: {}",
                    updated.get_type(),
                    updated.get_nkeys()
                );
                self.root_ptr = Some(self.encode(updated));
            }
        }
        Ok(())
    }

    #[instrument(name = "tree search", skip_all)]
    fn search(&self, key: Key) -> Option<Value> {
        info!("searching: {key}",);
        self.tree_search(self.decode(self.root_ptr?).borrow().as_tn(), key)
    }

    fn get_root(&self) -> Option<Pointer> {
        self.root_ptr
    }
    fn set_root(&mut self, ptr: Option<Pointer>) {
        self.root_ptr = ptr
    }
    // entry point for scan query
    fn scan(&self, mode: ScanMode) -> Result<ScanIter<'_, P>> {
        if self.root_ptr.is_none() {
            return Err(Error::SearchError("tree is empty".to_string()));
        }
        mode.into_iter(self)
            .ok_or(ScanError::IterCreateError("couldnt turn into iter".to_string()).into())
    }
}

impl<P: Pager> BTree<P> {
    pub fn new(pager: Weak<P>) -> Self {
        BTree {
            root_ptr: None,
            pager: pager,
        }
    }
    // callbacks
    pub fn decode(&self, ptr: Pointer) -> Rc<RefCell<Node>> {
        let strong = self.pager.upgrade().expect("tree callback decode failed");
        strong.page_read(ptr, NodeFlag::Tree)
    }

    pub fn encode(&self, node: TreeNode) -> Pointer {
        let strong = self.pager.upgrade().expect("tree callback encode failed");
        strong.page_alloc(Node::Tree(node))
    }

    pub fn dealloc(&self, ptr: Pointer) {
        let strong = self.pager.upgrade().expect("tree callback dealloc failed");
        strong.dealloc(ptr);
    }

    /// recursive insertion, node = current node, returns updated node
    fn tree_insert(
        &mut self,
        node: &TreeNode,
        key: Key,
        val: Value,
        flag: SetFlag,
    ) -> Option<TreeNode> {
        let mut new = TreeNode::new();
        let idx = node.lookupidx(&key);
        match node.get_type() {
            NodeType::Leaf => {
                debug_print_tree(&node, idx);

                // updating or inserting kv
                new.insert(node, key, val, idx, flag)?;
                Some(new)
            }
            // walking down the tree until we hit a leaf node
            NodeType::Node => {
                debug_print_tree(&node, idx);

                let kptr = node.get_ptr(idx); // ptr of child below us
                if let Some(knode) =
                    BTree::tree_insert(self, self.decode(kptr).borrow().as_tn(), key, val, flag)
                // node below us
                {
                    assert_eq!(knode.get_key(0).unwrap(), node.get_key(idx).unwrap());

                    // potential split
                    let split = knode.split().unwrap();

                    // delete old child
                    self.dealloc(kptr);

                    // update child ptr
                    new.insert_nkids(self, node, idx, split).unwrap();
                    Some(new)
                } else {
                    None
                }
            }
        }
    }

    /// recursive deletion, node = current node, returns updated node in case a deletion happened
    fn tree_delete(&mut self, mut node: &TreeNode, key: &Key) -> Option<TreeNode> {
        let idx = node.lookupidx(key);
        match node.get_type() {
            NodeType::Leaf => {
                debug_print_tree(&node, idx);

                if node.get_key(idx).unwrap() == *key {
                    debug!("deleting key {} at idx {idx}", key.to_string());
                    let mut new = TreeNode::new();
                    new.leaf_kvdelete(&node, idx).unwrap();
                    new.set_header(NodeType::Leaf, node.get_nkeys() - 1);
                    Some(new)
                } else {
                    debug!("key not found!");
                    None
                }
            }
            NodeType::Node => {
                debug_print_tree(&node, idx);

                let kptr = node.get_ptr(idx);
                match BTree::tree_delete(self, self.decode(kptr).borrow().as_tn(), key) {
                    // no update below us
                    None => return None,
                    // node was updated below us, checking for merge...
                    Some(updated_child) => {
                        let mut new = TreeNode::new();
                        let cur_nkeys = node.get_nkeys();

                        match node.merge_check(self, &updated_child, idx) {
                            // we need to merge
                            Some(dir) => {
                                let left: Pointer;
                                let right: Pointer;
                                let mut merged_node = TreeNode::new();
                                let merge_type = updated_child.get_type();

                                match dir {
                                    MergeDirection::Left(sibling) => {
                                        debug!(
                                            "merging {idx} with left node at idx {}, cur_nkeys {cur_nkeys}...",
                                            idx - 1
                                        );

                                        right = kptr;
                                        left = node.get_ptr(idx - 1);
                                        merged_node
                                            .merge(
                                                sibling.borrow().as_tn(),
                                                &updated_child,
                                                merge_type,
                                            )
                                            .expect("merge error when merging with left node");

                                        debug!(
                                            "merged node: type {:?} nkeys {}",
                                            merged_node.get_type(),
                                            merged_node.get_nkeys()
                                        );

                                        new.merge_setptr(self, node, merged_node, idx - 1).ok()?;
                                    }
                                    MergeDirection::Right(sibling) => {
                                        debug!(
                                            "merging {idx} with right node at idx {}, cur_nkeys {cur_nkeys}...",
                                            idx + 1
                                        );

                                        left = kptr;
                                        right = node.get_ptr(idx + 1);
                                        merged_node
                                            .merge(
                                                &updated_child,
                                                sibling.borrow().as_tn(),
                                                merge_type,
                                            )
                                            .expect("merge error when merging with right node");

                                        debug!(
                                            "merged node: type {:?} nkeys {}",
                                            merged_node.get_type(),
                                            merged_node.get_nkeys()
                                        );

                                        new.merge_setptr(self, node, merged_node, idx).ok()?;
                                    }
                                };
                                // delete old nodes
                                self.dealloc(left);
                                self.dealloc(right);
                                Some(new)
                            }
                            // no merge necessary, or no sibling to merge with
                            None => {
                                self.dealloc(kptr);
                                // empty child without siblings
                                if updated_child.get_nkeys() == 0 && cur_nkeys == 1 {
                                    assert!(idx == 0);
                                    // bubble up to be merged later
                                    new.set_header(NodeType::Node, 0);
                                    return Some(new);
                                }
                                // no merge, update new child
                                //
                                // updating key of node in case the 0th key in child got deleted
                                if *key != updated_child.get_key(0).unwrap() {
                                    let cur_type = node.get_type();
                                    new.leaf_kvupdate(
                                        node,
                                        idx,
                                        updated_child.get_key(0).unwrap(),
                                        "".into(),
                                    )
                                    .unwrap();
                                    new.set_header(cur_type, cur_nkeys);
                                    new.set_ptr(idx, self.encode(updated_child));
                                    return Some(new);
                                };
                                let mut new = node.clone();
                                new.set_ptr(idx, self.encode(updated_child));
                                debug!("key deleted without merge");
                                Some(new)
                            }
                        }
                    }
                }
            }
        }
    }

    fn tree_search(&self, node: &TreeNode, key: Key) -> Option<Value> {
        let idx = node.lookupidx(&key);
        let key_s = key.to_string();

        match node.get_type() {
            NodeType::Leaf => {
                debug_print_tree(&node, idx);

                if node.get_key(idx).unwrap() == key {
                    debug!("key {key_s:?} found!");
                    return Some(node.get_val(idx).unwrap());
                } else {
                    debug!("key {key_s:?} not found!");
                    None
                }
            }
            NodeType::Node => {
                debug_print_tree(&node, idx);

                let kptr = node.get_ptr(idx);
                BTree::tree_search(self, self.decode(kptr).borrow().as_tn(), key)
            }
        }
    }
}

impl<P: Pager> Debug for BTree<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BTree")
            .field("root_ptr", &self.root_ptr)
            .finish()
    }
}

#[cfg(test)]
mod test {
    use std::error::Error;

    use crate::database::pager::mempage_tree;

    use super::*;
    use rand::Rng;
    use test_log::test;
    use tracing::{Level, info, span};

    #[test]
    fn simple_insert() {
        let tree = mempage_tree();
        tree.set("1".into(), "hello".into(), SetFlag::UPSERT)
            .unwrap();
        tree.set("2".into(), "world".into(), SetFlag::UPSERT)
            .unwrap();

        let t_ref = tree.pager.btree.borrow();

        assert_eq!(tree.get("1".into()).unwrap(), "hello".into());
        assert_eq!(tree.get("2".into()).unwrap(), "world".into());
        assert_eq!(
            t_ref
                .decode(t_ref.get_root().unwrap())
                .borrow()
                .as_tn()
                .get_nkeys(),
            3
        );
        assert_eq!(
            t_ref
                .decode(t_ref.get_root().unwrap())
                .borrow()
                .as_tn()
                .get_type(),
            NodeType::Leaf
        );
    }

    #[test]
    fn simple_delete() {
        let tree = mempage_tree();

        tree.set("1".into(), "hello".into(), SetFlag::UPSERT)
            .unwrap();
        tree.set("2".into(), "world".into(), SetFlag::UPSERT)
            .unwrap();
        tree.set("3".into(), "bonjour".into(), SetFlag::UPSERT)
            .unwrap();
        {
            let t_ref = tree.pager.btree.borrow();
            assert_eq!(
                t_ref
                    .decode(t_ref.get_root().unwrap())
                    .borrow()
                    .as_tn()
                    .get_type(),
                NodeType::Leaf
            );

            assert_eq!(
                t_ref
                    .decode(t_ref.get_root().unwrap())
                    .borrow()
                    .as_tn()
                    .get_nkeys(),
                4
            );
        }

        tree.delete("2".into()).unwrap();
        let t_ref = tree.pager.btree.borrow();
        assert_eq!(tree.get("1".into()).unwrap(), "hello".into());
        assert_eq!(tree.get("3".into()).unwrap(), "bonjour".into());
        assert_eq!(
            t_ref
                .decode(t_ref.get_root().unwrap())
                .borrow()
                .as_tn()
                .get_nkeys(),
            3
        );
    }

    #[test]
    fn insert_split1() {
        let tree = mempage_tree();

        for i in 1u16..=200u16 {
            tree.set(format!("{i}").into(), "value".into(), SetFlag::UPSERT)
                .unwrap()
        }
        let t_ref = tree.pager.btree.borrow();
        assert_eq!(
            t_ref
                .decode(t_ref.get_root().unwrap())
                .borrow()
                .as_tn()
                .get_type(),
            NodeType::Node
        );
        assert_eq!(tree.get("40".into()).unwrap(), "value".into());
        assert_eq!(tree.get("90".into()).unwrap(), "value".into());
        assert_eq!(tree.get("150".into()).unwrap(), "value".into());
        assert_eq!(tree.get("170".into()).unwrap(), "value".into());
        assert_eq!(tree.get("200".into()).unwrap(), "value".into());
    }

    #[test]
    fn insert_split2() {
        let tree = mempage_tree();

        for i in 1u16..=400u16 {
            tree.set(format!("{i}").into(), "value".into(), SetFlag::UPSERT)
                .unwrap()
        }
        let t_ref = tree.pager.btree.borrow();
        assert_eq!(
            t_ref
                .decode(t_ref.get_root().unwrap())
                .borrow()
                .as_tn()
                .get_type(),
            NodeType::Node
        );
        assert_eq!(tree.get("40".into()).unwrap(), "value".into());
        assert_eq!(tree.get("90".into()).unwrap(), "value".into());
        assert_eq!(tree.get("150".into()).unwrap(), "value".into());
        assert_eq!(tree.get("170".into()).unwrap(), "value".into());
        assert_eq!(tree.get("200".into()).unwrap(), "value".into());
        assert_eq!(tree.get("300".into()).unwrap(), "value".into());
        assert_eq!(tree.get("400".into()).unwrap(), "value".into());
    }

    #[test]
    fn merge_delete1() {
        let tree = mempage_tree();

        let span = span!(Level::DEBUG, "test span");
        let _guard = span.enter();

        for i in 1u16..=200u16 {
            tree.set(format!("{i}").into(), "value".into(), SetFlag::UPSERT)
                .unwrap()
        }
        for i in 1u16..=200u16 {
            tree.delete(format!("{i}").into()).unwrap()
        }
        let t_ref = tree.pager.btree.borrow();
        assert_eq!(
            t_ref
                .decode(t_ref.get_root().unwrap())
                .borrow()
                .as_tn()
                .get_type(),
            NodeType::Leaf
        );
        assert_eq!(
            t_ref
                .decode(t_ref.get_root().unwrap())
                .borrow()
                .as_tn()
                .get_nkeys(),
            1
        )
    }

    #[test]
    fn merge_delete_left_right() {
        let tree = mempage_tree();

        for i in 1u16..=400u16 {
            tree.set(format!("{i}").into(), "value".into(), SetFlag::UPSERT)
                .unwrap()
        }
        for i in 1u16..=400u16 {
            tree.delete(format!("{i}").into()).unwrap()
        }
        let t_ref = tree.pager.btree.borrow();
        assert_eq!(
            t_ref
                .decode(t_ref.get_root().unwrap())
                .borrow()
                .as_tn()
                .get_type(),
            NodeType::Leaf
        );
        assert_eq!(
            t_ref
                .decode(t_ref.get_root().unwrap())
                .borrow()
                .as_tn()
                .get_nkeys(),
            1
        )
    }

    #[test]
    fn merge_delete_right_left() {
        let tree = mempage_tree();

        for i in 1u16..=400u16 {
            tree.set(format!("{i}").into(), "value".into(), SetFlag::UPSERT)
                .unwrap()
        }
        for i in (1..=400u16).rev() {
            tree.delete(format!("{i}").into()).unwrap()
        }
        let t_ref = tree.pager.btree.borrow();
        assert_eq!(
            t_ref
                .decode(t_ref.get_root().unwrap())
                .borrow()
                .as_tn()
                .get_type(),
            NodeType::Leaf
        );
        assert_eq!(
            t_ref
                .decode(t_ref.get_root().unwrap())
                .borrow()
                .as_tn()
                .get_nkeys(),
            1
        );
    }

    #[test]
    fn merge_delete3() {
        let tree = mempage_tree();
        let span = span!(Level::DEBUG, "test span");
        let _guard = span.enter();

        for i in 1u16..=400u16 {
            tree.set(format!("{i}").into(), "value".into(), SetFlag::UPSERT)
                .unwrap()
        }
        for i in (1..=400u16).rev() {
            tree.delete(format!("{i}").into()).unwrap()
        }
        tree.delete(Key::new_empty()).unwrap();
        let t_ref = tree.pager.btree.borrow();
        assert_eq!(t_ref.get_root(), None);
    }

    #[test]
    fn insert_big() {
        let tree = mempage_tree();

        for i in 1u16..=1000u16 {
            tree.set(format!("{i}").into(), "value".into(), SetFlag::UPSERT)
                .unwrap()
        }
        {
            let t_ref = tree.pager.btree.borrow();
            assert_eq!(
                t_ref
                    .decode(t_ref.get_root().unwrap())
                    .borrow()
                    .as_tn()
                    .get_type(),
                NodeType::Node
            );
        }
        for i in 1u16..=1000u16 {
            assert_eq!(tree.get(format!("{i}").into()).unwrap(), "value".into())
        }
    }

    #[test]
    fn random_1k() {
        let tree = mempage_tree();

        for _ in 1u16..=1000 {
            tree.set(
                format!("{:?}", rand::rng().random_range(1..1000)).into(),
                "val".into(),
                SetFlag::UPSERT,
            )
            .unwrap()
        }
    }

    #[test]
    fn update_existing_key() {
        let tree = mempage_tree();

        tree.set("key".into(), "value1".into(), SetFlag::UPSERT)
            .unwrap();
        // overwrite same key
        tree.set("key".into(), "value2".into(), SetFlag::UPSERT)
            .unwrap();

        // value should be updated
        assert_eq!(tree.get("key".into()).unwrap(), "value2".into());
    }

    #[test]
    fn delete_nonexistent() {
        let tree = mempage_tree();

        // deleting a missing key should return an error (DeleteError)
        let res = tree.delete("this-key-does-not-exist".into());
        assert!(
            res.is_err(),
            "expected error when deleting non-existent key"
        );
    }

    // wip return error
    #[should_panic]
    #[test]
    fn value_too_large_rejected() {
        let tree = mempage_tree();

        let key = "big".to_string();
        let too_big = "🚀".repeat(2000); // 

        assert!(
            tree.set(key.into(), too_big.into(), SetFlag::UPSERT)
                .is_err()
        );
    }

    #[test]
    fn reinsert_after_delete() {
        let tree = mempage_tree();

        tree.set("re".into(), "first".into(), SetFlag::UPSERT)
            .unwrap();
        tree.delete("re".into()).unwrap();
        // now reinsert with a different value
        tree.set("re".into(), "second".into(), SetFlag::UPSERT)
            .unwrap();

        assert_eq!(tree.get("re".into()).unwrap(), "second".into());
    }

    #[test]
    fn encode_decode_roundtrip() {
        // direct encode/decode roundtrip for a single node using the pager/btree ref
        let tree = mempage_tree();
        let t_ref = tree.pager.btree.borrow();

        let mut node = TreeNode::new();
        node.set_header(NodeType::Leaf, 1);
        node.kvptr_append(0, Pointer::from(0), "round".into(), "trip".into())
            .unwrap();

        let ptr = t_ref.encode(node);
        let decoded = t_ref.decode(ptr);

        assert_eq!(decoded.borrow().as_tn().get_type(), NodeType::Leaf);
        assert_eq!(decoded.borrow().as_tn().get_nkeys(), 1);
        assert_eq!(decoded.borrow().as_tn().get_key(0).unwrap(), "round".into());
        assert_eq!(decoded.borrow().as_tn().get_val(0).unwrap(), "trip".into());

        // cleanup
        t_ref.dealloc(ptr);
    }

    #[test]
    fn random_ops_oracle() {
        use rand::Rng;
        use std::collections::BTreeMap;

        let mut rng = rand::rng();
        let tree = mempage_tree();

        // oracle stores logical (user-level) keys
        let mut oracle: BTreeMap<String, String> = BTreeMap::new();

        for _ in 0..1500 {
            let k = format!("{}", rng.random_range(1..=800));

            if rng.random_bool(0.7) {
                // insert
                let v = format!("v{}", rng.random::<u32>());
                tree.set(k.clone().into(), v.clone().into(), SetFlag::UPSERT)
                    .unwrap();
                oracle.insert(k, v);
            } else {
                // delete (ignore error if key is missing)
                let _ = tree.delete(k.clone().into());
                oracle.remove(&k);
            }
        }

        // all oracle keys must exist in tree with correct value
        for (k, v) in oracle.iter() {
            let res = tree.get(k.clone().into());
            assert!(res.is_ok(), "expected key {} to exist in tree", k);
            assert_eq!(
                res.unwrap(),
                v.clone().into(),
                "value mismatch for key {}",
                k
            );
        }

        // spot-check some keys that likely do not exist
        for _ in 0..50 {
            let k = format!("{}", rng.random_range(900..=1200));
            if !oracle.contains_key(&k) {
                assert!(
                    tree.get(k.clone().into()).is_err(),
                    "unexpected key {} found in tree",
                    k
                );
            }
        }
    }

    #[test]
    fn unicode_and_long_values() {
        let tree = mempage_tree();

        // 🚀 is 4 bytes in UTF-8 → 750 × 4 = 3000 bytes (value size cap)
        let key = "ключ-ユニコード-🧪".to_string();
        let long_val = "🚀".repeat(750);

        assert_eq!(long_val.len(), 3000);

        tree.set(key.clone().into(), long_val.clone().into(), SetFlag::UPSERT)
            .unwrap();

        assert_eq!(tree.get(key.into()).unwrap(), long_val.into());
    }

    #[test]
    fn insert_flag_rejects_duplicates() {
        let tree = mempage_tree();

        tree.set("key1".into(), "value1".into(), SetFlag::INSERT)
            .unwrap();

        // INSERT flag should reject duplicate key
        let result = tree.set("key1".into(), "value2".into(), SetFlag::INSERT);
        assert!(result.is_err(), "INSERT flag should reject duplicate keys");
    }

    #[test]
    fn update_flag_rejects_new_keys() {
        let tree = mempage_tree();

        // UPDATE flag should reject non-existent key
        let result = tree.set("nonexistent".into(), "value".into(), SetFlag::UPDATE);
        assert!(result.is_err(), "UPDATE flag should reject new keys");
    }

    #[test]
    fn update_flag_modifies_existing() {
        let tree = mempage_tree();

        tree.set("key1".into(), "value1".into(), SetFlag::UPSERT)
            .unwrap();

        // UPDATE flag should modify existing key
        tree.set("key1".into(), "value2".into(), SetFlag::UPDATE)
            .unwrap();

        assert_eq!(tree.get("key1".into()).unwrap(), "value2".into());
    }

    #[test]
    fn upsert_flag_inserts_new() {
        let tree = mempage_tree();

        tree.set("key1".into(), "value1".into(), SetFlag::UPSERT)
            .unwrap();

        assert_eq!(tree.get("key1".into()).unwrap(), "value1".into());
    }

    #[test]
    fn upsert_flag_updates_existing() {
        let tree = mempage_tree();

        tree.set("key1".into(), "value1".into(), SetFlag::UPSERT)
            .unwrap();
        tree.set("key1".into(), "value2".into(), SetFlag::UPSERT)
            .unwrap();

        assert_eq!(tree.get("key1".into()).unwrap(), "value2".into());
    }

    #[test]
    fn insert_flag_multiple_keys() {
        let tree = mempage_tree();

        for i in 1..=10 {
            tree.set(
                format!("key{}", i).into(),
                format!("val{}", i).into(),
                SetFlag::INSERT,
            )
            .unwrap();
        }

        for i in 1..=10 {
            assert_eq!(
                tree.get(format!("key{}", i).into()).unwrap(),
                format!("val{}", i).into()
            );
        }
    }

    #[test]
    fn update_flag_multiple_existing_keys() {
        let tree = mempage_tree();

        for i in 1..=10 {
            tree.set(
                format!("key{}", i).into(),
                format!("val{}", i).into(),
                SetFlag::UPSERT,
            )
            .unwrap();
        }

        for i in 1..=10 {
            tree.set(
                format!("key{}", i).into(),
                format!("updated{}", i).into(),
                SetFlag::UPDATE,
            )
            .unwrap();
        }

        for i in 1..=10 {
            assert_eq!(
                tree.get(format!("key{}", i).into()).unwrap(),
                format!("updated{}", i).into()
            );
        }
    }
}
