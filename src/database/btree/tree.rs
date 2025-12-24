use std::fmt::Debug;
use std::rc::Weak;

use tracing::debug;
use tracing::info;
use tracing::instrument;

use crate::database::btree::cursor::scan_open;
use crate::database::btree::cursor::scan_single;
use crate::database::{
    btree::{
        cursor::{Compare, Cursor, ScanMode},
        node::*,
    },
    errors::Error,
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

pub(crate) trait Tree {
    type Codec: Pager;

    fn insert(&mut self, key: Key, value: Value) -> Result<(), Error>;
    fn delete(&mut self, key: Key) -> Result<(), Error>;
    fn search(&self, key: Key) -> Option<Value>;
    fn query(&self, mode: ScanMode) -> Option<Vec<(Key, Value)>>;

    fn set_root(&mut self, ptr: Option<Pointer>);
    fn get_root(&self) -> Option<Pointer>;
}

impl<P: Pager> Tree for BTree<P> {
    type Codec = P;

    #[instrument(name = "tree insert", skip_all)]
    fn insert(&mut self, key: Key, val: Value) -> Result<(), Error> {
        info!("inserting key: {key}, val: {val}",);
        // get root node
        let root = match self.root_ptr {
            Some(ptr) => {
                debug!(?ptr, "getting root ptr at:");
                self.decode(ptr)
            }
            None => {
                debug!("no root found, creating new root");
                let mut new_root = TreeNode::new();
                new_root.set_header(NodeType::Leaf, 2);
                new_root.kvptr_append(0, Pointer::from(0), "".into(), "".into())?; // empty key to remove edge case
                new_root.kvptr_append(1, Pointer::from(0), key, val)?;
                self.root_ptr = Some(self.encode(new_root));
                return Ok(());
            }
        };

        // recursively insert kv
        let updated_root = self.tree_insert(root, key, val);
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
    fn delete(&mut self, key: Key) -> Result<(), Error> {
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
            .tree_delete(self.decode(root_ptr), &key)
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
        self.tree_search(self.decode(self.root_ptr?), key)
    }

    // entry point for scan query
    fn query(&self, mode: ScanMode) -> Option<Vec<(Key, Value)>> {
        if self.root_ptr.is_none() {
            return None;
        }
        match mode {
            ScanMode::Single(key) => scan_single(self, &key),
            ScanMode::Open(key, compare) => scan_open(self, &key, compare),
            ScanMode::Closed { lo, hi } => todo!(),
        }
    }

    fn get_root(&self) -> Option<Pointer> {
        self.root_ptr
    }
    fn set_root(&mut self, ptr: Option<Pointer>) {
        self.root_ptr = ptr
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
    pub fn decode(&self, ptr: Pointer) -> TreeNode {
        let strong = self.pager.upgrade().expect("tree callback decode failed");
        strong.page_read(ptr, NodeFlag::Tree).as_tree()
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
    fn tree_insert(&mut self, node: TreeNode, key: Key, val: Value) -> TreeNode {
        let mut new = TreeNode::new();
        let idx = node.lookupidx(&key);
        match node.get_type() {
            NodeType::Leaf => {
                debug_print_tree(&node, idx);

                // updating or inserting kv
                new.insert(node, key, val, idx);
            }
            // walking down the tree until we hit a leaf node
            NodeType::Node => {
                debug_print_tree(&node, idx);

                let kptr = node.get_ptr(idx); // ptr of child below us
                let knode = BTree::tree_insert(self, self.decode(kptr), key, val); // node below us
                assert_eq!(knode.get_key(0).unwrap(), node.get_key(idx).unwrap());

                // potential split
                let split = knode.split().unwrap();

                // delete old child
                self.dealloc(kptr);

                // update child ptr
                new.insert_nkids(self, node, idx, split).unwrap();
            }
        }
        new
    }

    /// recursive deletion, node = current node, returns updated node in case a deletion happened
    fn tree_delete(&mut self, mut node: TreeNode, key: &Key) -> Option<TreeNode> {
        let idx = node.lookupidx(key);
        match node.get_type() {
            NodeType::Leaf => {
                debug_print_tree(&node, idx);

                if node.get_key(idx).unwrap() == *key {
                    debug!("deleting key {} at idx {idx}", key.to_string().unwrap());
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
                match BTree::tree_delete(self, self.decode(kptr), key) {
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
                                            .merge(sibling, updated_child, merge_type)
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
                                            .merge(updated_child, sibling, merge_type)
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
                                        " ".into(),
                                    )
                                    .unwrap();
                                    new.set_header(cur_type, cur_nkeys);
                                    new.set_ptr(idx, self.encode(updated_child));
                                    return Some(new);
                                };
                                node.set_ptr(idx, self.encode(updated_child));
                                debug!("key deleted without merge");
                                Some(node)
                            }
                        }
                    }
                }
            }
        }
    }

    fn tree_search(&self, node: TreeNode, key: Key) -> Option<Value> {
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
                BTree::tree_search(self, self.decode(kptr), key)
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
        tree.set("1".into(), "hello".into()).unwrap();
        tree.set("2".into(), "world".into()).unwrap();

        let t_ref = tree.pager.btree.borrow();

        assert_eq!(tree.get("1".into()).unwrap(), "hello".into());
        assert_eq!(tree.get("2".into()).unwrap(), "world".into());
        assert_eq!(t_ref.decode(t_ref.get_root().unwrap()).get_nkeys(), 3);
        assert_eq!(
            t_ref.decode(t_ref.get_root().unwrap()).get_type(),
            NodeType::Leaf
        );
    }

    #[test]
    fn simple_delete() {
        let tree = mempage_tree();

        tree.set("1".into(), "hello".into()).unwrap();
        tree.set("2".into(), "world".into()).unwrap();
        tree.set("3".into(), "bonjour".into()).unwrap();
        {
            let t_ref = tree.pager.btree.borrow();
            assert_eq!(
                t_ref.decode(t_ref.get_root().unwrap()).get_type(),
                NodeType::Leaf
            );

            assert_eq!(t_ref.decode(t_ref.get_root().unwrap()).get_nkeys(), 4);
        }

        tree.delete("2".into()).unwrap();
        let t_ref = tree.pager.btree.borrow();
        assert_eq!(tree.get("1".into()).unwrap(), "hello".into());
        assert_eq!(tree.get("3".into()).unwrap(), "bonjour".into());
        assert_eq!(t_ref.decode(t_ref.get_root().unwrap()).get_nkeys(), 3);
    }

    #[test]
    fn insert_split1() {
        let tree = mempage_tree();

        for i in 1u16..=200u16 {
            tree.set(format!("{i}").into(), "value".into()).unwrap()
        }
        let t_ref = tree.pager.btree.borrow();
        assert_eq!(
            t_ref.decode(t_ref.get_root().unwrap()).get_type(),
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
            tree.set(format!("{i}").into(), "value".into()).unwrap()
        }
        let t_ref = tree.pager.btree.borrow();
        assert_eq!(
            t_ref.decode(t_ref.get_root().unwrap()).get_type(),
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
            tree.set(format!("{i}").into(), "value".into()).unwrap()
        }
        for i in 1u16..=200u16 {
            tree.delete(format!("{i}").into()).unwrap()
        }
        let t_ref = tree.pager.btree.borrow();
        assert_eq!(
            t_ref.decode(t_ref.get_root().unwrap()).get_type(),
            NodeType::Leaf
        );
        assert_eq!(t_ref.decode(t_ref.get_root().unwrap()).get_nkeys(), 1)
    }

    #[test]
    fn merge_delete_left_right() {
        let tree = mempage_tree();

        for i in 1u16..=400u16 {
            tree.set(format!("{i}").into(), "value".into()).unwrap()
        }
        for i in 1u16..=400u16 {
            tree.delete(format!("{i}").into()).unwrap()
        }
        let t_ref = tree.pager.btree.borrow();
        assert_eq!(
            t_ref.decode(t_ref.get_root().unwrap()).get_type(),
            NodeType::Leaf
        );
        assert_eq!(t_ref.decode(t_ref.get_root().unwrap()).get_nkeys(), 1)
    }

    #[test]
    fn merge_delete_right_left() {
        let tree = mempage_tree();

        for i in 1u16..=400u16 {
            tree.set(format!("{i}").into(), "value".into()).unwrap()
        }
        for i in (1..=400u16).rev() {
            tree.delete(format!("{i}").into()).unwrap()
        }
        let t_ref = tree.pager.btree.borrow();
        assert_eq!(
            t_ref.decode(t_ref.get_root().unwrap()).get_type(),
            NodeType::Leaf
        );
        assert_eq!(t_ref.decode(t_ref.get_root().unwrap()).get_nkeys(), 1);
    }

    #[test]
    fn merge_delete3() {
        let tree = mempage_tree();
        let span = span!(Level::DEBUG, "test span");
        let _guard = span.enter();

        for i in 1u16..=400u16 {
            tree.set(format!("{i}").into(), "value".into()).unwrap()
        }
        for i in (1..=400u16).rev() {
            tree.delete(format!("{i}").into()).unwrap()
        }
        tree.delete("".into()).unwrap();
        let t_ref = tree.pager.btree.borrow();
        assert_eq!(t_ref.get_root(), None);
    }

    #[test]
    fn insert_big() {
        let tree = mempage_tree();

        for i in 1u16..=1000u16 {
            tree.set(format!("{i}").into(), "value".into()).unwrap()
        }
        {
            let t_ref = tree.pager.btree.borrow();
            assert_eq!(
                t_ref.decode(t_ref.get_root().unwrap()).get_type(),
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
            )
            .unwrap()
        }
    }

    #[test]
    fn scan_single() -> Result<(), Box<dyn Error>> {
        let tree = mempage_tree();

        for i in 1u16..=100u16 {
            tree.set(format!("{i}").into(), "value".into()).unwrap()
        }

        for i in 1u16..=100u16 {
            let key = format!("{i}").into();
            let q = ScanMode::Single(key);
            let res = tree.pager.btree.borrow().query(q);

            assert!(res.is_some());
            let res: Vec<Record> = res.unwrap().into_iter().map(Record::from_kv).collect();

            assert_eq!(res[0].to_string()?, format!("{i}value"));
        }

        Ok(())
    }

    #[test]
    fn scan_open() -> Result<(), Box<dyn Error>> {
        let tree = mempage_tree();

        for i in 1i64..=10i64 {
            tree.set(i.into(), "value".into()).unwrap()
        }

        let key = 5i64.into();
        let q = ScanMode::Open(key, Compare::GT);
        let res = tree.pager.btree.borrow().query(q);

        assert!(res.is_some());
        assert_eq!(res.as_ref().unwrap().len(), 5);

        let mut recs = res.unwrap().into_iter().map(Record::from_kv).into_iter();

        assert_eq!(recs.next().unwrap().to_string()?, "6value");
        assert_eq!(recs.next().unwrap().to_string()?, "7value");
        assert_eq!(recs.next().unwrap().to_string()?, "8value");
        assert_eq!(recs.next().unwrap().to_string()?, "9value");
        assert_eq!(recs.next().unwrap().to_string()?, "10value");
        Ok(())
    }
}
