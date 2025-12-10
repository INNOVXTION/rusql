use std::fmt::Debug;
use std::rc::Weak;

use tracing::debug;
use tracing::info;

use crate::database::pager::diskpager::NodeFlag;
use crate::database::pager::diskpager::{KVEngine, Pager};
use crate::database::{btree::node::*, errors::Error, types::*};

pub(crate) struct BTree<P: Pager> {
    root_ptr: Option<Pointer>,
    pager: Weak<P>,
}

pub(crate) trait Tree {
    type Codec: Pager;

    fn insert(&mut self, key: &str, value: &str) -> Result<(), Error>;
    fn delete(&mut self, key: &str) -> Result<(), Error>;
    fn search(&self, key: &str) -> Option<String>;

    fn set_root(&mut self, ptr: Option<Pointer>);
    fn get_root(&self) -> Option<Pointer>;
}

impl<P: Pager> Tree for BTree<P> {
    type Codec = P;
    fn insert(&mut self, key: &str, val: &str) -> Result<(), Error> {
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
                new_root.kvptr_append(0, Pointer::from(0), "", "")?; // empty key to remove edge case
                new_root.kvptr_append(1, Pointer::from(0), key, val)?;
                self.root_ptr = Some(self.encode(new_root));
                return Ok(());
            }
        };
        // insert kv
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
            let key = node.get_key(0)?.to_string();
            new_root.kvptr_append(i as u16, self.encode(node), &key, "")?;
        }
        // encoding new root and updating tree ptr
        self.root_ptr = Some(self.encode(new_root));
        debug!(
            "inserted with root split, new root ptr {}",
            self.root_ptr.unwrap()
        );
        Ok(())
    }

    fn delete(&mut self, key: &str) -> Result<(), Error> {
        info!("deleting kv...");
        let root_ptr = match self.root_ptr {
            Some(n) => n,
            None => {
                return Err(Error::DeleteError(
                    "cant delete from empty tree!".to_string(),
                ));
            }
        };
        let updated = self
            .tree_delete(self.decode(root_ptr), key)
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
    fn search(&self, key: &str) -> Option<String> {
        info!("searching for {key}...");
        self.tree_search(self.decode(self.root_ptr?), key)
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
        let strong = self.pager.upgrade().unwrap();
        strong.page_read(ptr, NodeFlag::Tree).as_tree()
    }
    pub fn encode(&self, node: TreeNode) -> Pointer {
        let strong = self.pager.upgrade().unwrap();
        strong.page_alloc(Node::Tree(node))
    }
    pub fn dealloc(&self, ptr: Pointer) {
        let strong = self.pager.upgrade().unwrap();
        strong.dealloc(ptr);
    }

    /// recursive insertion, node = current node, returns updated node
    fn tree_insert(&mut self, node: TreeNode, key: &str, val: &str) -> TreeNode {
        let mut new = TreeNode::new();
        let idx = node.lookupidx(key);
        match node.get_type() {
            NodeType::Leaf => {
                // debug!(idx, "looking through leaf...");
                // for i in 0..node.get_nkeys() {
                //     debug!(idx = i, key = node.get_key(i).unwrap(), "-- keys in node:");
                // }
                // updating or inserting kv
                new.insert(node, key, val, idx);
            }
            // walking down the tree until we hit a leaf node
            NodeType::Node => {
                debug!("traversing through node...");
                for i in 0..node.get_nkeys() {
                    debug!(idx = i, key = node.get_key(i).unwrap(), "-- keys in node:");
                }
                let kptr = node.get_ptr(idx); // ptr of child below us
                let knode = BTree::tree_insert(self, self.decode(kptr), key, val); // node below us
                assert_eq!(knode.get_key(0).unwrap(), node.get_key(idx).unwrap());
                let split = knode.split().unwrap(); // potential split
                // delete old child
                self.dealloc(kptr);
                // update child ptr
                new.insert_nkids(self, node, idx, split).unwrap();
            }
        }
        new
    }

    /// recursive deletion, node = current node, returns updated node in case a deletion happened
    ///
    /// TODO: update height
    fn tree_delete(&mut self, mut node: TreeNode, key: &str) -> Option<TreeNode> {
        let idx = node.lookupidx(key);
        match node.get_type() {
            NodeType::Leaf => {
                // debug!(idx, "looking through leaf...");
                // for i in 0..node.get_nkeys() {
                //     debug!(idx = i, key = node.get_key(i).unwrap(), "-- keys in node:");
                // }
                if node.get_key(idx).unwrap() == key {
                    debug!("deleting key {key} at idx {idx}");
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
                debug!(idx, "traversing through node...");
                for i in 0..node.get_nkeys() {
                    debug!(idx = i, key = node.get_key(i).unwrap(), "-- keys in node:");
                }
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
                                        new.merge_setptr(self, node, merged_node, idx - 1).unwrap();
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
                                        new.merge_setptr(self, node, merged_node, idx).unwrap();
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
                                if key != updated_child.get_key(0).unwrap() {
                                    let cur_type = node.get_type();
                                    new.leaf_kvupdate(
                                        node,
                                        idx,
                                        updated_child.get_key(0).unwrap(),
                                        " ",
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

    fn tree_search(&self, node: TreeNode, key: &str) -> Option<String> {
        let idx = node.lookupidx(key);
        match node.get_type() {
            NodeType::Leaf => {
                // debug!(idx, "looking through leaf...");
                // for i in 0..node.get_nkeys() {
                //     debug!(idx = i, key = node.get_key(i).unwrap(), "-- keys in node:");
                // }
                if node.get_key(idx).unwrap() == key {
                    debug!("key {key} found!");
                    return Some(node.get_val(idx).unwrap().to_string());
                } else {
                    debug!("key {key} not found!");
                    None
                }
            }
            NodeType::Node => {
                debug!(idx, "traversing through node...");
                for i in 0..node.get_nkeys() {
                    debug!(idx = i, key = node.get_key(i).unwrap(), "-- keys in node:");
                }
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
    use crate::database::pager::mempage_tree;

    use super::*;
    use rand::Rng;
    use test_log::test;
    use tracing::{Level, info, span};

    #[test]
    fn simple_insert() {
        let tree = mempage_tree();
        let t_ref = tree.btree.borrow();
        info!("help i just want to log :(");

        tree.set("1", "hello").unwrap();
        tree.set("2", "world").unwrap();

        assert_eq!(tree.get("1").unwrap(), "hello");
        assert_eq!(tree.get("2").unwrap(), "world");
        assert_eq!(t_ref.decode(t_ref.get_root().unwrap()).get_nkeys(), 3);
        assert_eq!(
            t_ref.decode(t_ref.get_root().unwrap()).get_type(),
            NodeType::Leaf
        );
    }

    #[test]
    fn simple_delete() {
        let tree = mempage_tree();
        let t_ref = tree.btree.borrow();

        tree.set("1", "hello").unwrap();
        tree.set("2", "world").unwrap();
        tree.set("3", "bonjour").unwrap();
        assert_eq!(
            t_ref.decode(t_ref.get_root().unwrap()).get_type(),
            NodeType::Leaf
        );

        assert_eq!(t_ref.decode(t_ref.get_root().unwrap()).get_nkeys(), 4);
        tree.delete("2").unwrap();

        assert_eq!(tree.get("1").unwrap(), "hello");
        assert_eq!(tree.get("3").unwrap(), "bonjour");
        assert_eq!(t_ref.decode(t_ref.get_root().unwrap()).get_nkeys(), 3);
    }

    #[test]
    fn insert_split1() {
        let tree = mempage_tree();
        let t_ref = tree.btree.borrow();

        for i in 1u16..=200u16 {
            tree.set(&format!("{i}"), "value").unwrap()
        }
        assert_eq!(
            t_ref.decode(t_ref.get_root().unwrap()).get_type(),
            NodeType::Node
        );
        assert_eq!(tree.get("40").unwrap(), "value");
        assert_eq!(tree.get("90").unwrap(), "value");
        assert_eq!(tree.get("150").unwrap(), "value");
        assert_eq!(tree.get("170").unwrap(), "value");
        assert_eq!(tree.get("200").unwrap(), "value");
    }

    #[test]
    fn insert_split2() {
        let tree = mempage_tree();
        let t_ref = tree.btree.borrow();

        for i in 1u16..=400u16 {
            tree.set(&format!("{i}"), "value").unwrap()
        }
        assert_eq!(
            t_ref.decode(t_ref.get_root().unwrap()).get_type(),
            NodeType::Node
        );
        assert_eq!(tree.get("50").unwrap(), "value");
        assert_eq!(tree.get("90").unwrap(), "value");
        assert_eq!(tree.get("150").unwrap(), "value");
        assert_eq!(tree.get("170").unwrap(), "value");
        assert_eq!(tree.get("200").unwrap(), "value");
        assert_eq!(tree.get("300").unwrap(), "value");
        assert_eq!(tree.get("400").unwrap(), "value");
    }

    #[test]
    fn merge_delete1() {
        let tree = mempage_tree();
        let t_ref = tree.btree.borrow();

        let span = span!(Level::DEBUG, "test span");
        let _guard = span.enter();

        for i in 1u16..=200u16 {
            tree.set(&format!("{i}"), "value").unwrap()
        }
        for i in 1u16..=200u16 {
            tree.delete(&format!("{i}")).unwrap()
        }
        assert_eq!(
            t_ref.decode(t_ref.get_root().unwrap()).get_type(),
            NodeType::Leaf
        );
        assert_eq!(t_ref.decode(t_ref.get_root().unwrap()).get_nkeys(), 1)
    }

    #[test]
    fn merge_delete_left_right() {
        let tree = mempage_tree();
        let t_ref = tree.btree.borrow();

        for i in 1u16..=400u16 {
            tree.set(&format!("{i}"), "value").unwrap()
        }
        for i in 1u16..=400u16 {
            tree.delete(&format!("{i}")).unwrap()
        }
        assert_eq!(
            t_ref.decode(t_ref.get_root().unwrap()).get_type(),
            NodeType::Leaf
        );
        assert_eq!(t_ref.decode(t_ref.get_root().unwrap()).get_nkeys(), 1)
    }

    #[test]
    fn merge_delete_right_left() {
        let tree = mempage_tree();
        let t_ref = tree.btree.borrow();

        for i in 1u16..=400u16 {
            tree.set(&format!("{i}"), "value").unwrap()
        }
        for i in (1..=400u16).rev() {
            tree.delete(&format!("{i}")).unwrap()
        }
        assert_eq!(
            t_ref.decode(t_ref.get_root().unwrap()).get_type(),
            NodeType::Leaf
        );
        assert_eq!(t_ref.decode(t_ref.get_root().unwrap()).get_nkeys(), 1);
    }

    #[test]
    fn merge_delete3() {
        let tree = mempage_tree();
        let t_ref = tree.btree.borrow();
        let span = span!(Level::DEBUG, "test span");
        let _guard = span.enter();

        for i in 1u16..=400u16 {
            tree.set(&format!("{i}"), "value").unwrap()
        }
        for i in (1..=400u16).rev() {
            tree.delete(&format!("{i}")).unwrap()
        }
        tree.delete("").unwrap();
        assert_eq!(t_ref.get_root(), None);
    }

    #[test]
    fn insert_big() {
        let tree = mempage_tree();
        let t_ref = tree.btree.borrow();

        for i in 1u16..=1000u16 {
            tree.set(&format!("{i}"), "value").unwrap()
        }
        assert_eq!(
            t_ref.decode(t_ref.get_root().unwrap()).get_type(),
            NodeType::Node
        );
        for i in 1u16..=1000u16 {
            assert_eq!(tree.get(&format!("{i}")).unwrap(), "value")
        }
    }

    #[test]
    fn random_1k() {
        let tree = mempage_tree();
        let t_ref = tree.btree.borrow();

        for _ in 1u16..=1000 {
            tree.set(&format!("{:?}", rand::rng().random_range(1..1000)), "val")
                .unwrap()
        }
    }
}
