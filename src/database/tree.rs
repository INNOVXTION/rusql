use std::fmt::Debug;

use tracing::debug;
use tracing::info;

use crate::database::{errors::Error, node::*, types::*};

pub struct BTree<'a> {
    pub root_ptr: Option<Pointer>,
    // callbacks
    pub decode: Box<dyn Fn(&Pointer) -> Node + 'a>, // get
    pub encode: Box<dyn FnMut(Node) -> Pointer + 'a>, // set
    pub dealloc: Box<dyn FnMut(Pointer) + 'a>,      // del
}

impl<'a> Debug for BTree<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BTree")
            .field("root_ptr", &self.root_ptr)
            .finish()
    }
}

impl<'a> BTree<'a> {
    pub fn insert(&mut self, key: &str, val: &str) -> Result<(), Error> {
        info!("inserting new kv...");
        // get root node
        let root = match self.root_ptr {
            Some(n) => (self.decode)(&n),
            None => {
                debug!("no root found, creating new root");
                let mut new_root = Node::new();
                new_root.set_header(NodeType::Leaf, 2);
                new_root.kvptr_append(0, Pointer::from(0), "", "")?; // empty key to remove edge case
                new_root.kvptr_append(1, Pointer::from(0), key, val)?;
                self.root_ptr = Some((self.encode)(new_root));
                return Ok(());
            }
        };
        // insert kv
        let updated_root = BTree::tree_insert(self, root, key, val);
        let mut split = updated_root.split()?;
        // deleting old root and creating a new one
        (self.dealloc)(self.root_ptr.unwrap());
        if split.0 == 1 {
            // no split, update root
            self.root_ptr = Some((self.encode)(split.1.remove(0)));
            debug!(
                "inserted without root split, root ptr {}",
                self.root_ptr.unwrap()
            );
            return Ok(());
        }
        // in case of split tree grows in height
        let mut new_root = Node::new();
        new_root.set_header(NodeType::Node, split.0);
        // iterate through node array from split to create new root node
        for (i, node) in split.1.into_iter().enumerate() {
            let key = node.get_key(0)?.to_string();
            new_root.kvptr_append(i as u16, (self.encode)(node), &key, "")?;
        }
        // encoding new root and updating tree ptr
        self.root_ptr = Some((self.encode)(new_root));
        debug!(
            "inserted with root split, new root ptr {}",
            self.root_ptr.unwrap()
        );
        Ok(())
    }

    /// recursive insertion, node = current node, returns updated node
    fn tree_insert(tree: &mut BTree, node: Node, key: &str, val: &str) -> Node {
        let mut new = Node::new();
        let idx = node.lookupidx(key);
        match node.get_type() {
            NodeType::Leaf => {
                // updating or inserting kv
                new.insert(node, key, val, idx);
            }
            // walking down the tree until we hit a leaf node
            NodeType::Node => {
                debug!(
                    "traversing through node, at idx: {idx} key: {}",
                    node.get_key(idx).unwrap()
                );
                let kptr = node.get_ptr(idx); // ptr of child below us
                let knode = BTree::tree_insert(tree, (tree.decode)(&kptr), key, val); // node below us
                let split = knode.split().unwrap(); // potential split
                // delete old child
                (tree.dealloc)(kptr);
                // update child ptr
                new.insert_nkids(tree, node, idx, split).unwrap();
            }
        }
        new
    }

    pub fn delete(&mut self, key: &str) -> Result<(), Error> {
        info!("deleting kv...");
        let root_ptr = match self.root_ptr {
            Some(n) => n,
            None => {
                return Err(Error::DeleteError(
                    "cant delete from empty tree!".to_string(),
                ));
            }
        };
        let updated = BTree::tree_delete(self, (self.decode)(&root_ptr), key)
            .ok_or(Error::DeleteError("key not found!".to_string()))?;
        (self.dealloc)(root_ptr);
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
                self.root_ptr = Some((self.encode)(updated));
            }
        }
        Ok(())
    }

    /// recursive deletion, node = current node, returns updated node in case a deletion happened
    ///
    /// TODO: update height
    fn tree_delete(tree: &mut BTree, mut node: Node, key: &str) -> Option<Node> {
        let idx = node.lookupidx(key);
        match node.get_type() {
            NodeType::Leaf => {
                if let Some(i) = node.searchidx(key) {
                    debug!("deleting key {key} at idx {idx}");
                    let mut new = Node::new();
                    new.leaf_kvdelete(&node, i).unwrap();
                    new.set_header(NodeType::Leaf, node.get_nkeys() - 1);
                    return Some(new);
                }
                debug!("key not found!");
                None
            }
            NodeType::Node => {
                debug!(
                    "traversing through node, at idx: {idx} key: {}",
                    node.get_key(idx).unwrap()
                );
                let kptr = node.get_ptr(idx);
                match BTree::tree_delete(tree, (tree.decode)(&kptr), key) {
                    // no update below us
                    None => return None,
                    // node was updated below us, checking for merge...
                    Some(updated_child) => {
                        let mut new = Node::new();
                        let cur_nkeys = node.get_nkeys();
                        match node.merge_check(tree, &updated_child, idx) {
                            // we need to merge
                            Some(dir) => {
                                let left: Pointer;
                                let right: Pointer;
                                let mut merged_node = Node::new();
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
                                        new.merge_setptr(tree, node, merged_node, idx - 1).unwrap();
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
                                        new.merge_setptr(tree, node, merged_node, idx).unwrap();
                                    }
                                };
                                // delete old nodes
                                (tree.dealloc)(left);
                                (tree.dealloc)(right);
                                Some(new)
                            }
                            // no merge necessary, or no sibling to merge with
                            None => {
                                (tree.dealloc)(kptr);
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
                                    new.set_ptr(idx, (tree.encode)(updated_child));
                                    return Some(new);
                                };
                                node.set_ptr(idx, (tree.encode)(updated_child));
                                debug!("key deleted without merge");
                                Some(node)
                            }
                        }
                    }
                }
            }
        }
    }

    pub fn search(&self, key: &str) -> Option<String> {
        info!("searching for {key}...");
        BTree::tree_search(self, (self.decode)(&self.root_ptr?), key)
    }

    fn tree_search(tree: &BTree, node: Node, key: &str) -> Option<String> {
        let idx = node.lookupidx(key);
        match node.get_type() {
            NodeType::Leaf => {
                if let Some(i) = node.searchidx(key) {
                    debug!("key {key} found!");
                    return Some(node.get_val(i).unwrap().to_string());
                }
                debug!("key {key} not found!");
                None
            }
            NodeType::Node => {
                debug!("traversing through node, at idx: {idx} key: {}", key);
                let kptr = node.get_ptr(idx);
                BTree::tree_search(tree, (tree.decode)(&kptr), key)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::database::pager::mempage_tree;

    use super::*;
    use rand::Rng;
    use test_log::test;
    use tracing::info;

    #[test]
    fn simple_insert() {
        let mut tree = mempage_tree();
        info!("help i just want to log :(");

        tree.insert("1", "hello").unwrap();
        tree.insert("2", "world").unwrap();

        assert_eq!(tree.search("1").unwrap(), "hello");
        assert_eq!(tree.search("2").unwrap(), "world");
        assert_eq!((tree.decode)(&tree.root_ptr.unwrap()).get_nkeys(), 3);
        assert_eq!(
            (tree.decode)(&tree.root_ptr.unwrap()).get_type(),
            NodeType::Leaf
        );
    }

    #[test]
    fn simple_delete() {
        let mut tree = mempage_tree();

        tree.insert("1", "hello").unwrap();
        tree.insert("2", "world").unwrap();
        tree.insert("3", "bonjour").unwrap();
        assert_eq!(
            (tree.decode)(&tree.root_ptr.unwrap()).get_type(),
            NodeType::Leaf
        );

        assert_eq!((tree.decode)(&tree.root_ptr.unwrap()).get_nkeys(), 4);
        tree.delete("2").unwrap();

        assert_eq!(tree.search("1").unwrap(), "hello");
        assert_eq!(tree.search("3").unwrap(), "bonjour");
        assert_eq!((tree.decode)(&tree.root_ptr.unwrap()).get_nkeys(), 3);
    }

    #[test]
    fn insert_split1() {
        let mut tree = mempage_tree();

        for i in 1u16..=200u16 {
            tree.insert(&format!("{i}"), "value").unwrap()
        }
        assert_eq!(
            (tree.decode)(&tree.root_ptr.unwrap()).get_type(),
            NodeType::Node
        );
        assert_eq!(tree.search("40").unwrap(), "value");
        assert_eq!(tree.search("90").unwrap(), "value");
        assert_eq!(tree.search("150").unwrap(), "value");
        assert_eq!(tree.search("170").unwrap(), "value");
        assert_eq!(tree.search("200").unwrap(), "value");
    }

    #[test]
    fn insert_split2() {
        let mut tree = mempage_tree();

        for i in 1u16..=400u16 {
            tree.insert(&format!("{i}"), "value").unwrap()
        }
        assert_eq!(
            (tree.decode)(&tree.root_ptr.unwrap()).get_type(),
            NodeType::Node
        );
        assert_eq!(tree.search("50").unwrap(), "value");
        assert_eq!(tree.search("90").unwrap(), "value");
        assert_eq!(tree.search("150").unwrap(), "value");
        assert_eq!(tree.search("170").unwrap(), "value");
        assert_eq!(tree.search("200").unwrap(), "value");
        assert_eq!(tree.search("300").unwrap(), "value");
        assert_eq!(tree.search("400").unwrap(), "value");
    }

    #[test]
    fn merge_delete1() {
        let mut tree = mempage_tree();

        for i in 1u16..=200u16 {
            tree.insert(&format!("{i}"), "value").unwrap()
        }
        for i in 1u16..=200u16 {
            tree.delete(&format!("{i}")).unwrap()
        }
        assert_eq!(
            (tree.decode)(&tree.root_ptr.unwrap()).get_type(),
            NodeType::Leaf
        );
        assert_eq!((tree.decode)(&tree.root_ptr.unwrap()).get_nkeys(), 1)
    }

    #[test]
    fn merge_delete_left_right() {
        let mut tree = mempage_tree();

        for i in 1u16..=400u16 {
            tree.insert(&format!("{i}"), "value").unwrap()
        }
        for i in 1u16..=400u16 {
            tree.delete(&format!("{i}")).unwrap()
        }
        assert_eq!(
            (tree.decode)(&tree.root_ptr.unwrap()).get_type(),
            NodeType::Leaf
        );
        assert_eq!((tree.decode)(&tree.root_ptr.unwrap()).get_nkeys(), 1)
    }

    #[test]
    fn merge_delete_right_left() {
        let mut tree = mempage_tree();

        for i in 1u16..=400u16 {
            tree.insert(&format!("{i}"), "value").unwrap()
        }
        for i in (1..=400u16).rev() {
            tree.delete(&format!("{i}")).unwrap()
        }
        assert_eq!(
            (tree.decode)(&tree.root_ptr.unwrap()).get_type(),
            NodeType::Leaf
        );
        assert_eq!((tree.decode)(&tree.root_ptr.unwrap()).get_nkeys(), 1);
    }

    #[test]
    fn merge_delete3() {
        let mut tree = mempage_tree();

        for i in 1u16..=400u16 {
            tree.insert(&format!("{i}"), "value").unwrap()
        }
        for i in (1..=400u16).rev() {
            tree.delete(&format!("{i}")).unwrap()
        }
        tree.delete("").unwrap();
        assert_eq!(tree.root_ptr, None);
    }

    #[test]
    fn insert_big() {
        let mut tree = mempage_tree();

        for i in 1u16..=1000u16 {
            tree.insert(&format!("{i}"), "value").unwrap()
        }
        assert_eq!(
            (tree.decode)(&tree.root_ptr.unwrap()).get_type(),
            NodeType::Node
        );
        for i in 1u16..=1000u16 {
            assert_eq!(tree.search(&format!("{i}")).unwrap(), "value")
        }
    }

    #[test]
    fn random_1k() {
        let mut tree = mempage_tree();

        for _ in 1u16..=1000 {
            tree.insert(&format!("{:?}", rand::rng().random_range(1..1000)), "val")
                .unwrap()
        }
    }
}
