use std::str::FromStr;

use tracing::debug;
use tracing::instrument;

use crate::database::node::*;
use crate::database::pager::*;
use crate::database::types::*;
use crate::errors::Error;

// determines when nodes should be merged, higher number = less merges

pub struct BTree {
    root_ptr: Option<u64>,
    height: u16,
}

#[allow(dead_code)]
impl BTree {
    pub fn new() -> Self {
        BTree {
            root_ptr: None,
            height: 0,
        }
    }

    #[instrument(skip(self))]
    pub fn insert(&mut self, key: &str, val: &str) -> Result<(), Error> {
        // check size limit
        if key.len() > BTREE_MAX_KEY_SIZE || val.len() > BTREE_MAX_VAL_SIZE {
            return Err(Error::InsertError("invalid key value size".to_string()));
        }
        // get root node
        let root = match self.root_ptr {
            Some(n) => node_get(n),
            None => {
                debug!("no root found, creating new root");
                let mut new_root = Node::new();
                new_root.set_header(NodeType::Leaf, 2);
                new_root.kvptr_append(0, 0, "", "")?; // empty key to remove edge case
                new_root.kvptr_append(1, 0, key, val)?;
                self.height += 1;
                self.root_ptr = Some(node_encode(new_root));
                return Ok(());
            }
        };
        // insert kv
        let updated_root = BTree::tree_insert(root, key, val);
        let mut split = updated_root.split()?;
        // deleting old root and creating a new one
        node_dealloc(self.root_ptr.unwrap());
        if split.0 == 1 {
            // no split, update root
            self.root_ptr = Some(node_encode(split.1.remove(0)));
            debug!("inserted without root split");
            return Ok(());
        }
        // in case of split tree grows in height
        let mut new_root = Node::new();
        new_root.set_header(NodeType::Node, split.0);
        // iterate through node array from split to create new root node
        for (i, node) in split.1.into_iter().enumerate() {
            let key = {
                let key_ref = node.get_key(0)?;
                str::from_utf8(key_ref)?
            }
            .to_string();
            new_root.kvptr_append(i as u16, node_encode(node), &key, "")?;
        }
        // encoding new root and updating tree ptr
        self.root_ptr = Some(node_encode(new_root));
        self.height += 1;
        debug!("inserted with root split");
        Ok(())
    }

    /// recursive insertion, node = current node, returns updated node
    ///
    /// TODO: update height
    fn tree_insert(node: Node, key: &str, val: &str) -> Node {
        let mut new = Node::new();
        let idx = node.lookupidx(key);
        debug!("inserting value: {key}, {val}, idx: {idx}");
        match node.get_type().unwrap() {
            NodeType::Leaf => {
                // updating or inserting kv
                if str::from_utf8(node.get_key(idx).unwrap()).unwrap() == key {
                    new.kv_update(node, idx, key, val).unwrap();
                } else {
                    new.kv_insert(node, idx + 1, key, val).unwrap();
                }
            }
            // walking down the tree until we hit a leaf node
            NodeType::Node => {
                let kptr = node.get_ptr(idx).unwrap(); // ptr of child below us
                let knode = BTree::tree_insert(node_get(kptr), key, val); // node below us
                let split = knode.split().unwrap(); // potential split
                // delete old child
                node_dealloc(kptr);
                // update child ptr
                new.insert_nkids(node, idx, split).unwrap();
            }
        }
        new
    }

    pub fn delete(&mut self, key: &str) -> Result<(), Error> {
        let root_ptr = match self.root_ptr {
            Some(n) => n,
            None => {
                return Err(Error::DeleteError(
                    "cant delete from empty tree!".to_string(),
                ));
            }
        };
        if let Some(updated) = BTree::tree_delete(node_get(root_ptr), key) {
            node_dealloc(root_ptr);
            self.root_ptr = Some(node_encode(updated));
            return Ok(());
        }
        Err(Error::DeleteError("key not found!".to_string()))
    }

    /// recursive deletion, node = current node, returns updated node in case a deletion happened
    ///
    /// TODO: update height
    fn tree_delete(mut node: Node, key: &str) -> Option<Node> {
        // del(key = 5)
        let idx = node.lookupidx(key); // 1
        match node.get_type().unwrap() {
            NodeType::Leaf => {
                if let Some(i) = node.searchidx(key) {
                    let mut new = Node::new();
                    new.kv_delete(&node, i).unwrap();
                    new.set_header(NodeType::Leaf, node.get_nkeys() - 1);
                    return Some(new);
                }
                return None; // key not found
            }
            NodeType::Node => {
                let kptr = node.get_ptr(idx).unwrap(); // child 2 ptr
                // in case leaf node was updated below us
                // updated chlild 2 comes back
                match BTree::tree_delete(node_get(kptr), key) {
                    None => return None, // no update below use
                    Some(updated_child) => {
                        let mut new = Node::new();
                        let cur_nkeys = node.get_nkeys();
                        match merge_check(&node, &updated_child, idx) {
                            // we need to merge
                            Some(dir) => {
                                debug!("merging node...");
                                let left: u64;
                                let right: u64;
                                let merge_index: u16; // idx of node we merge with
                                let mut merged_node = Node::new();
                                let merge_type = updated_child.get_type().unwrap();
                                match dir {
                                    MergeDirection::Left(sibling) => {
                                        right = kptr;
                                        left = node
                                            .get_ptr(idx - 1)
                                            .expect("idx error when merging with left");
                                        merge_index = idx - 1;
                                        merged_node
                                            .merge(sibling, updated_child, merge_type)
                                            .unwrap();
                                    }
                                    MergeDirection::Right(sibling) => {
                                        left = kptr;
                                        right = node
                                            .get_ptr(idx + 1)
                                            .expect("idx error when merging with right");
                                        merge_index = idx + 1;
                                        merged_node
                                            .merge(updated_child, sibling, merge_type)
                                            .unwrap();
                                    }
                                };
                                // delete old nodes
                                node_dealloc(left);
                                node_dealloc(right);
                                // set ptr of new merged node
                                node.set_ptr(merge_index, node_encode(merged_node)).unwrap();
                                // update node and delete key of node we merged away
                                new.kv_delete(&node, idx).unwrap();
                                // return updated internal node
                                return Some(new);
                            }
                            // no merge necessary, or no sibling to merge with
                            None => {
                                node_dealloc(kptr);
                                // empty child without siblings
                                if updated_child.get_nkeys() == 0 && cur_nkeys == 1 {
                                    assert!(idx == 0);
                                    new.set_header(NodeType::Node, 0);
                                    return Some(new);
                                }
                                // no merge, update new child
                                node.set_ptr(idx, node_encode(updated_child)).unwrap();
                                // new.set_header(NodeType::Node, cur_nkeys);
                                // new.insert_nkids(node, idx, (0, vec![updated_child]))
                                //     .unwrap();
                                return Some(node);
                            }
                        }
                    }
                }
            }
        }
    }

    pub fn search(&self, key: &str) -> Option<String> {
        let root_ptr = match self.root_ptr {
            Some(n) => n,
            None => return None,
        };
        BTree::tree_search(node_get(root_ptr), key)
    }

    fn tree_search(node: Node, key: &str) -> Option<String> {
        let idx = node.lookupidx(key);
        match node.get_type().unwrap() {
            NodeType::Leaf => {
                if let Some(i) = node.searchidx(key) {
                    debug!("key {key} found!");
                    return Some(
                        String::from_str(str::from_utf8(node.get_val(i).unwrap()).unwrap())
                            .unwrap(),
                    );
                }
                debug!("key {key} not found!");
                None
            }
            NodeType::Node => {
                let kptr = node.get_ptr(idx).unwrap();
                BTree::tree_search(node_get(kptr), key)
            }
        }
    }
}

/// which sibling we need to merge with
enum MergeDirection {
    Left(Node),
    Right(Node),
}

/// checks if new node needs merging
///
/// returns sibling node to merge with
fn merge_check(cur: &Node, new: &Node, idx: u16) -> Option<MergeDirection> {
    if new.nbytes() > MERGE_FACTOR as u16 {
        return None; // no merge necessary
    }
    let new_size = new.nbytes() - crate::database::node::HEADER_OFFSET as u16;
    // check left
    if idx > 0 {
        debug!("merging with left node");
        let sibling = node_get(cur.get_ptr(idx - 1).unwrap());
        let sibling_size = sibling.nbytes();
        if sibling_size + new_size < PAGE_SIZE as u16 {
            return Some(MergeDirection::Left(sibling));
        }
    }
    // check right
    if idx + 1 < cur.get_nkeys() {
        debug!("merging with right node");
        let sibling = node_get(cur.get_ptr(idx + 1).unwrap());
        let sibling_size = sibling.nbytes();
        if sibling_size + new_size < PAGE_SIZE as u16 {
            return Some(MergeDirection::Right(sibling));
        }
    }
    debug!("no merge possible");
    None
}

#[cfg(test)]
mod test {
    use super::*;
    use test_log::test;
    use tracing::info;

    #[test]
    fn tree_insert() {
        info!("help i just want to log :(");
        let mut tree = BTree::new();
        tree.insert("1", "hello").unwrap();
        tree.insert("2", "world").unwrap();

        assert_eq!(tree.search("1").unwrap(), "hello");
        assert_eq!(tree.search("2").unwrap(), "world");
        assert!(tree.search("3").is_none());
    }

    #[test]
    fn should_fail() {
        info!("help i just want to log :(");
        let mut tree = BTree::new();
        tree.insert("1", "hello").unwrap();
        assert_eq!(tree.search("2").unwrap(), "hello");
    }

    // // test delete
    // #[test]
    // fn delete() {}
}
