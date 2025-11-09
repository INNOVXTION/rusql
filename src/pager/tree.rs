use tracing::instrument;

use crate::errors::Error;
use crate::pager::node::*;

const BTREE_MAX_KEY_SIZE: usize = 1000;
const BTREE_MAX_VAL_SIZE: usize = 3000;
const MERGE_FACTOR: usize = PAGE_SIZE / 4; // determines when nodes should be merged, higher number = less merges

pub struct BTree {
    root_ptr: Option<u64>,
    height: u16,
}

#[allow(dead_code)]
impl BTree {
    pub fn new() -> Self {
        todo!()
    }

    pub fn insert(&mut self, key: &str, val: &str) -> Result<(), Error> {
        // check size limit
        if key.len() > BTREE_MAX_KEY_SIZE || val.len() > BTREE_MAX_VAL_SIZE {
            return Err(Error::InsertError("invalid key value size".to_string()));
        }
        // get root node
        let root = match self.root_ptr {
            Some(n) => node_get(n),
            None => {
                let mut new_root = Node::new();
                new_root.set_header(NodeType::Leaf, 2);
                new_root.append_kvptr(0, 0, "", "")?; // empty key to remove edge case
                new_root.append_kvptr(1, 0, key, val)?;
                return Ok(());
            }
        };
        // insert kv
        let updated_root = BTree::tree_insert(root, key, val);
        let mut split = updated_root.split()?;
        if split.0 == 1 {
            // no split, update root
            return Ok(node_encode_at(split.1.remove(0), self.root_ptr.unwrap())?);
        }
        // deleting old root and creating a new one
        node_dealloc(self.root_ptr.unwrap());
        let mut new_root = Node::new();
        new_root.set_header(NodeType::Node, split.0);
        // iterate through node array from split to create new root node
        for (i, node) in split.1.into_iter().enumerate() {
            let key = {
                let key_ref = node.get_key(0)?;
                str::from_utf8(key_ref)?
            }
            .to_string();
            new_root.append_kvptr(i as u16, node_encode(node), &key, "")?;
        }
        // encoding new root and updating tree ptr
        self.root_ptr = Some(node_encode(new_root));
        Ok(())
    }

    /// recursive insertion, node = current node, returns updated node
    fn tree_insert(node: Node, key: &str, val: &str) -> Node {
        let mut new_node = Node::new();
        let idx = node.lookupidx(key);
        match node.get_type().unwrap() {
            NodeType::Leaf => {
                // updating or inserting kv
                if str::from_utf8(node.get_key(idx).unwrap()).unwrap() == key {
                    new_node.leaf_kvupdate(node, idx, key, val).unwrap();
                } else {
                    new_node.leaf_kvinsert(node, idx + 1, key, val).unwrap();
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
                new_node.insert_nkids(node, idx, split).unwrap();
            }
        }
        // TODO: encode to disk
        new_node
    }

    pub fn delete(&mut self, key: &str) -> Result<(), Error> {
        todo!()
    }

    /// recursive deletion, node = current node, returns updated node in case a deletion happened
    pub fn tree_delete(node: Node, key: &str) -> Option<Node> {
        // del(key = 5)
        let idx = node.lookupidx(key); // 1
        match node.get_type().unwrap() {
            NodeType::Leaf => {
                if let Some(i) = node.searchidx(key) {
                    let mut new = Node::new();
                    new.leaf_deletekv(&node, i).unwrap();
                    new.set_header(node.get_type().unwrap(), node.get_nkeys() - 1);
                    return Some(new);
                }
                return None;
            }
            NodeType::Node => {
                let kptr = node.get_ptr(idx).unwrap(); // child 2 ptr
                // in case leaf node was updated below us
                // updated chlild 2 comes back
                if let Some(updated_child) = BTree::tree_delete(node_get(kptr), key) {
                    let mut new = Node::new();
                    match merge_check(&node, &updated_child, idx) {
                        // we need to merge
                        Some(dir) => {
                            let left: u64; // pointer to siblings
                            let right: u64;
                            let merge_index: u16; // idx of node we merge with
                            let mut merged_node = Node::new();
                            let merge_type = updated_child.get_type().unwrap();
                            match dir {
                                MergeDirection::left(sibling) => {
                                    right = kptr;
                                    left = node
                                        .get_ptr(idx - 1)
                                        .expect("idx error when merging with left");
                                    merge_index = idx - 1;
                                    merged_node
                                        .merge(sibling, updated_child, merge_type)
                                        .unwrap();
                                }
                                MergeDirection::right(sibling) => {
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
                            new.set_header(node.get_type().unwrap(), node.get_nkeys() - 1);
                            // set ptr of new merged node
                            new.set_ptr(merge_index, node_encode(merged_node)).unwrap();
                            // update node and delete key of node we merged away
                            new.leaf_deletekv(&node, idx).unwrap();
                            // return updated internal node
                            return Some(new);
                        }
                        // no merge
                        None => {
                            new.set_header(node.get_type().unwrap(), node.get_nkeys());
                            new.set_ptr(idx, node_encode(updated_child)).unwrap();
                            return Some(new);
                        }
                    }
                }
                return None;
            }
        }
    }
}

/// loads page into memoory as a node
pub fn node_get(ptr: u64) -> Node {
    todo!()
}

/// finds a free spot to write node to
pub fn node_encode(node: Node) -> u64 {
    todo!()
}

/// write node to disk at page
pub fn node_encode_at(node: Node, ptr: u64) -> Result<(), Error> {
    todo!()
}

/// deallocate page
pub fn node_dealloc(ptr: u64) {
    todo!()
}

/// which sibling we need to merge with
pub enum MergeDirection {
    left(Node),
    right(Node),
}

/// checks if new node needs merging
///
/// if it needs to be merged, returns sibling nodes which should be merged
fn merge_check(cur: &Node, new: &Node, idx: u16) -> Option<MergeDirection> {
    if new.nbytes() > MERGE_FACTOR as u16 {
        return None; // no merge necessary
    }
    let new_size = new.nbytes();
    // check left
    if idx > 0 {
        let sibling = node_get(cur.get_ptr(idx - 1).unwrap());
        let sibling_size = sibling.nbytes();
        if sibling_size + new_size < PAGE_SIZE as u16 {
            return Some(MergeDirection::left(sibling));
        }
    }
    // check right
    if idx < cur.get_nkeys() - 1 {
        let sibling = node_get(cur.get_ptr(idx + 1).unwrap());
        let sibling_size = sibling.nbytes();
        if sibling_size + new_size < PAGE_SIZE as u16 {
            return Some(MergeDirection::right(sibling));
        }
    }
    None
}
