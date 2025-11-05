use crate::errors::Error;
use crate::pager::node::*;

const BTREE_MAX_KEY_SIZE: usize = 1000;
const BTREE_MAX_VAL_SIZE: usize = 3000;

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

    pub fn delete(key: &str) -> Result<(), Error> {
        todo!()
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
                new_node.insert_ptr(node, idx, split).unwrap();
            }
        }
        // TODO: encode to disk
        new_node
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
