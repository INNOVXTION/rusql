use crate::pager::node::*;
use crate::helper::*;
use crate::errors::Error;

struct BTree {
    root_ptr: Option<u64>,
    height: u16,
}

impl BTree {
    // loads page into memoory as a node
    fn get_node(&self, ptr: u64) -> Node {
        todo!()
    }
    // write node to disk
    fn encode_node(node: Node) -> u64 {
        todo!()
    }
    // deallocate page
    fn del_node(ptr: u64) {
        todo!()
    }
    fn tree_insert(&mut self, node: Node, key: &str, val: &str) -> Node {
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
                let knode = self.tree_insert(self.get_node(kptr), key, val); // node below us
                let split = knode.split().unwrap(); // potential split
                // delete old child
                BTree::del_node(kptr);
                // update child ptr
                new_node.replace_ptr(node, idx, split);
            }
        }
        new_node
    }
}
 