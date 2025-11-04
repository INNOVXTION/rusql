use crate::pager::node::*;
use crate::helper::*;
use crate::errors::Error;

struct BTree {
    root_ptr: Option<u64>,
    height: u16,
}

impl BTree {
    fn get_node(&self, ptr: u64) -> Node {
        todo!()
    }
    fn tree_insert(&mut self, node: Node, key: &str, val: &str) -> Node {
        let mut new_node = Node::new();
        let idx = node.lookupidx(key);
        match node.get_type().unwrap() {
            NodeType::Leaf => {
                if str::from_utf8(node.get_key(idx).unwrap()).unwrap() == key {
                    // found! updating
                    new_node.leaf_kvupdate(node, idx, key, val).unwrap();
                } else {
                    new_node.leaf_kvinsert(node, idx + 1, key, val).unwrap();
                }
            }
            NodeType::Node => {
                let kptr = node.get_ptr(idx).unwrap();
                let knode = self.tree_insert(self.get_node(kptr), key, val);
                let split = knode.split().unwrap();
            }
        }
        new_node
    }
}
 