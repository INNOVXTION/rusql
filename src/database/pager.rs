use std::cell::RefCell;
use std::collections::HashMap;

use crate::database::node::Node;

thread_local! {
    pub static FREELIST: RefCell<Vec<u64>> = RefCell::new(vec![1, 2, 3, 4, 5]);
    pub static PAGER: RefCell<HashMap<u64, Node>> = RefCell::new(HashMap::new());
}

/// loads page into memoory as a node
pub(crate) fn node_get(ptr: u64) -> Node {
    PAGER.with_borrow_mut(|x| x.get(&ptr).unwrap().clone())
}

/// finds a free spot to write node to
pub(crate) fn node_encode(node: Node) -> u64 {
    let free_page = FREELIST.with_borrow_mut(|v| v.pop().expect("no free page available"));
    PAGER.with_borrow_mut(|p| p.insert(free_page, node));
    free_page
}

/// deallocate page
pub(crate) fn node_dealloc(ptr: u64) {
    FREELIST.with_borrow_mut(|v| v.push((v.len() + 1) as u64));
    PAGER.with_borrow_mut(|x| x.remove(&ptr)).unwrap();
}
