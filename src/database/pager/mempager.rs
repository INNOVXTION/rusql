use std::{collections::HashMap, sync::OnceLock};

use parking_lot::Mutex;
use tracing::{debug, error};

use crate::database::{
    node::Node,
    tree::BTree,
    types::{PAGE_SIZE, Pointer},
};

pub static GLOBAL_PAGER: OnceLock<Mutex<MemoryPager>> = OnceLock::new();

#[derive(Debug)]
pub struct MemoryPager {
    freelist: Vec<u64>,
    pages: HashMap<u64, Node>,
}

impl MemoryPager {
    pub fn new() -> Self {
        MemoryPager {
            freelist: Vec::from_iter((1..=100).rev()),
            pages: HashMap::new(),
        }
    }
}

#[allow(unused)]
pub fn mempage_tree() -> BTree {
    GLOBAL_PAGER.set(Mutex::new(MemoryPager::new()));
    BTree {
        root_ptr: None,
        decode: Box::new(decode),
        encode: Box::new(encode),
        dealloc: Box::new(dealloc),
    }
}

/// callbacks for memory pager
fn decode(ptr: &Pointer) -> Node {
    let pager = GLOBAL_PAGER.get().unwrap().lock();
    pager
        .pages
        .get(&ptr.0)
        .unwrap_or_else(|| {
            error!("couldnt retrieve page at ptr {}", ptr);
            panic!("page decode error")
        })
        .clone()
}

fn encode(node: Node) -> Pointer {
    if node.get_nkeys() > PAGE_SIZE as u16 {
        panic!("trying to encode node exceeding page size");
    }
    let mut pager = GLOBAL_PAGER.get().unwrap().lock();
    let free_page = pager.freelist.pop().expect("no free page available");
    debug!("encoding node at ptr {}", free_page);
    pager.pages.insert(free_page, node);
    Pointer(free_page)
}

fn dealloc(ptr: Pointer) {
    debug!("deleting node at ptr {}", ptr.0);
    let mut pager = GLOBAL_PAGER.get().unwrap().lock();
    pager.freelist.push(ptr.0);
    pager
        .pages
        .remove(&ptr.0)
        .expect("couldnt remove() page number");
}
