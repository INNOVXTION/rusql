/*
 * in memory pager used for testing BTree implementations
 */
use std::{cell::RefCell, collections::HashMap, rc::Rc};

use tracing::{debug, error};

use crate::database::{
    btree::{BTree, Tree},
    pager::diskpager::Pager,
    types::{Node, Pointer},
};

pub struct MemoryPager {
    freelist: RefCell<Vec<u64>>,
    pages: RefCell<HashMap<u64, Node>>,
    btree: Box<dyn Tree<Codec = Self>>,
}

#[allow(unused)]
pub fn mempage_tree<P: Pager>() -> Rc<MemoryPager> {
    Rc::new_cyclic(|w| MemoryPager {
        freelist: RefCell::new(vec![]),
        pages: RefCell::new(HashMap::<u64, Node>::new()),
        btree: Box::new(BTree::<MemoryPager>::new(w.clone())),
    })
}

impl Pager for MemoryPager {
    fn page_read(&self, ptr: Pointer, flag: super::diskpager::NodeFlag) -> Node {
        self.pages
            .borrow_mut()
            .get(&ptr.0)
            .unwrap_or_else(|| {
                error!("couldnt retrieve page at ptr {}", ptr);
                panic!("page decode error")
            })
            .clone()
    }

    fn page_alloc(&self, node: Node) -> Pointer {
        if !node.fits_page() {
            panic!("trying to encode node exceeding page size");
        }
        let free_page = self
            .freelist
            .borrow_mut()
            .pop()
            .expect("no free page available");
        debug!("encoding node at ptr {}", free_page);
        self.pages.borrow_mut().insert(free_page, node);
        Pointer(free_page)
    }

    fn dealloc(&self, ptr: Pointer) {
        debug!("deleting node at ptr {}", ptr.0);
        self.freelist.borrow_mut().push(ptr.0);
        self.pages
            .borrow_mut()
            .remove(&ptr.0)
            .expect("couldnt remove() page number");
    }

    fn encode(&self, node: Node) -> Pointer {
        unreachable!()
        // not needed for in memory pager
    }

    fn update(&self, ptr: Pointer) -> *mut super::freelist::FLNode {
        unreachable!()
        // not needed for in memory pager
    }
}
