/*
 * in memory pager used for testing BTree implementations, does not support a freelist currently
 */
use std::{cell::RefCell, collections::HashMap, sync::Arc};

use tracing::{debug, error};

use crate::database::{
    btree::{BTree, ScanMode, SetFlag, SetResponse, Tree},
    errors::{Error, Result},
    pager::{Pager, diskpager::GCCallbacks},
    tables::{Key, Record, Value},
    types::{Node, Pointer},
};

/// outward facing api
///
/// deprecated, for mempager testing only
pub(crate) trait KVEngine {
    fn get(&self, key: Key) -> Result<Value>;
    fn scan(&self, mode: ScanMode) -> Result<Vec<Record>>;
    fn set(&self, key: Key, val: Value, flag: SetFlag) -> Result<()>;
    fn delete(&self, key: Key) -> Result<()>;
}

// wrapper
pub(crate) struct MemPager {
    pub(crate) pager: Arc<MemoryPager>,
}

impl KVEngine for MemPager {
    fn get(&self, key: Key) -> Result<Value> {
        self.pager.get(key)
    }

    fn set(&self, key: Key, val: Value, flag: SetFlag) -> Result<()> {
        let _ = self.pager.set(key, val, flag)?;
        Ok(())
    }

    fn delete(&self, key: Key) -> Result<()> {
        self.pager.delete(key)
    }

    fn scan(&self, mode: crate::database::btree::ScanMode) -> Result<Vec<Record>> {
        Ok(self
            .pager
            .tree
            .borrow()
            .scan(mode)
            .map_err(|e| Error::SearchError(format!("scan error {e}")))?
            .collect_records())
    }
}

pub struct MemoryPager {
    freelist: RefCell<Vec<u64>>,
    pages: RefCell<HashMap<Pointer, Node>>,
    pub tree: Box<RefCell<BTree<MemoryPager>>>,
}

/// constructor for in memory pager and tree
#[allow(unused)]
pub fn mempage_tree() -> MemPager {
    MemPager {
        pager: Arc::new_cyclic(|w| MemoryPager {
            freelist: RefCell::new(Vec::from_iter((1..=100).rev())),
            pages: RefCell::new(HashMap::<Pointer, Node>::new()),
            tree: Box::new(RefCell::new(BTree::<MemoryPager>::new(w.clone()))),
        }),
    }
}

impl MemoryPager {
    fn get(&self, key: Key) -> Result<Value> {
        self.tree
            .borrow()
            .get(key)
            .ok_or(Error::SearchError("value not found".to_string()))
    }

    fn set(&self, key: Key, value: Value, flag: SetFlag) -> Result<SetResponse> {
        self.tree.borrow_mut().set(key, value, flag)
    }

    fn delete(&self, key: Key) -> Result<()> {
        self.tree.borrow_mut().delete(key)
    }
}

impl Pager for MemoryPager {
    fn page_read(&self, ptr: Pointer, flag: super::diskpager::NodeFlag) -> Arc<Node> {
        Arc::new(
            self.pages
                .borrow_mut()
                .get(&ptr)
                .unwrap_or_else(|| {
                    error!("couldnt retrieve page at ptr {}", ptr);
                    panic!("page decode error")
                })
                .clone(),
        )
    }

    fn page_alloc(&self, node: Node, version: u64) -> Pointer {
        if !node.fits_page() {
            panic!("trying to encode node exceeding page size");
        }
        let free_page = self
            .freelist
            .borrow_mut()
            .pop()
            .expect("no free page available");
        debug!("encoding node at ptr {}", free_page);
        self.pages.borrow_mut().insert(free_page.into(), node);
        Pointer(free_page)
    }

    fn dealloc(&self, ptr: Pointer) {
        debug!("deleting node at ptr {}", ptr.0);
        self.freelist.borrow_mut().push(ptr.0);
        self.pages
            .borrow_mut()
            .remove(&ptr)
            .expect("couldnt remove() page number");
    }
}

// impl GCCallbacks for MemoryPager {
//     fn page_read(&self, ptr: Pointer, flag: super::diskpager::NodeFlag) -> Arc<Node> {
//         Arc::new(
//             self.pages
//                 .borrow_mut()
//                 .get(&ptr)
//                 .unwrap_or_else(|| {
//                     error!("couldnt retrieve page at ptr {}", ptr);
//                     panic!("page decode error")
//                 })
//                 .clone(),
//         )
//     }
//     // not needed for in memory pager
//     fn encode(&self, node: Node) -> Pointer {
//         if !node.fits_page() {
//             panic!("trying to encode node exceeding page size");
//         }
//         let free_page = self
//             .freelist
//             .borrow_mut()
//             .pop()
//             .expect("no free page available");
//         debug!("encoding node at ptr {}", free_page);
//         self.pages.borrow_mut().insert(free_page.into(), node);
//         Pointer(free_page)
//     }

//     // not needed for in memory pager
//     fn update(&self, ptr: Pointer) -> Arc<RefCell<Node>> {
//         unreachable!()
//     }
// }
