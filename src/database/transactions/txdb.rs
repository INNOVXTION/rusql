use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::Ordering;
use std::{cell::RefCell, rc::Rc, sync::Arc};

use parking_lot::{Mutex, RwLock};
use tracing::debug;

use crate::database::pager::diskpager::PageOrigin;
use crate::database::{
    pager::{NodeFlag, Pager},
    transactions::{kvdb::KVDB, tx::TXKind},
    types::{Node, Pointer},
};

// per transaction resource struct
pub struct TXDB {
    pub db_link: Arc<KVDB>,                // shared resource
    pub tx_buf: Option<RefCell<TXBuffer>>, // isolated resource
    pub version: u64,
}

pub struct TXBuffer {
    pub write_map: HashMap<Pointer, TXBufWriteEntry>,
    pub dealloc_map: HashSet<Pointer>,
    pub nappend: u32,
}

pub struct TXBufWriteEntry {
    pub node: Arc<Node>,
    pub origin: PageOrigin,
}

impl TXDB {
    pub fn new(db: &Arc<KVDB>, kind: TXKind) -> Self {
        match kind {
            TXKind::Read => Self {
                db_link: db.clone(),
                tx_buf: None,
                version: db.pager.version.load(Ordering::Acquire),
            },
            TXKind::Write => Self {
                db_link: db.clone(),
                tx_buf: Some(RefCell::new(TXBuffer {
                    write_map: HashMap::new(),
                    dealloc_map: HashSet::new(),
                    nappend: 0,
                })),
                version: db.pager.version.load(Ordering::Acquire),
            },
        }
    }
    fn debug_print(&self) {
        #[cfg(test)]
        {
            if let Ok("debug") = std::env::var("RUSQL_LOG_TX").as_deref() {
                debug!("{:-<10}", "-");
                debug!(
                    len = self.tx_buf.as_ref().unwrap().borrow().write_map.len(),
                    nappend = self.tx_buf.as_ref().unwrap().borrow().nappend,
                    "current TX buffer:"
                );
                for e in self.tx_buf.as_ref().unwrap().borrow().write_map.iter() {
                    debug!(
                        "- {:<10}, {:<10}, {:?}",
                        e.0,
                        e.1.node.get_type(),
                        e.1.origin
                    )
                }
                debug!("");
                debug!("dealloc queue:");
                for e in self.tx_buf.as_ref().unwrap().borrow().dealloc_map.iter() {
                    debug!("- {e}")
                }
                debug!("{:-<10}", "-");
            }
        }
    }

    fn is_synced(&self) -> bool {
        let buf = self.tx_buf.as_ref().unwrap().borrow();

        for e in buf.write_map.iter() {
            if buf.dealloc_map.contains(e.0) {
                return false;
            }
        }
        true
    }
}

impl Pager for TXDB {
    fn page_read(&self, ptr: Pointer, flag: NodeFlag) -> Arc<Node> {
        assert_ne!(ptr.get(), 0);
        // read own buffer first
        if let Some(b) = self.tx_buf.as_ref()
            && let Some(n) = b.borrow_mut().write_map.get(&ptr)
        {
            debug!("page found in TX buffer!");
            return n.node.clone();
        }
        self.db_link.pager.read(ptr, flag, self.version)
    }

    fn page_alloc(&self, node: Node, version: u64) -> Pointer {
        let mut buf = self.tx_buf.as_ref().unwrap().borrow_mut();
        debug!(nappend = buf.nappend, "allocating new page");

        // request pointer from pager
        let page = self.db_link.pager.alloc(&node, version, buf.nappend);

        // store node in TX buffer
        let r = buf.write_map.insert(
            page.ptr,
            TXBufWriteEntry {
                node: Arc::new(node),
                origin: page.origin,
            },
        );
        if r.is_none() && PageOrigin::Append == page.origin {
            // if the page didnt exist and the new page came from an append
            buf.nappend += 1;
        }

        // sync with deallocations
        buf.dealloc_map.remove(&page.ptr);
        drop(buf);

        #[cfg(test)]
        {
            if let Ok("debug") = std::env::var("RUSQL_LOG_TX").as_deref() {
                self.debug_print();
            }
        };

        debug_assert!(self.is_synced());
        debug!("handing out: {}", page.ptr);

        assert_ne!(page.ptr.get(), 0, "never receive the mp for writes");
        page.ptr
    }

    fn dealloc(&self, ptr: Pointer) {
        assert_ne!(ptr.get(), 0, "never mark the mp for deallocation");
        debug!(%ptr, "adding to dealloc q:");
        let mut buf = self.tx_buf.as_ref().unwrap().borrow_mut();

        // push to dealloc list
        buf.dealloc_map.insert(ptr);

        // sync write buffer
        if let Some(entry) = buf.write_map.remove(&ptr)
            && entry.origin == PageOrigin::Append
        {
            // buf.nappend -= 1;
        };

        drop(buf);
        #[cfg(test)]
        {
            if let Ok("debug") = std::env::var("RUSQL_LOG_TX").as_deref() {
                self.debug_print();
            }
        };
        debug_assert!(self.is_synced());
    }
}
