use std::collections::{HashMap, VecDeque};
use std::sync::atomic::Ordering;
use std::{cell::RefCell, rc::Rc, sync::Arc};

use parking_lot::{Mutex, RwLock};
use tracing::debug;

use crate::database::pager::diskpager::PageOrigin;
use crate::database::{
    pager::{DiskBuffer, NodeFlag, Pager},
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
    pub write_map: HashMap<Pointer, Arc<Node>>,
    pub dealloc_q: VecDeque<Pointer>,
    pub nappend: u32,
}

#[derive(Debug)]
pub enum TXWrite {
    Write(Arc<Node>),
    Retire,
}

impl std::fmt::Display for TXWrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TXWrite::Write(ref_cell) => write!(f, "write node"),
            TXWrite::Retire => write!(f, "dealloc node"),
        }
    }
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
                    dealloc_q: VecDeque::new(),
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
                    debug!("{:<10}, {:<10}", e.0, e.1.get_type())
                }
                debug!("dealloc queue:");
                for e in self.tx_buf.as_ref().unwrap().borrow().dealloc_q.iter() {
                    debug!("{e}")
                }
                debug!("{:-<10}", "-");
            }
        }
    }
}

impl Pager for TXDB {
    fn page_read(&self, ptr: Pointer, flag: NodeFlag) -> Arc<Node> {
        // read own buffer first
        if let Some(b) = self.tx_buf.as_ref()
            && let Some(n) = b.borrow_mut().write_map.remove(&ptr)
        {
            debug!("page found in TX buffer!");
            return n;
        }
        self.db_link.pager.read(ptr, flag, self.version)
    }

    fn page_alloc(&self, node: Node, version: u64) -> Pointer {
        let mut buf = self.tx_buf.as_ref().unwrap().borrow_mut();

        // request pointer from pager
        let page = self.db_link.pager.alloc(&node, version, buf.nappend);

        // store node in TX buffer
        if let None = buf.write_map.insert(page.ptr, Arc::new(node))
            && let PageOrigin::Append = page.origin
        {
            // if the page didnt exist and the new page came from an append
            buf.nappend += 1;
        }

        #[cfg(test)]
        {
            debug!(%page.ptr, "appending from disk");
            if let Ok("debug") = std::env::var("RUSQL_LOG_TX").as_deref() {
                drop(buf);
                self.debug_print();
            }
        };

        page.ptr
    }

    fn dealloc(&self, ptr: Pointer) {
        debug!(%ptr, "adding to dealloc q:");
        let mut buf = self.tx_buf.as_ref().unwrap().borrow_mut();
        // marks page inside TX buffer as retired
        buf.dealloc_q.push_back(ptr);
    }
}
