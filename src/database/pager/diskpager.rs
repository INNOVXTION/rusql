use std::cell::{Cell, Ref, RefCell};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::os::fd::OwnedFd;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use parking_lot::{Mutex, RawRwLock, RwLock, RwLockWriteGuard};
use parking_lot::{MutexGuard, ReentrantMutex};
use rustix::fs::{fstat, fsync, ftruncate};
use tracing::{debug, error, info, instrument, warn};

use crate::create_file_sync;
use crate::database::BTree;
use crate::database::errors::FLError;
use crate::database::helper::as_page;
use crate::database::pager::buffer::{DiskBuffer, OngoingTX, SharedBuffer};
use crate::database::pager::freelist::{FLConfig, FLNode, FreeList, GC};
use crate::database::pager::lru::{LRU, debug_print};
use crate::database::pager::metapage::*;
use crate::database::pager::mmap::*;
use crate::database::pager::transaction::TXHistory;
use crate::database::tables::{Key, Record, Value};
use crate::database::transactions::tx::TX;
use crate::database::{
    btree::{SetFlag, Tree, TreeNode},
    errors::{Error, PagerError},
    types::*,
};

/// indicates the encoding/decoding style of a node
#[derive(Debug)]
pub(crate) enum NodeFlag {
    Tree,
    Freelist,
}

#[derive(Debug, Clone, Copy)]
pub struct AllocatedPage {
    pub ptr: Pointer,
    pub version: u64,
    pub origin: PageOrigin,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PageOrigin {
    Append,
    Freelist,
}

impl std::fmt::Display for NodeFlag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "node type: {:?}", self)
    }
}

pub(crate) struct DiskPager {
    path: &'static str,
    pub database: OwnedFd,
    pub mmap: RwLock<Mmap>,
    pub buf_shared: RwLock<SharedBuffer>, // shared read only buffer
    pub freelist: RwLock<FreeList>,
    pub buf_fl: RwLock<DiskBuffer>, // freelist exclusive read-write buffer

    pub npages: AtomicU64,
    pub failed: AtomicBool,
    pub tree: RwLock<Option<Pointer>>, // root pointer

    pub version: AtomicU64,
    pub ongoing: RwLock<OngoingTX>,
    pub history: RwLock<TXHistory>,

    pub lock: Mutex<()>, // used to block new tx being created and current tx being committed

                         // WIP
                         // clean factor
                         // counter after deletion for cleanup
}

/// internal callback API
pub(crate) trait Pager {
    // tree callbacks
    fn page_read(&self, ptr: Pointer, flag: NodeFlag) -> Arc<Node>; //tree decode
    fn page_alloc(&self, node: Node, version: u64) -> Pointer; //tree encode
    fn dealloc(&self, ptr: Pointer); // tree dealloc/del
}

pub(crate) trait GCCallbacks {
    // FL callbacks
    fn page_read(&self, ptr: Pointer, flag: NodeFlag) -> Arc<Node>; //tree decode
    fn encode(&self, node: Node) -> Pointer; // FL encode
    fn update(&self, ptr: Pointer) -> RwLockWriteGuard<'_, DiskBuffer>; // FL update
}

impl GCCallbacks for DiskPager {
    /// decodes a page, checks buffer before reading disk
    ///
    /// `BTree.get`, reads a page possibly from buffer or disk
    fn page_read(&self, ptr: Pointer, flag: NodeFlag) -> Arc<Node> {
        let buf_ref = self.buf_fl.read();

        // check buffer first
        debug!(node=?flag, %ptr, "page read");
        if let Some(n) = buf_ref.get(ptr) {
            debug!("page found in buffer!");
            Arc::new(n.clone())
        } else {
            debug!("reading from disk...");
            drop(buf_ref);
            let mut buf_ref = self.buf_fl.write();
            let n = self.decode(ptr, flag);

            buf_ref.insert_clean(ptr, n);
            Arc::new(buf_ref.get(ptr).expect("we just inserted it").clone())
        }
    }
    /// adds pages to buffer to be encoded to disk later (append)
    ///
    /// does not check if node exists in buffer!
    ///
    /// pageAppend
    fn encode(&self, node: Node) -> Pointer {
        let mut buf = self.buf_fl.write();
        let ptr = Pointer(self.npages.load(Ordering::Relaxed) + buf.nappend);

        // empty db has n_pages = 1 (meta page)
        assert!(node.fits_page());

        debug!(
            "encode: adding {:?} at page: {} to buffer",
            node.get_type(),
            ptr.0
        );

        buf.nappend += 1;
        buf.insert_dirty(ptr, node);
        buf.debug_print();

        assert_ne!(ptr.0, 0);
        ptr
    }

    /// callback for free list
    ///
    /// checks buffer for allocated page and returns pointer
    fn update(&self, ptr: Pointer) -> RwLockWriteGuard<'_, DiskBuffer> {
        self.buf_fl.write()

        // // checking buffer,...
        // let entry = match buf.get(ptr) {
        //     Some(n) => {
        //         debug!("updating {} in buffer", ptr);
        //         n
        //     }
        //     None => {
        //         // decoding page from disk and loading it into buffer
        //         debug!(%ptr, "reading free list from disk...");
        //         buf.insert_dirty(ptr, self.decode(ptr, NodeFlag::Freelist));
        //         buf.get(ptr).expect("we just inserted it")
        //     }
        // };
        // buf.debug_print();
        // entry
    }
}

struct FLUpdate<'a> {
    node: &'a mut Node,
    guard: MutexGuard<'a, DiskBuffer>,
}

impl DiskPager {
    /// initializes pager
    ///
    /// opens file, and sets up callbacks for the tree
    pub fn open(path: &'static str) -> Result<Arc<Self>, Error> {
        let mut pager = Arc::new_cyclic(|w| DiskPager {
            path,
            database: create_file_sync(path).expect("file open error"),
            failed: false.into(),
            buf_shared: RwLock::new(SharedBuffer::new()),
            mmap: RwLock::new(Mmap {
                total: 0,
                chunks: vec![],
            }),
            npages: 0.into(),
            tree: RwLock::new(None),
            freelist: RwLock::new(FreeList::new(w.clone())),
            buf_fl: RwLock::new(DiskBuffer::new()),
            version: 1.into(),
            ongoing: RwLock::new(OngoingTX {
                map: BTreeMap::new(),
            }),
            history: RwLock::new(TXHistory {
                history: HashMap::new(),
                cap: 0,
            }),
            lock: Mutex::new(()),
        });

        let fd_size = fstat(&pager.database)
            .map_err(|e| {
                error!("Error when getting file size");
                Error::PagerError(PagerError::FDError(e))
            })
            .unwrap()
            .st_size as u64;

        mmap_extend(&pager, PAGE_SIZE).expect("mmap extend error");
        metapage_read(&mut pager, fd_size);

        #[cfg(test)]
        {
            debug!(
                "\npager initialized:\nmmap.total {}\nn_pages {}\nchunks.len {}",
                pager.mmap.read().total,
                pager.npages.load(Ordering::Relaxed),
                pager.mmap.read().chunks.len(),
            );
        }

        Ok(pager)
    }

    /// decodes a page from the mmap
    ///
    /// kv.pageRead, db.pageRead -> pagereadfile
    pub fn decode(&self, ptr: Pointer, node_type: NodeFlag) -> Node {
        assert!(mmap_extend(self, ptr.0 as usize * PAGE_SIZE).is_ok());
        let mmap_ref = self.mmap.read();

        #[cfg(test)]
        {
            debug!(
                "decoding ptr: {}, amount of chunks {}, chunk 0 size {}",
                ptr.0,
                mmap_ref.chunks.len(),
                mmap_ref.chunks[0].len()
            );
        }

        let mut start: usize = 0;
        for chunk in mmap_ref.chunks.iter() {
            let end = start + chunk.len / PAGE_SIZE;
            if ptr.0 < end as u64 {
                let offset: usize = PAGE_SIZE * (ptr.0 as usize - start);

                // TODO chane to Arc directly
                let mut node = match node_type {
                    NodeFlag::Tree => Node::Tree(TreeNode::new()),
                    NodeFlag::Freelist => Node::Freelist(FLNode::new()),
                };

                node[..PAGE_SIZE].copy_from_slice(&chunk[offset..offset + PAGE_SIZE]);
                debug!("returning node at offset {offset}, {}", as_page(offset));

                return node;
            }
            start = end;
        }
        error!("bad pointer: {}", ptr.0);
        panic!()
    }

    /// triggers truncation logic once the freelist exceeds TRUNC_THRESHOLD entries
    pub fn cleanup_check(&self) -> Result<(), Error> {
        let list: Vec<Pointer> = self
            .freelist
            .read()
            .peek_ptr()
            .ok_or(FLError::TruncateError(
                "could not retrieve pointer from FL".to_string(),
            ))?;

        if list.len() > TRUNC_THRESHOLD {
            self.truncate(list)
        } else {
            Ok(())
        }
    }

    /// attempts to truncate the file. Makes calls to and modifies freelist. This function should therefore be called
    /// after tree operations. Truncation amount is based on count_trunc_pages() algorithm
    #[instrument(skip_all)]
    pub fn truncate(&self, list: Vec<Pointer>) -> Result<(), Error> {
        let _guard = self.lock.lock();

        let npages = self.npages.load(Ordering::Relaxed);
        if npages <= 2 {
            return Err(
                FLError::TruncateError("cant truncate from empty database".to_string()).into(),
            );
        }

        match count_trunc_pages(npages, &list) {
            Some(count) => {
                for i in 0..count {
                    // removing items from freelist
                    let ptr = self
                        .freelist
                        .write()
                        .get()
                        .ok_or(FLError::PopError("couldnt pop from freelist".to_string()))?;

                    debug_assert_eq!(list[i as usize], ptr);

                    if list[i as usize] != ptr {
                        return Err(FLError::TruncateError(format!(
                            "pointer {}, doesnt match {}",
                            list[i as usize], ptr
                        ))
                        .into());
                    }
                }
                let new_npage = npages - count;

                self.npages.store(new_npage, Ordering::SeqCst);
                metapage_write(self, &metapage_save(self))?;
                fsync(&self.database)?;

                ftruncate(&self.database, new_npage * PAGE_SIZE as u64)?;
                fsync(&self.database)?;

                Ok(())
            }
            None => Ok(()),
        }
    }

    pub fn read(&self, ptr: Pointer, flag: NodeFlag, version: u64) -> Arc<Node> {
        debug!(node=?flag, %ptr, version, "reading page...");
        let mut buf_shr = self.buf_shared.write();

        // check buffer first
        if let Some(n) = buf_shr.get(ptr, version) {
            debug!("page found in buffer!");
            n.clone()
        } else {
            debug!("reading from disk...");

            let n = self.decode(ptr, flag);

            buf_shr.insert(ptr, n, version);
            buf_shr.get(ptr, version).expect("we just added it")
        }
    }

    pub fn alloc(&self, node: &Node, version: u64, nappend: u32) -> AllocatedPage {
        assert!(node.fits_page());

        let max_ver = match self.ongoing.write().get_oldest_version() {
            Some(n) => n,
            None => self.version.load(Ordering::Relaxed),
        };
        let mut fl_ref = self.freelist.write();
        fl_ref.set_max_ver(max_ver);

        // check freelist first
        if let Some(ptr) = fl_ref.get() {
            debug!("allocating from buffer");

            assert_ne!(ptr.0, 0);

            let page = AllocatedPage {
                ptr,
                version,
                origin: PageOrigin::Freelist,
            };

            page
        } else {
            debug!("allocating from append");

            let ptr = Pointer(self.npages.load(Ordering::Relaxed) + nappend as u64);

            assert_ne!(ptr.0, 0);

            debug!(
                "encode: adding {:?} at page: {} to buffer",
                node.get_type(),
                ptr.0
            );

            let page = AllocatedPage {
                ptr,
                version,
                origin: PageOrigin::Append,
            };

            page
        }
    }
}

/// returns the number of pages that can be safely truncated, by evaluating a contiguous sequence at the end of the freelist. This function has O(n logn ) worst case performance.
///
/// credit to ranveer
fn count_trunc_pages(npages: u64, freelist: &[Pointer]) -> Option<u64> {
    if freelist.is_empty() || npages <= 2 {
        return None;
    }

    let max_possible = (npages - 2) as usize;

    let mut seen = HashSet::new();
    let mut min_page = u64::MAX;
    let mut max_page = 0u64;

    let mut best: Option<u64> = None;
    let mut saw_last_page_anywhere = false;

    let first_page = freelist[0].get();

    for (i, ptr) in freelist.iter().enumerate() {
        if i >= max_possible {
            break;
        }

        let page = ptr.get();

        if page == npages - 1 {
            saw_last_page_anywhere = true;
        }

        if page < npages {
            if seen.insert(page) {
                min_page = min_page.min(page);
                max_page = max_page.max(page);
            }
        }

        let k = (i + 1) as u64;
        if k > 1
            && seen.len() as u64 == k
            && max_page == npages - 1
            && max_page - min_page + 1 == k
            && min_page == npages - k
        {
            best = Some(k);
        }
    }
    if best.is_none()
        && saw_last_page_anywhere
        && (first_page == npages - 1 || first_page >= npages)
    {
        return Some(1);
    }
    best
}

#[cfg(test)]
mod truncate {
    use super::*;

    fn ptr(page: u64) -> Pointer {
        Pointer::from(page)
    }

    #[test]
    fn cleanup_helper1() {
        let list: Vec<Pointer> = vec![Pointer(6), Pointer(9), Pointer(8), Pointer(7), Pointer(4)];
        let res = count_trunc_pages(10, &list);
        assert_eq!(res, Some(4));

        let list: Vec<Pointer> = vec![Pointer(6), Pointer(9), Pointer(8), Pointer(4), Pointer(7)];
        let res = count_trunc_pages(10, &list);
        assert_eq!(res, None);

        let list: Vec<Pointer> = vec![Pointer(9), Pointer(8), Pointer(7), Pointer(6), Pointer(5)];
        let res = count_trunc_pages(10, &list);
        assert_eq!(res, Some(5));

        let list: Vec<Pointer> = vec![Pointer(9), Pointer(4), Pointer(7), Pointer(6), Pointer(5)];
        let res = count_trunc_pages(10, &list);
        assert_eq!(res, Some(1));

        let list: Vec<Pointer> = vec![Pointer(1), Pointer(4), Pointer(7), Pointer(6), Pointer(5)];
        let res = count_trunc_pages(10, &list);
        assert_eq!(res, None);
    }
    #[test]
    fn test_cleanup_check_empty_list() {
        let result = count_trunc_pages(100, &[]);
        assert_eq!(result, None);
    }

    #[test]
    fn test_cleanup_check_npages_too_small() {
        let list = vec![ptr(0), ptr(1)];
        let result = count_trunc_pages(2, &list);
        assert_eq!(result, None);
    }

    #[test]
    fn test_cleanup_check_small_tail_sequence() {
        // Only 5 pages at tail - function should still return it
        let list = vec![ptr(99), ptr(98), ptr(97), ptr(96), ptr(95)];
        let result = count_trunc_pages(100, &list);
        assert_eq!(result, Some(5), "Should return even small sequences");
    }

    #[test]
    fn test_cleanup_check_single_page() {
        let list = vec![ptr(99)];
        let result = count_trunc_pages(100, &list);
        assert_eq!(result, Some(1), "Single tail page should be detected");
    }

    #[test]
    fn test_cleanup_check_exactly_100_pages() {
        let list: Vec<Pointer> = (900..1000).map(ptr).collect();
        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, Some(100));
    }

    #[test]
    fn test_cleanup_check_tail_sequence_unordered() {
        let mut list: Vec<Pointer> = (900..1000).map(ptr).collect();
        list.reverse();

        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, Some(100), "Order shouldn't matter");
    }

    #[test]
    fn test_cleanup_check_large_tail_sequence() {
        let list: Vec<Pointer> = (800..1000).map(ptr).collect();
        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, Some(200));
    }

    #[test]
    fn test_cleanup_check_gap_in_sequence() {
        // Missing page 98
        let list = vec![ptr(99), ptr(97), ptr(96), ptr(95)];
        let result = count_trunc_pages(100, &list);
        assert_eq!(result, Some(1), "Gap breaks the tail sequence");
    }

    #[test]
    fn test_cleanup_check_not_at_tail() {
        let list: Vec<Pointer> = (400..500).map(ptr).collect();
        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, None, "Pages not at tail should return None");
    }

    #[test]
    fn test_cleanup_check_first_element_breaks_pattern() {
        let mut list = vec![ptr(500)];
        list.extend((900..1000).map(ptr));

        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, None, "First non-tail element breaks pattern");
    }

    #[test]
    fn test_cleanup_check_entire_file_nearly_free() {
        let list: Vec<Pointer> = (2..1000).map(ptr).collect();
        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, Some(998));
    }

    #[test]
    fn test_cleanup_check_shuffled_valid_tail() {
        let mut list: Vec<Pointer> = (850..1000).map(ptr).collect();
        let len = list.len();

        for i in 0..list.len() / 2 {
            list.swap(i, len - 1 - i);
        }

        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, Some(150));
    }

    #[test]
    fn test_cleanup_check_duplicate_pages() {
        // claude got confused, this test might not make sense
        let list = vec![ptr(999), ptr(999), ptr(998), ptr(997)];

        let result = count_trunc_pages(1000, &list);
        assert_eq!(
            result,
            Some(1),
            "Finds largest valid tail despite duplicate"
        );
    }

    #[test]
    fn test_cleanup_check_realistic_fragmentation() {
        let mut list = vec![];

        // 150 tail pages in reverse order
        for i in (850..1000).rev() {
            list.push(ptr(i));
        }

        // Some middle pages
        list.extend(vec![ptr(500), ptr(501), ptr(502)]);

        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, Some(150));
    }

    #[test]
    fn test_cleanup_check_alternating_gaps() {
        // Even numbers only: 990, 992, 994, 996, 998
        let list = vec![ptr(998), ptr(996), ptr(994), ptr(992), ptr(990)];
        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, None, "Gaps make it non-consecutive");
    }

    #[test]
    fn test_cleanup_check_max_possible_calculation() {
        // Test that max_possible = min(list.len(), npages-2)

        // Case 1: list.len() < npages-2
        let list: Vec<Pointer> = (990..1000).map(ptr).collect(); // 10 items
        let result = count_trunc_pages(1000, &list); // npages-2 = 998
        assert_eq!(result, Some(10), "Limited by list length");

        // Case 2: list.len() > npages-2
        let list2: Vec<Pointer> = (0..100).map(ptr).collect(); // 100 items
        let result2 = count_trunc_pages(50, &list2); // npages-2 = 48
        // Can only check up to 48 pages
        assert_eq!(result2, None, "Not a valid tail for npages=50");
    }
    #[test]
    fn test_prefix_invalidated_by_duplicate_early() {
        let list = vec![ptr(999), ptr(999), ptr(998)];
        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, Some(1));
    }

    #[test]
    fn test_prefix_valid_then_invalid_page_stops_growth() {
        let list = vec![ptr(999), ptr(998), ptr(400)];
        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, Some(2));
    }

    #[test]
    fn test_prefix_valid_then_gap_breaks() {
        let list = vec![ptr(999), ptr(997), ptr(996)];
        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, Some(1));
    }

    #[test]
    fn test_prefix_unordered_but_consecutive() {
        let list = vec![ptr(998), ptr(999), ptr(997)];
        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, Some(3));
    }

    #[test]
    fn test_prefix_exact_tail_then_duplicate() {
        let list = vec![ptr(999), ptr(998), ptr(997), ptr(998)];
        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, Some(3));
    }

    #[test]
    fn test_prefix_exact_tail_then_out_of_range() {
        let list = vec![ptr(999), ptr(998), ptr(997), ptr(2000)];
        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, Some(3));
    }

    #[test]
    fn test_prefix_contains_zero_page_breaks_tail() {
        let list = vec![ptr(999), ptr(0), ptr(998)];
        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, Some(1));
    }

    #[test]
    fn test_prefix_long_valid_then_gap() {
        let list = vec![ptr(999), ptr(998), ptr(997), ptr(995)];
        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, Some(3));
    }

    #[test]
    fn test_prefix_valid_exact_np_minus_two_at_tail() {
        let list: Vec<Pointer> = (6..104).map(ptr).collect();
        let result = count_trunc_pages(104, &list);
        assert_eq!(result, Some(98));
    }

    #[test]
    fn test_prefix_with_reversed_large_tail() {
        let mut list: Vec<Pointer> = (900..1000).map(ptr).collect();
        list.reverse();
        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, Some(100));
    }

    #[test]
    fn test_prefix_partial_tail_only() {
        let list = vec![ptr(999), ptr(998), ptr(500), ptr(997)];
        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, Some(2));
    }

    #[test]
    fn test_prefix_duplicate_late_does_not_extend() {
        let list = vec![ptr(999), ptr(998), ptr(997), ptr(997)];
        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, Some(3));
    }

    #[test]
    fn test_prefix_starts_at_tail_minus_one_fails() {
        let list = vec![ptr(998), ptr(997), ptr(996)];
        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, None);
    }

    #[test]
    fn test_prefix_valid_with_minimal_tail() {
        let list = vec![ptr(999), ptr(998)];
        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, Some(2));
    }
}
