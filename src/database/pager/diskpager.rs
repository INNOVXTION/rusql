use std::cell::{Cell, Ref, RefCell};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::os::fd::OwnedFd;
use std::rc::Rc;

use parking_lot::Mutex;
use parking_lot::ReentrantMutex;
use rustix::fs::{fstat, fsync, ftruncate};
use tracing::{debug, error, info, instrument, warn};

use crate::create_file_sync;
use crate::database::BTree;
use crate::database::api::tx::TX;
use crate::database::errors::FLError;
use crate::database::helper::as_page;
use crate::database::pager::buffer::{NodeBuffer, OngoingTX};
use crate::database::pager::freelist::{FLConfig, FLNode, FreeList, GC};
use crate::database::pager::metapage::*;
use crate::database::pager::mmap::*;
use crate::database::pager::transaction::TXHistory;
use crate::database::tables::{Key, Record, Value};
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

#[derive(Debug, Clone, Copy)]
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
    pub buffer: RefCell<NodeBuffer>, // read only pages
    pub mmap: RefCell<Mmap>,

    pub failed: Cell<bool>,

    pub tree: Rc<RefCell<dyn Tree<Codec = Self>>>, // only used as reference for transactions
    pub freelist: Rc<RefCell<dyn GC<Codec = Self>>>,

    pub version: RefCell<u64>,
    pub ongoing: RefCell<OngoingTX>,

    pub history: RefCell<TXHistory>,
    pub rollback: RefCell<MetaPage>,

    pub lock: ReentrantMutex<()>,
    // WIP
    // clean factor
    // counter after deletion for cleanup
}

/// internal callback API
pub(crate) trait Pager {
    // tree callbacks
    fn page_read(&self, ptr: Pointer, flag: NodeFlag) -> Rc<RefCell<Node>>; //tree decode
    fn page_alloc(&self, node: Node, version: u64) -> Pointer; //tree encode
    fn dealloc(&self, ptr: Pointer); // tree dealloc/del
}

pub(crate) trait GCCallbacks {
    // FL callbacks
    fn encode(&self, node: Node) -> Pointer; // FL encode
    fn update(&self, ptr: Pointer) -> Rc<RefCell<Node>>; // FL update
}

impl Pager for DiskPager {
    /// decodes a page, checks buffer before reading disk
    ///
    /// `BTree.get`, reads a page possibly from buffer or disk
    fn page_read(&self, ptr: Pointer, flag: NodeFlag) -> Rc<RefCell<Node>> {
        let mut buf_ref = self.buffer.borrow_mut();
        // check buffer first
        debug!(node=?flag, %ptr, "page read");
        if let Some(n) = buf_ref.get(ptr) {
            debug!("page found in buffer!");
            n
        } else {
            debug!("reading from disk...");
            let n = self.decode(ptr, flag);

            buf_ref.insert_clean(ptr, n.clone(), *self.version.borrow());
            buf_ref.get(ptr).expect("we just inserted it")
        }
    }

    /// allocates a new page to be encoded, checks freelist first before appending to disk
    ///
    /// pageAlloc, new
    fn page_alloc(&self, node: Node, version: u64) -> Pointer {
        // check freelist first
        debug!("allocating ...");
        if let Some(ptr) = self.freelist.borrow_mut().get() {
            assert_ne!(ptr.0, 0);

            // loading page into buffer
            self.buffer
                .borrow_mut()
                .insert_dirty(ptr, node, *self.version.borrow());

            self.buffer.borrow().debug_print();
            ptr
        } else {
            // increment append?
            debug!("appending to disk...");
            self.encode(node)
        }
    }

    /// marks a page for deallocation. That page still lingers inside the buffer and is removed before the
    /// next transaction
    /// PushTail
    fn dealloc(&self, ptr: Pointer) {
        // adding to freelist
        self.freelist
            .borrow_mut()
            .append(ptr, *self.version.borrow())
            .expect("dealloc error");

        // // retiring page from buffer, to be cleared later
        // if let Some(entry) = self.buffer.borrow_mut().hmap.get_mut(&ptr) {
        //     entry.retired = true;
        // };
    }
}

impl GCCallbacks for DiskPager {
    /// adds pages to buffer to be encoded to disk later (append)
    ///
    /// does not check if node exists in buffer!
    ///
    /// pageAppend
    fn encode(&self, node: Node) -> Pointer {
        let _guard = self.lock.lock();

        let buf_ref = &mut self.buffer.borrow_mut();
        let ptr = Pointer(buf_ref.npages + buf_ref.nappend);

        // empty db has n_pages = 1 (meta page)
        assert!(node.fits_page());

        debug!(
            "encode: adding {:?} at page: {} to buffer",
            node.get_type(),
            ptr.0
        );

        buf_ref.nappend += 1;
        buf_ref.insert_dirty(ptr, node, *self.version.borrow());
        buf_ref.debug_print();

        assert_ne!(ptr.0, 0);
        ptr
    }

    /// callback for free list
    ///
    /// checks buffer for allocated page and returns pointer
    fn update(&self, ptr: Pointer) -> Rc<RefCell<Node>> {
        let _guard = self.lock.lock();

        let buf = &mut self.buffer.borrow_mut();

        // checking buffer,...
        let entry = match buf.get(ptr) {
            Some(n) => {
                debug!("updating {} in buffer", ptr);
                n
            }
            None => {
                // decoding page from disk and loading it into buffer
                debug!(%ptr, "reading free list from disk...");
                buf.insert_dirty(
                    ptr,
                    self.decode(ptr, NodeFlag::Freelist),
                    *self.version.borrow(),
                );
                buf.get(ptr).expect("we just inserted it")
            }
        };
        buf.debug_print();
        entry
    }
}

impl DiskPager {
    /// initializes pager
    ///
    /// opens file, and sets up callbacks for the tree
    pub fn open(path: &'static str) -> Result<Rc<Self>, Error> {
        let mut pager = Rc::new_cyclic(|w| DiskPager {
            path,
            database: create_file_sync(path).expect("file open error"),
            failed: Cell::new(false),
            buffer: RefCell::new(NodeBuffer {
                hmap: HashMap::new(),
                nappend: 0,
                npages: 0,
            }),
            mmap: RefCell::new(Mmap {
                total: 0,
                chunks: vec![],
            }),
            tree: Rc::new(RefCell::new(BTree::<Self>::new(w.clone()))),
            freelist: Rc::new(RefCell::new(FreeList::<Self>::new(w.clone()))),

            version: RefCell::new(1),
            ongoing: RefCell::new(OngoingTX {
                map: BTreeMap::new(),
            }),
            rollback: RefCell::new(MetaPage::new()),
            history: RefCell::new(TXHistory {
                history: HashMap::new(),
                limit: 0,
            }),
            lock: ReentrantMutex::new(()),
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
                pager.mmap.borrow().total,
                pager.buffer.borrow().npages,
                pager.mmap.borrow().chunks.len(),
            );
        }

        Ok(pager)
    }

    #[instrument(name = "pager update file", skip_all)]
    pub fn update_or_revert(&self, recov_page: &MetaPage) -> Result<(), Error> {
        debug!("tree operation complete, updating file");

        // making sure the meta page is a known good state after a potential write error
        if self.failed.get() {
            debug!("failed update detected, restoring meta page...");

            metapage_write(self, recov_page).expect("meta page recovery write error");
            fsync(&self.database).expect("fsync metapage for restoration failed");
            self.failed.set(false);
        };

        // in case the file writing fails, we revert back to the old meta page
        if let Err(e) = self.file_update() {
            warn!(%e, "file update failed! Reverting meta page...");

            // save the pager from before the current operation to be rewritten later
            metapage_load(self, recov_page);

            // discard buffer
            self.buffer.borrow_mut().hmap.clear();
            self.failed.set(true);

            return Err(Error::PagerError(e));
        }
        Ok(())
    }

    /// write sequence
    pub fn file_update(&self) -> Result<(), PagerError> {
        // updating free list for next update
        self.freelist.borrow_mut().set_max_seq();

        // flush buffer to disk
        self.page_write()?;
        fsync(&self.database)?;

        // write currently loaded metapage to disk
        metapage_write(self, &metapage_save(self))?;
        fsync(&self.database)?;

        Ok(())
    }

    /// helper function: writePages, flushes the buffer
    pub fn page_write(&self) -> Result<(), PagerError> {
        debug!("writing page...");

        let buf = self.buffer.borrow();
        let buf_len = buf.hmap.len();
        let npage = buf.npages;

        // extend the mmap if needed
        let new_size = (npage as usize + buf_len) * PAGE_SIZE; // amount of pages in bytes
        mmap_extend(self, new_size).map_err(|e| {
            error!(%e, new_size, "Error when extending mmap");
            e
        })?;
        debug!(
            nappend = buf.nappend,
            buffer = buf_len,
            "pages to be written:"
        );

        // iterate over buffer and write nodes to designated pages
        let mut bytes_written: usize = 0;
        let mut count = 0;

        for pair in buf.to_dirty_iter() {
            debug!("writing {:<10} at {:<5}", pair.1.get_type(), pair.0);
            assert!(pair.0.get() != 0); // never write to the meta page

            let offset = pair.0.get() * PAGE_SIZE as u64;
            let io_slice = rustix::io::IoSlice::new(&pair.1[..PAGE_SIZE]);
            bytes_written +=
                rustix::io::pwrite(&self.database, &io_slice, offset).map_err(|e| {
                    error!(?e, "page writing error!");
                    PagerError::WriteFileError(e)
                })?;
            count += 1;
        }
        debug!(bytes_written, "bytes written:");
        if bytes_written != count * PAGE_SIZE {
            return Err(PagerError::PageWriteError(
                "wrong amount of bytes written".to_string(),
            ));
        };

        // adjust buffer
        drop(buf);
        let mut buf = self.buffer.borrow_mut();

        buf.npages += buf.nappend;
        buf.nappend = 0;
        buf.clear();

        Ok(())
    }

    /// decodes a page from the mmap
    ///
    /// kv.pageRead, db.pageRead -> pagereadfile
    fn decode(&self, ptr: Pointer, node_type: NodeFlag) -> Node {
        let mmap_ref = self.mmap.borrow();

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
        let list: Vec<Pointer> =
            self.freelist
                .borrow()
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
        let npages = self.buffer.borrow().npages;
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
                        .borrow_mut()
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

                self.buffer.borrow_mut().npages = new_npage;
                metapage_write(self, &metapage_save(self))?;
                fsync(&self.database)?;

                ftruncate(&self.database, new_npage * PAGE_SIZE as u64)?;
                fsync(&self.database)?;

                Ok(())
            }
            None => Ok(()),
        }
    }

    pub fn read(&self, ptr: Pointer, flag: NodeFlag) -> Rc<RefCell<Node>> {
        let _lock = self.lock.lock();
        let mut buf_ref = self.buffer.borrow_mut();

        // check buffer first
        debug!(node=?flag, %ptr, "page read");
        if let Some(n) = buf_ref.get(ptr) {
            debug!("page found in buffer!");
            n
        } else {
            debug!("reading from disk...");
            // buf_ref.debug_print();

            let n = self.decode(ptr, flag);

            buf_ref.insert_clean(ptr, n.clone(), *self.version.borrow());
            buf_ref.get(ptr).expect("we just inserted it")
        }
    }

    pub fn alloc(&self, node: &Node, version: u64, nappend: u32) -> AllocatedPage {
        let _lock = self.lock.lock();

        let max_ver = match self.ongoing.borrow_mut().get_oldest_version() {
            Some(n) => n,
            None => *self.version.borrow(),
        };
        let mut fl_ref = self.freelist.borrow_mut();
        fl_ref.set_max_ver(max_ver);

        // check freelist first
        if let Some(ptr) = fl_ref.get() {
            debug!("allocating from buffer");

            assert_ne!(ptr.0, 0);
            self.buffer.borrow().debug_print();

            let page = AllocatedPage {
                ptr,
                version,
                origin: PageOrigin::Freelist,
            };

            page
        } else {
            debug!("allocating from append");

            let buf_ref = self.buffer.borrow();
            let ptr = Pointer(buf_ref.npages + nappend as u64);
            assert_ne!(ptr.0, 0);

            // empty db has n_pages = 1 (meta page)
            assert!(node.fits_page());

            debug!(
                "encode: adding {:?} at page: {} to buffer",
                node.get_type(),
                ptr.0
            );

            buf_ref.debug_print();

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
