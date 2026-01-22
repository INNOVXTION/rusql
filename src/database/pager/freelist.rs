use parking_lot::{Mutex, RwLockWriteGuard};
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Weak};

use crate::database::codec::{NumDecode, NumEncode};
use crate::database::pager::DiskPager;
use crate::database::pager::diskpager::{GCCallbacks, NodeFlag, Pager};
use crate::database::types::{FREE_PAGE, Node, VER_SIZE};
use crate::database::{
    btree::TreeNode,
    errors::FLError,
    types::{PAGE_SIZE, PTR_SIZE, Pointer},
};
use crate::debug_if_env;
use tracing::{debug, warn};

pub(crate) struct FreeList {
    pub pager: Weak<DiskPager>,

    head_page: Option<Pointer>,
    head_seq: usize,
    tail_page: Option<Pointer>,
    tail_seq: usize,

    pub max_seq: usize, // maximum amount of items in the list

    cur_ver: u64,    // reflects the current pager
    max_ver: u64,    // version permitted to give out, oldest version in diskpager.ongoing
    pub npages: u64, // number of available pages
}

/*
                     first_item
                         ↓
head_page -> [ next |    xxxxx ]
                ↓
             [ next | xxxxxxxx ]
                ↓
tail_page -> [ NULL | xxxx     ]
                         ↑
                     last_item
*/

#[derive(Debug)]
pub(crate) struct FLConfig {
    pub head_page: Option<Pointer>,
    pub head_seq: usize,
    pub tail_page: Option<Pointer>,
    pub tail_seq: usize,

    pub cur_ver: u64,
    pub max_ver: u64,
    pub npages: u64,
}

pub(crate) trait GC {
    fn get(&mut self) -> Option<Pointer>;
    fn append(&mut self, ptr: Pointer, version: u64) -> Result<(), FLError>;

    fn get_config(&self) -> FLConfig;
    fn set_config(&mut self, flc: &FLConfig);

    fn peek_ptr(&self) -> Option<Vec<Pointer>>;
    fn set_max_seq(&mut self);
    fn set_max_ver(&mut self, version: u64);
    fn set_cur_ver(&mut self, version: u64);
}

impl GC for FreeList {
    /// removes a page from the head, decrement head seq
    fn get(&mut self) -> Option<Pointer> {
        assert!(self.head_page.is_some());

        // if self.head_page == self.tail_page {
        //     assert!(self.npages as usize == (self.tail_seq - self.head_seq));
        //     assert!(self.tail_seq >= self.max_seq);
        // }

        if self.npages == 0 {
            return None;
        }

        match self.pop_head() {
            (Some(ptr), Some(head)) => {
                debug!(%ptr, head=%head, "retrieving from freelist with head: ");
                self.append(head, FREE_PAGE).unwrap();
                Some(ptr)
            }
            (Some(ptr), None) => {
                debug!(%ptr, "retrieving from freelist: ");
                Some(ptr)
            }
            (None, None) => None,
            _ => unreachable!(),
        }
    }

    /// add a page to the tail increment tail seq
    /// PushTail
    fn append(&mut self, ptr: Pointer, version: u64) -> Result<(), FLError> {
        debug!("appending {}, version: {version} to free list...", ptr);

        assert_ne!(ptr.get(), 0, "we cant append 0 to the fl");
        assert!(self.tail_page.is_some());

        // if self.head_page == self.tail_page {
        //     assert!(self.tail_seq >= self.max_seq);
        // }

        // updates tail page, by getting a reference to the buffer if its already in there
        // updating appending the pointer
        let seq = self.tail_seq;
        assert_ne!(seq, FREE_LIST_CAP);
        let idx = seq_to_idx(seq);

        self.update_set_ptr(self.tail_page.unwrap(), ptr, version, idx);
        self.tail_seq += 1;
        self.npages += 1;

        // allocating new node if the the node is full
        if seq_to_idx(self.tail_seq) == 0 {
            debug!("tail page full...");
            match self.pop_head() {
                // head page is empty
                (None, None) => {
                    debug!("head node empty!");

                    let new_node = self.encode(FLNode::new()); // this stays as encode
                    self.update_set_next(self.tail_page.unwrap(), new_node);

                    self.tail_page = Some(new_node);
                    self.tail_seq = 0; // experimental
                }

                // setting new page
                (Some(next), None) => {
                    debug!("got page from head...");
                    assert_ne!(next.0, 0);

                    self.update_set_next(self.tail_page.unwrap(), next);

                    self.tail_page = Some(next);
                    self.tail_seq = 0; // experimental
                }

                // getting the last item of the head node and the head node itself
                (Some(next), Some(head)) => {
                    debug!("got last ptr and head!.");
                    assert_ne!(next.0, 0);
                    assert_ne!(head.0, 0);

                    // sets current tail next to new page
                    self.update_set_next(self.tail_page.unwrap(), next);

                    // moves tail to new empty page
                    self.tail_page = Some(next);

                    // appending the empty head
                    self.update_set_ptr(self.tail_page.unwrap(), head, FREE_PAGE, 0);

                    self.tail_seq = 0; // experimental
                    self.tail_seq += 1; // accounting for re-added head
                    self.npages += 1;
                }
                _ => unreachable!(),
            }
        }
        Ok(())
    }

    /// retrieves list of all pointers inside freelist, as if popped by head
    ///
    /// calls encode and does not interact with the buffer so it should be called after the database has been written down
    fn peek_ptr(&self) -> Option<Vec<Pointer>> {
        if self.is_empty() {
            debug!("peek: fl is empty");
            return None;
        }
        let strong = self.pager.upgrade().unwrap();
        let mut buf = strong.buf_fl.write();

        let mut list: Vec<Pointer> = vec![];
        let mut head = self.head_seq;

        debug!(head = ?self.head_page, tail = ?self.tail_page, "retrieving peek list");
        let mut head_page = self.head_page?;
        let tail_page = self.tail_page?;

        let mut node = match buf.get(head_page) {
            Some(n) => n,
            None => {
                // decoding page from disk and loading it into buffer
                buf.insert_clean(head_page, strong.decode(head_page, NodeFlag::Freelist));
                buf.get(head_page).expect("we just inserted it")
            }
        };
        debug!("peeking at {head_page}");

        let max = self.tail_seq;
        loop {
            if head_page == tail_page && head == max {
                // both pointer meet on the same page = free list is empty
                break;
            }
            let ptr = node.as_fl().get_ptr(seq_to_idx(head)).0;

            debug_if_env!("RUSQL_LOG_PEEK", {
                debug!("pushing {ptr}");
            });

            list.push(ptr);
            head += 1;

            if seq_to_idx(head) == 0 {
                head_page = node.as_fl().get_next();
                debug!("peeking at {head_page}");
                node = match buf.get(head_page) {
                    Some(n) => n,
                    None => {
                        // decoding page from disk and loading it into buffer
                        buf.insert_clean(head_page, strong.decode(head_page, NodeFlag::Freelist));
                        buf.get(head_page).expect("we just inserted it")
                    }
                };
                head = 0;
            }
        }
        debug_if_env!("RUSQL_LOG_PEEK", {
            debug!("returning from peek ptr: {:?}", list);
        });
        Some(list)
    }

    fn get_config(&self) -> FLConfig {
        FLConfig {
            head_page: self.head_page,
            head_seq: self.head_seq,
            tail_page: self.tail_page,
            tail_seq: self.tail_seq,
            cur_ver: self.cur_ver,
            max_ver: self.max_ver,
            npages: self.npages,
        }
    }

    fn set_config(&mut self, flc: &FLConfig) {
        self.head_page = flc.head_page;
        self.head_seq = flc.head_seq;
        self.tail_page = flc.tail_page;
        self.tail_seq = flc.tail_seq;
        self.cur_ver = flc.cur_ver;
        self.max_ver = flc.max_ver;
        self.npages = flc.npages;
    }

    /// increments the max_seq for next transaction cycle
    fn set_max_seq(&mut self) {
        debug!("freelist: setting max seq to {}", self.tail_seq);
        self.max_seq = self.tail_seq
    }

    fn set_max_ver(&mut self, version: u64) {
        self.max_ver = version
    }

    fn set_cur_ver(&mut self, version: u64) {
        self.cur_ver = version
    }
}

impl FreeList {
    // callbacks

    /// reads page, gets page, removes from buffer if available
    fn decode(&self, ptr: Pointer) -> Arc<Node> {
        let strong = self.pager.upgrade().unwrap();
        strong.page_read(ptr, NodeFlag::Freelist)
    }

    /// appends page to disk, doesnt make a buffer check
    fn encode(&self, node: FLNode) -> Pointer {
        let strong = self.pager.upgrade().unwrap();
        strong.encode(Node::Freelist(node))
    }

    // /// returns ptr to node inside the allocation buffer
    // fn update(&self, ptr: Pointer) -> Rc<RefCell<Node>> {
    //     let strong = self.pager.upgrade().unwrap();
    //     strong.update(ptr)
    // }

    /// new uninitialized
    pub fn new(pager: Weak<DiskPager>) -> Self {
        FreeList {
            head_page: None,
            head_seq: 0,
            tail_page: None,
            tail_seq: 0,
            max_seq: 0,
            pager: pager,
            cur_ver: 0,
            max_ver: 0,
            npages: 0,
        }
    }

    /// flPop
    fn pop_head(&mut self) -> (Option<Pointer>, Option<Pointer>) {
        debug!("freelist node request");
        // experimental
        // head seq cant overtake max seq when on the same page as tail
        if self.is_empty() {
            // no free page available
            debug!(
                max_seq = self.max_seq,
                tail_seq = self.tail_seq,
                "freelist is empty"
            );
            return (None, None);
        }

        let seq = self.head_seq;
        assert_ne!(seq, FREE_LIST_CAP);
        let idx = seq_to_idx(seq);
        let ptr = self.update_get_ptr(self.head_page.unwrap(), idx);

        // is the version retrieved newer than max_ver
        if ptr.1 > self.max_ver {
            warn!(
                max_ver = self.max_ver,
                ptr_ver = ptr.1,
                "cant give out freelist node"
            );
            return (None, None);
        }

        self.head_seq += 1;
        self.npages -= 1;

        debug!(
            "getting ptr {} from head at {}",
            ptr.0,
            self.head_page.unwrap()
        );

        // in case the head page is empty we reuse it
        if seq_to_idx(self.head_seq) == 0 {
            let head = self.head_page.unwrap();

            // self.head_page = Some(node.get_next());
            self.head_page = Some(self.update_get_next(self.head_page.unwrap()));
            self.head_seq = 0; // experimental: resetting the counter

            // evict from buffer
            let strong = self.pager.upgrade().unwrap();
            let mut buf = strong.update(head);
            buf.delete(head);

            return (Some(ptr.0), Some(head));
        }
        (Some(ptr.0), None)
    }

    /// gets pointer from idx
    fn update_get_ptr(&self, ptr: Pointer, idx: u16) -> (Pointer, u64) {
        let strong = self.pager.upgrade().unwrap();
        let mut buf = strong.update(ptr);

        // checking buffer,...
        let entry = match buf.get(ptr) {
            Some(n) => {
                debug!("updating {} in buffer", ptr);
                n
            }
            None => {
                // decoding page from disk and loading it into buffer
                debug!(%ptr, "reading free list from disk...");
                buf.insert_dirty(ptr, strong.decode(ptr, NodeFlag::Freelist));
                buf.get(ptr).expect("we just inserted it")
            }
        };

        let node_ptr = entry.as_fl();
        node_ptr.get_ptr(idx)
    }

    /// sets ptr at idx for free list node
    fn update_set_ptr(&self, fl: Pointer, ptr: Pointer, version: u64, idx: u16) {
        let strong = self.pager.upgrade().unwrap();
        let mut buf = strong.update(ptr);

        // checking buffer,...
        let entry = match buf.get_mut(fl) {
            Some(n) => {
                debug!("updating {} in buffer", fl);
                n
            }
            None => {
                // decoding page from disk and loading it into buffer
                debug!(%fl, "reading free list from disk...");
                buf.insert_dirty(fl, strong.decode(fl, NodeFlag::Freelist));
                buf.get_mut(fl).expect("we just inserted it")
            }
        };

        let node_ptr = entry.as_fl_mut();
        node_ptr.set_ptr(idx, ptr, version);
        buf.debug_print();
        buf.set_dirty(&fl);
    }

    /// gets next ptr from free list node
    fn update_get_next(&self, ptr: Pointer) -> Pointer {
        let strong = self.pager.upgrade().unwrap();
        let mut buf = strong.update(ptr);

        // checking buffer,...
        let entry = match buf.get(ptr) {
            Some(n) => {
                debug!("updating {} in buffer", ptr);
                n
            }
            None => {
                // decoding page from disk and loading it into buffer
                debug!(%ptr, "reading free list from disk...");
                buf.insert_dirty(ptr, strong.decode(ptr, NodeFlag::Freelist));
                buf.get(ptr).expect("we just inserted it")
            }
        };

        let node_ptr = entry.as_fl();
        node_ptr.get_next()
    }

    /// sets next ptr for free list node
    fn update_set_next(&self, fl: Pointer, ptr: Pointer) {
        let strong = self.pager.upgrade().unwrap();
        let mut buf = strong.update(ptr);

        // checking buffer,...
        let entry = match buf.get_mut(fl) {
            Some(n) => {
                debug!("updating {} in buffer", fl);
                n
            }
            None => {
                // decoding page from disk and loading it into buffer
                debug!(%fl, "reading free list from disk...");
                buf.insert_dirty(fl, strong.decode(fl, NodeFlag::Freelist));
                buf.get_mut(fl).expect("we just inserted it")
            }
        };

        let node_ptr = entry.as_fl_mut();
        node_ptr.set_next(ptr);
        buf.set_dirty(&fl);
    }

    fn is_empty(&self) -> bool {
        if self.head_page != self.tail_page {
            return false;
        }
        if self.head_seq >= self.max_seq {
            return true;
        }
        if self.head_seq >= self.tail_seq {
            return true;
        }
        false
    }
}

// converts seq to idx
fn seq_to_idx(seq: usize) -> u16 {
    seq as u16 % FREE_LIST_CAP as u16
}

const FREE_LIST_NEXT: usize = PTR_SIZE;
const FREE_LIST_CAP: usize = (PAGE_SIZE - FREE_LIST_NEXT) / (PTR_SIZE + VER_SIZE); // 255.5

// -------Free List Node-------
// | next | pointers | unused |
// |  8B  | n*(8B+8B)|   ...  |

#[derive(Debug)]
pub struct FLNode(Box<[u8; PAGE_SIZE]>);

impl FLNode {
    pub fn new() -> Self {
        FLNode(Box::new([0u8; PAGE_SIZE]))
    }

    fn get_next(&self) -> Pointer {
        debug!("getting next");

        let ptr = (&self[0..]).read_u64();
        assert_ne!(ptr, 0, "next fl cant be 0");
        Pointer(ptr)
    }

    fn set_next(&mut self, ptr: Pointer) {
        debug!(ptr = ?ptr, "setting next");
        assert_ne!(ptr.get(), 0, "we can put the mp to the fl");

        (&mut self[0..]).write_u64(ptr.get());
    }

    fn get_ptr(&self, idx: u16) -> (Pointer, u64) {
        // debug!(idx, "getting pointer from free list node at");
        let offset = FREE_LIST_NEXT + ((PTR_SIZE + VER_SIZE) * idx as usize);
        let mut r_slice = &self[offset..];

        let ptr = r_slice.read_u64();
        let ver = r_slice.read_u64();

        assert_ne!(ptr, 0, "we cant receive the mp from the fl");
        (Pointer(ptr), ver)
    }

    fn set_ptr(&mut self, idx: u16, ptr: Pointer, version: u64) {
        debug!(idx, ptr = ?ptr, version = version, "setting pointer in fl node");
        assert_ne!(ptr.get(), 0, "not possible to set ptr 0 in fl");
        let offset = FREE_LIST_NEXT + ((PTR_SIZE + VER_SIZE) * idx as usize);
        let w_slice = &mut self[offset..];

        w_slice.write_u64(ptr.get()).write_u64(version);
    }
}

impl From<TreeNode> for FLNode {
    fn from(value: TreeNode) -> Self {
        let mut n = FLNode::new();
        n.copy_from_slice(&value[..PAGE_SIZE]);
        n
    }
}

impl Clone for FLNode {
    fn clone(&self) -> Self {
        FLNode(self.0.clone())
    }
}

impl Deref for FLNode {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0[..]
    }
}

impl DerefMut for FLNode {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0[..]
    }
}

impl Debug for FreeList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FreeList")
            .field("head page", &self.head_page)
            .field("head seq", &self.head_seq)
            .field("tail page", &self.tail_page)
            .field("tail seq", &self.tail_seq)
            .finish()
    }
}

#[cfg(test)]
mod test {
    use crate::database::{
        pager::freelist::{FLNode, FREE_LIST_CAP, seq_to_idx},
        types::Pointer,
    };

    #[test]
    fn modulo() {
        let x = 7;
        assert_eq!(x % 10, 7);
        let x = 0;
        assert_eq!(x % 10, 0);
        let x = 1;
        assert_eq!(x % 10, 1);
        let x = 10;
        assert_eq!(x % 10, 0);

        let p1 = Some(Pointer::from(1));
        let p2 = Some(Pointer::from(2));

        let seq1 = 0;
        let seq2 = 255;

        assert_eq!(seq1 % FREE_LIST_CAP, seq2 % FREE_LIST_CAP);

        assert_ne!(p1, p2)
    }

    #[test]
    fn test_fl_node_set_and_get_next() {
        let mut node = FLNode::new();
        let ptr = Pointer(100);

        node.set_next(ptr);
        let retrieved = node.get_next();

        assert_eq!(retrieved, ptr);
    }

    #[test]
    fn test_fl_node_set_and_get_ptr_single() {
        let mut node = FLNode::new();
        let ptr = Pointer(42);
        let version = 1u64;
        let idx = 0u16;

        node.set_ptr(idx, ptr, version);
        let (retrieved_ptr, retrieved_ver) = node.get_ptr(idx);

        assert_eq!(retrieved_ptr, ptr);
        assert_eq!(retrieved_ver, version);
    }

    #[test]
    fn test_fl_node_set_and_get_ptr_multiple() {
        let mut node = FLNode::new();

        // Set multiple pointers at different indices
        let test_cases = vec![
            (0, Pointer(10), 1u64),
            (1, Pointer(20), 2u64),
            (5, Pointer(50), 5u64),
            (10, Pointer(100), 10u64),
        ];

        for (idx, ptr, ver) in &test_cases {
            node.set_ptr(*idx, *ptr, *ver);
        }

        // Verify all stored correctly
        for (idx, ptr, ver) in test_cases {
            let (retrieved_ptr, retrieved_ver) = node.get_ptr(idx);
            assert_eq!(retrieved_ptr, ptr, "Failed at index {}", idx);
            assert_eq!(retrieved_ver, ver, "Failed at index {}", idx);
        }
    }

    #[test]
    fn test_fl_node_ptr_at_boundary() {
        let mut node = FLNode::new();

        // Test first and last valid indices
        let first_idx = 0u16;
        let last_idx = (FREE_LIST_CAP - 1) as u16;

        node.set_ptr(first_idx, Pointer(111), 11);
        node.set_ptr(last_idx, Pointer(222), 22);

        let (ptr1, ver1) = node.get_ptr(first_idx);
        let (ptr2, ver2) = node.get_ptr(last_idx);

        assert_eq!(ptr1, Pointer(111));
        assert_eq!(ver1, 11);
        assert_eq!(ptr2, Pointer(222));
        assert_eq!(ver2, 22);
    }

    #[test]
    fn test_fl_node_next_and_ptr_separate() {
        let mut node = FLNode::new();

        // Set next pointer
        let next_ptr = Pointer(999);
        node.set_next(next_ptr);

        // Set data pointers at different indices
        node.set_ptr(0, Pointer(1), 1);
        node.set_ptr(1, Pointer(2), 2);

        // Verify next pointer is unchanged
        let retrieved_next = node.get_next();
        assert_eq!(retrieved_next, next_ptr);

        // Verify data pointers are correct
        let (ptr0, ver0) = node.get_ptr(0);
        let (ptr1, ver1) = node.get_ptr(1);
        assert_eq!(ptr0, Pointer(1));
        assert_eq!(ver0, 1);
        assert_eq!(ptr1, Pointer(2));
        assert_eq!(ver1, 2);
    }

    #[test]
    fn test_seq_to_idx_wrapping() {
        // Test that seq wraps around correctly
        assert_eq!(seq_to_idx(0), 0);
        assert_eq!(seq_to_idx(1), 1);
        assert_eq!(seq_to_idx(FREE_LIST_CAP - 1), (FREE_LIST_CAP - 1) as u16);
        assert_eq!(seq_to_idx(FREE_LIST_CAP), 0); // Should wrap
        assert_eq!(seq_to_idx(FREE_LIST_CAP + 1), 1); // Should wrap
        assert_eq!(seq_to_idx(2 * FREE_LIST_CAP), 0); // Should wrap
    }

    #[test]
    fn test_fl_node_overwrite_ptr() {
        let mut node = FLNode::new();
        let idx = 5u16;

        // Set initial value
        node.set_ptr(idx, Pointer(100), 1);
        let (ptr, ver) = node.get_ptr(idx);
        assert_eq!(ptr, Pointer(100));
        assert_eq!(ver, 1);

        // Overwrite with new value
        node.set_ptr(idx, Pointer(200), 2);
        let (ptr, ver) = node.get_ptr(idx);
        assert_eq!(ptr, Pointer(200));
        assert_eq!(ver, 2);
    }

    #[test]
    fn test_fl_node_independent_slots() {
        let mut node = FLNode::new();

        // Set multiple slots and verify they don't interfere
        for i in 0..10 {
            node.set_ptr(i as u16, Pointer((i + 100) as u64), i as u64);
        }

        // Verify all values are independent
        for i in 0..10 {
            let (ptr, ver) = node.get_ptr(i as u16);
            assert_eq!(
                ptr.get(),
                (i + 100) as u64,
                "Pointer mismatch at index {}",
                i
            );
            assert_eq!(ver, i as u64, "Version mismatch at index {}", i);
        }
    }
}
