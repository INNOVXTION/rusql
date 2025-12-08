use std::fmt::Debug;
use std::ops::{Deref, DerefMut};

use crate::database::{
    btree::TreeNode,
    errors::FLError,
    helper::{read_pointer, write_pointer},
    types::{PAGE_SIZE, PTR_SIZE, Pointer},
};
use tracing::debug;

pub(crate) struct FreeList {
    pub head_page: Option<Pointer>,
    pub head_seq: usize,
    pub tail_page: Option<Pointer>,
    pub tail_seq: usize,
    // maximum amount of items in the list
    max_seq: usize,
    //
    // callbacks
    // reads page, gets page, removes from buffer if available
    pub decode: Box<dyn Fn(Pointer) -> FLNode>,
    // appends page, goes straight into buffer
    pub encode: Box<dyn Fn(FLNode) -> Pointer>,
    /*
    returns ptr to node inside the allocation buffer
    SAFETY:
    needs to be called in isolation, calls to encode can resize the buffer
    and therefore invalidate the returning pointer, use the dedicated helper functions!
    */
    pub update: Box<dyn Fn(Pointer) -> *mut FLNode>,
}

impl FreeList {
    /// new uninitialized
    pub fn new() -> Self {
        FreeList {
            head_page: None,
            head_seq: 0,
            tail_page: None,
            tail_seq: 0,
            max_seq: 0,
            decode: Box::new(|_| panic!("not initialized")),
            encode: Box::new(|_| panic!("not initialized")),
            update: Box::new(|_| panic!("not initialized")),
        }
    }
    /// removes a page from the head, decrement head seq
    /// PopHead
    pub fn get(&mut self) -> Option<Pointer> {
        match self.pop_head() {
            (Some(ptr), Some(head)) => {
                debug!(%ptr, "retrieving from freelist with head: ");
                self.append(head).unwrap();
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

    /// flPop
    fn pop_head(&mut self) -> (Option<Pointer>, Option<Pointer>) {
        if self.head_seq == self.max_seq {
            // no free page available
            return (None, None);
        }
        // let node = (self.decode)(self.head_page.unwrap()); // loading free list node
        // let ptr = node.get_ptr(seq_to_idx(self.head_seq));
        let ptr = self.get_ptr(self.head_page.unwrap(), seq_to_idx(self.head_seq));
        debug!(
            "getting ptr {} from head at {}",
            ptr,
            self.head_page.unwrap()
        );
        self.head_seq += 1;
        // in case the head page is empty we reuse it
        if seq_to_idx(self.head_seq) == 0 {
            let head = self.head_page.unwrap();
            // self.head_page = Some(node.get_next());
            self.head_page = Some(self.get_next(self.head_page.unwrap()));
            return (Some(ptr), Some(head));
        }
        (Some(ptr), None)
    }

    /// add a page to the tail increment tail seq
    /// PushTail
    pub fn append(&mut self, ptr: Pointer) -> Result<(), FLError> {
        // updates tail page, by getting a reference to the buffer if its already in there
        // updating appending the pointer
        debug!("appending {} to free list...", ptr);
        assert!(self.tail_page.is_some());
        self.set_ptr_buf(self.tail_page.unwrap(), ptr, seq_to_idx(self.tail_seq));
        self.tail_seq += 1;
        // allocating new node if the the node is full
        if seq_to_idx(self.tail_seq) == 0 {
            debug!("tail page full...");
            match self.pop_head() {
                // head page is empty
                (None, None) => {
                    debug!("head node empty!");
                    let new_node = (self.encode)(FLNode::new()); // this stays as encode
                    self.set_next(self.tail_page.unwrap(), new_node);
                    self.tail_page = Some(new_node);
                }
                // setting new page
                (Some(next), None) => {
                    debug!("got page from head...");
                    assert_ne!(next.0, 0);
                    self.set_next(self.tail_page.unwrap(), next);
                    self.tail_page = Some(next);
                }
                // getting the last item of the head node and the head node itself
                (Some(next), Some(head)) => {
                    debug!("got last ptr and head!.");
                    assert_ne!(next.0, 0);
                    assert_ne!(head.0, 0);
                    // sets current tail next to new page
                    self.set_next(self.tail_page.unwrap(), next);
                    // moves tail to new empty page
                    self.tail_page = Some(next);
                    // appending the empty head
                    self.set_ptr_buf(self.tail_page.unwrap(), head, 0);
                    self.tail_seq += 1; // accounting for re-added head
                }
                _ => unreachable!(),
            }
        }
        Ok(())
    }

    pub fn set_max_seq(&mut self) {
        self.max_seq = self.tail_seq
    }
    /// safety function to call update()
    /// gets pointer from idx
    fn get_ptr(&self, node: Pointer, idx: u16) -> Pointer {
        // SAFETY: see callback at the top
        unsafe {
            let node_ptr = (self.update)(node);
            (*node_ptr).get_ptr(idx)
        }
    }

    /// safety function to call update()
    /// sets ptr at idx for free list node
    fn set_ptr_buf(&self, node: Pointer, ptr: Pointer, idx: u16) {
        // SAFETY: see callback at the top
        unsafe {
            let node_ptr = (self.update)(node);
            (*node_ptr).set_ptr(idx, ptr);
        }
    }

    /// safety function to call update()
    /// gets next ptr from free list node
    fn get_next(&self, node: Pointer) -> Pointer {
        // SAFETY: see callback at the top
        unsafe {
            let node_ptr = (self.update)(node);
            (*node_ptr).get_next()
        }
    }

    /// safety function to call update()
    /// sets next ptr for free list node
    fn set_next(&self, node: Pointer, ptr: Pointer) {
        // SAFETY: see callback at the top
        unsafe {
            let node_ptr = (self.update)(node);
            (*node_ptr).set_next(ptr);
        }
    }

    // retrieves sorted list of all pointers inside freelist
    // does not interact with the buffer and should be called after the database has been written down
    pub fn collect_ptr(&self) -> Vec<Pointer> {
        let mut list: Vec<Pointer> = vec![];
        let mut head = self.head_seq;
        let max = self.max_seq;
        let mut node = (self.decode)(self.head_page.unwrap());
        while head < max {
            list.push(node.get_ptr(seq_to_idx(head)));
            head += 1;
            if seq_to_idx(head) == 0 {
                node = (self.decode)(node.get_next());
            }
        }
        list.sort();
        list
    }
}

// converts seq to idx
fn seq_to_idx(seq: usize) -> u16 {
    seq as u16 % FREE_LIST_CAP as u16
}

const FREE_LIST_NEXT: usize = 8;
const FREE_LIST_CAP: usize = (PAGE_SIZE - FREE_LIST_NEXT) / 8;

// -------Free List Node-------
// | next | pointers | unused |
// |  8B  |   n*8B   |   ...  |

#[derive(Debug)]
pub struct FLNode(Box<[u8; PAGE_SIZE]>);

impl FLNode {
    pub fn new() -> Self {
        FLNode(Box::new([0u8; PAGE_SIZE]))
    }

    fn get_next(&self) -> Pointer {
        debug!("getting next");
        read_pointer(&self, 0).expect("reading next ptr failed")
    }

    fn set_next(&mut self, ptr: Pointer) {
        debug!(ptr = ?ptr, "setting next");
        write_pointer(self, 0, ptr).unwrap()
    }

    fn get_ptr(&self, idx: u16) -> Pointer {
        debug!(idx, "getting pointer from free list node");
        read_pointer(self, FREE_LIST_NEXT + (FREE_LIST_NEXT * idx as usize))
            .expect("reading ptr failed")
    }

    fn set_ptr(&mut self, idx: u16, ptr: Pointer) {
        debug!(idx, ptr = ?ptr, "free list node: setting pointer");
        write_pointer(self, FREE_LIST_NEXT + (PTR_SIZE * idx as usize), ptr).unwrap();
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
