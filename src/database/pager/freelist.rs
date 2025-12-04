use std::fmt::Debug;
use std::ops::{Deref, DerefMut};

use tracing::debug;

use crate::database::types::Node;
use crate::database::{
    errors::FLError,
    helper::{read_pointer, write_pointer},
    node::TreeNode,
    types::{PAGE_SIZE, PTR_SIZE, Pointer},
};

pub(crate) struct FreeList {
    head_page: Option<Pointer>,
    head_seq: usize,
    tail_page: Option<Pointer>,
    tail_seq: usize,
    // maximum amount of items in the list
    max_seq: usize,
    //
    // callbacks
    // reads page, get
    pub decode: Box<dyn Fn(Pointer) -> FLNode>,
    // appends page, set
    pub encode: Box<dyn FnMut(FLNode) -> Pointer>,
    // returns a reference to a node in updates buffer
    pub update: Box<dyn Fn(Pointer) -> FLNode>,
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
                self.append(head).unwrap();
                Some(ptr)
            }
            (Some(ptr), None) => Some(ptr),
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
        let node = (self.decode)(self.head_page.unwrap());
        let ptr = read_pointer(&node, seq_to_idx(self.head_seq)).unwrap();
        self.head_seq += 1;
        // in case the head page is empty we reuse it
        if seq_to_idx(self.head_seq) == 0 {
            let head = self.head_page.unwrap();
            self.head_page = Some(node.get_next());
            return (Some(ptr), Some(head));
        }
        (Some(ptr), None)
    }

    /// add a page to the tail increment tail seq
    /// PushTail
    pub fn append(&mut self, ptr: Pointer) -> Result<(), FLError> {
        // updates tail page, by getting a reference to the buffer if its already in there
        // updating appending the pointer
        let mut cur_tail = (self.update)(self.tail_page.unwrap()); // TODO: check for none?
        cur_tail.set_ptr(seq_to_idx(self.tail_seq) as u16, ptr);
        self.tail_seq += 1;
        // allocating new node if the the node is full
        if seq_to_idx(self.tail_seq) == 0 {
            match self.pop_head() {
                // head page is empty
                (None, None) => {
                    let new_node = (self.encode)(FLNode::new());
                    cur_tail.set_next(new_node);
                    self.tail_page = Some(new_node);
                }
                // setting new page
                (Some(next), None) => {
                    cur_tail.set_next(next);
                    self.tail_page = Some(next);
                }
                // getting the last item of the head node and the head node iteself
                // appending the empty head as well
                (Some(next), Some(head)) => {
                    cur_tail.set_next(next);
                    let mut node = (self.update)(next);
                    node.set_ptr(0, head);
                    (self.encode)(node);
                    self.tail_page = Some(next);
                    self.tail_seq += 1; // accounting for re-added head
                }
                _ => unreachable!(),
            }
        }
        (self.encode)(cur_tail);
        Ok(())
    }

    pub fn set_max_seq(&mut self) {
        self.max_seq = self.tail_seq
    }
}

// converts seq to idx
fn seq_to_idx(seq: usize) -> usize {
    seq as usize % FREE_LIST_CAP
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

    // removes a pointer from the free list
    fn get_ptr(&mut self, idx: u16) -> Pointer {
        debug!(idx, "getting pointer");
        read_pointer(self, FREE_LIST_NEXT * idx as usize).expect("reading ptr failed")
    }

    // adds a pointer to the free list
    fn set_ptr(&mut self, idx: u16, ptr: Pointer) {
        debug!(idx, ptr = ?ptr, "setting pointer");
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
