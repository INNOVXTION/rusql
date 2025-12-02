use std::ops::{Deref, DerefMut};

use tracing::debug;

use crate::database::{
    errors::FLError,
    helper::{read_pointer, write_pointer, write_u16},
    types::{PAGE_SIZE, PTR_SIZE, Pointer},
};

struct FreeList {
    head_page: Option<Pointer>,
    head_seq: usize,
    tail_page: Option<Pointer>,
    tail_seq: usize,
    max_seq: usize,                              // maximum amount of items in the list
    pub decode: Box<dyn Fn(&Pointer) -> FLNode>, // reads page, get
    pub encode: Box<dyn FnMut(FLNode) -> Pointer>, // appends page, set
    pub update: Box<dyn Fn(&Pointer) -> FLNode>, // reads page, new
}

impl FreeList {
    // removes a page from the head, decrement head seq
    // PopHead
    pub fn get(&mut self) -> Option<Pointer> {
        let (ptr, head) = self.pop().unwrap();
        todo!()
    }

    fn pop(&mut self) -> Option<(Pointer, Option<Pointer>)> {
        if self.head_seq == self.max_seq {
            return None;
        }
        let node = (self.decode)(&self.head_page.unwrap());
        self.head_seq += 1;
        if seq_to_idx(self.head_seq) == 0 {
            let next = node.get_next();
        }
        let ptr = read_pointer(&node, seq_to_idx(self.head_seq)).unwrap();
    }

    // add a page to the tail increment tail seq
    // PushTail
    pub fn set(ptr: Pointer) -> Result<(), FLError> {
        todo!()
    }

    fn push() {}
    fn set_max_seq(&mut self) {
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

pub struct FLNode(Box<[u8; PAGE_SIZE]>);

impl FLNode {
    fn new() -> Self {
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
        write_u16(self, FREE_LIST_NEXT, idx as u16 - 1).unwrap();
        read_pointer(self, FREE_LIST_NEXT * idx as usize).expect("reading ptr failed")
    }

    // adds a pointer to the free list
    fn set_ptr(&mut self, idx: u16, ptr: Pointer) {
        debug!(idx, ptr = ?ptr, "setting pointer");
        write_pointer(self, FREE_LIST_NEXT + (PTR_SIZE * idx as usize), ptr).unwrap();
        // increment idx
        write_u16(self, FREE_LIST_NEXT, idx + 1).unwrap();
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
