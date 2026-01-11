use std::cell::RefCell;
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};
use std::rc::{Rc, Weak};

use crate::database::codec::{NumDecode, NumEncode};
use crate::database::pager::diskpager::{GCCallbacks, NodeFlag, Pager};
use crate::database::types::{FREE_PAGE, Node, VER_SIZE};
use crate::database::{
    btree::TreeNode,
    errors::FLError,
    types::{PAGE_SIZE, PTR_SIZE, Pointer},
};
use tracing::debug;

pub(crate) struct FreeList<P: GCCallbacks> {
    pub pager: Weak<P>,

    head_page: Option<Pointer>,
    head_seq: usize,
    tail_page: Option<Pointer>,
    tail_seq: usize,

    pub max_seq: usize, // maximum amount of items in the list

    cur_ver: u64, // reflects the current pager
    max_ver: u64, // version permitted to give out, oldest version in diskpager.ongoing
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

pub(crate) struct FLConfig {
    pub head_page: Option<Pointer>,
    pub head_seq: usize,
    pub tail_page: Option<Pointer>,
    pub tail_seq: usize,

    pub cur_ver: u64,
    pub max_ver: u64,
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

impl<P: GCCallbacks> GC for FreeList<P> {
    /// removes a page from the head, decrement head seq
    fn get(&mut self) -> Option<Pointer> {
        match self.pop_head() {
            (Some(ptr), Some(head)) => {
                debug!(%ptr, "retrieving from freelist with head: ");
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
        debug!("appending {} to free list...", ptr);
        assert!(self.tail_page.is_some());

        // updates tail page, by getting a reference to the buffer if its already in there
        // updating appending the pointer
        self.update_set_ptr(
            self.tail_page.unwrap(),
            ptr,
            version,
            seq_to_idx(self.tail_seq),
        );
        self.tail_seq += 1;

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
            return None;
        }

        let mut list: Vec<Pointer> = vec![];
        let mut head = self.head_seq;

        let mut head_page = self.head_page?;
        let tail_page = self.tail_page?;

        let max = self.max_seq;
        let mut node = self.decode(self.head_page.unwrap());

        loop {
            if head_page == tail_page && head == max {
                // both pointer meet on the same page = free list is empty
                break;
            }

            list.push(node.borrow().as_fl().get_ptr(seq_to_idx(head)).0);
            head += 1;

            if seq_to_idx(head) == 0 {
                head_page = node.borrow().as_fl().get_next();
                node = self.decode(head_page);
                head = 0;
            }
        }
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
        }
    }

    fn set_config(&mut self, flc: &FLConfig) {
        self.head_page = flc.head_page;
        self.head_seq = flc.head_seq;
        self.tail_page = flc.tail_page;
        self.tail_seq = flc.tail_seq;
        self.cur_ver = flc.cur_ver;
        self.max_ver = flc.max_ver;
    }

    /// increments the max_seq for next transaction cycle
    fn set_max_seq(&mut self) {
        self.max_seq = self.tail_seq
    }

    fn set_max_ver(&mut self, version: u64) {
        self.max_ver = version
    }

    fn set_cur_ver(&mut self, version: u64) {
        self.cur_ver = version
    }
}

impl<P: GCCallbacks> FreeList<P> {
    // callbacks

    /// reads page, gets page, removes from buffer if available
    fn decode(&self, ptr: Pointer) -> Rc<RefCell<Node>> {
        let strong = self.pager.upgrade().unwrap();
        strong.page_read(ptr, NodeFlag::Freelist)
    }

    /// appends page to disk, doesnt make a buffer check
    fn encode(&self, node: FLNode) -> Pointer {
        let strong = self.pager.upgrade().unwrap();
        strong.encode(Node::Freelist(node))
    }

    /// returns ptr to node inside the allocation buffer
    fn update(&self, ptr: Pointer) -> Rc<RefCell<Node>> {
        let strong = self.pager.upgrade().unwrap();
        strong.update(ptr)
    }

    /// new uninitialized
    pub fn new(pager: Weak<P>) -> Self {
        FreeList {
            head_page: None,
            head_seq: 0,
            tail_page: None,
            tail_seq: 0,
            max_seq: 0,
            pager: pager,
            cur_ver: 0,
            max_ver: 0,
        }
    }

    /// flPop
    fn pop_head(&mut self) -> (Option<Pointer>, Option<Pointer>) {
        // experimental
        // head seq cant overtake max seq when on the same page as tail
        if self.is_empty() {
            // no free page available
            return (None, None);
        }

        let ptr = self.update_get_ptr(self.head_page.unwrap(), seq_to_idx(self.head_seq));

        // is the version retrieved newer than max_ver
        if ptr.1 > self.max_ver {
            return (None, None);
        }

        self.head_seq += 1;
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
            return (Some(ptr.0), Some(head));
        }
        (Some(ptr.0), None)
    }

    /// gets pointer from idx
    fn update_get_ptr(&self, node: Pointer, idx: u16) -> (Pointer, u64) {
        let r = self.update(node);
        let mut r_mut = r.borrow_mut();
        let node_ptr = r_mut.as_fl_mut();
        node_ptr.get_ptr(idx)
    }

    /// sets ptr at idx for free list node
    fn update_set_ptr(&self, node: Pointer, ptr: Pointer, version: u64, idx: u16) {
        let r = self.update(node);
        let mut r_mut = r.borrow_mut();
        let node_ptr = r_mut.as_fl_mut();
        node_ptr.set_ptr(idx, ptr, version);
    }

    /// gets next ptr from free list node
    fn update_get_next(&self, node: Pointer) -> Pointer {
        let r = self.update(node);
        let mut r_mut = r.borrow_mut();
        let node_ptr = r_mut.as_fl_mut();
        node_ptr.get_next()
    }

    /// sets next ptr for free list node
    fn update_set_next(&self, node: Pointer, ptr: Pointer) {
        let r = self.update(node);
        let mut r_mut = r.borrow_mut();
        let node_ptr = r_mut.as_fl_mut();
        node_ptr.set_next(ptr);
    }

    fn is_empty(&self) -> bool {
        if self.head_page == self.tail_page && self.head_seq == self.max_seq {
            true
        } else {
            false
        }
    }
}

// converts seq to idx
fn seq_to_idx(seq: usize) -> u16 {
    seq as u16 % FREE_LIST_CAP as u16
}

const FREE_LIST_NEXT: usize = 8;
const FREE_LIST_CAP: usize = (PAGE_SIZE - FREE_LIST_NEXT) / (PTR_SIZE + VER_SIZE);

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

        (&self[0..]).read_u64().into()
    }

    fn set_next(&mut self, ptr: Pointer) {
        debug!(ptr = ?ptr, "setting next");

        (&mut self[0..]).write_u64(ptr.get());
    }

    fn get_ptr(&self, idx: u16) -> (Pointer, u64) {
        debug!(idx, "getting pointer from free list node");
        let offset = FREE_LIST_NEXT + ((PTR_SIZE + VER_SIZE) * idx as usize);
        let mut slice = &self[offset..];

        let ptr = Pointer::from(slice.read_u64());
        let ver = slice.read_u64();

        (ptr, ver)
    }

    fn set_ptr(&mut self, idx: u16, ptr: Pointer, version: u64) {
        debug!(idx, ptr = ?ptr, "free list node: setting pointer");
        let offset = FREE_LIST_NEXT + ((PTR_SIZE + VER_SIZE) * idx as usize);

        (&mut self[offset..])
            .write_u64(ptr.get())
            .write_u64(version);
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

impl<P: Pager + GCCallbacks> Debug for FreeList<P> {
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
    use crate::database::types::Pointer;

    #[test]
    fn modulo() {
        let x = 7;
        assert_eq!(x % 10, 7);
        let x = 0;
        assert_eq!(x % 10, 0);
        let x = 1;
        assert_eq!(x % 10, 1);

        let p1 = Some(Pointer::from(1));
        let p2 = Some(Pointer::from(2));

        assert_ne!(p1, p2)
    }
}
