use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::OsString;
use std::fmt::Display;
use std::fs::File;

use crate::database::node::Node;
use crate::database::tree::BTree;
use crate::database::types::PAGE_SIZE;

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Eq, Hash)]
pub struct Pointer(u64);

impl From<u64> for Pointer {
    fn from(value: u64) -> Self {
        Pointer(value)
    }
}

impl Pointer {
    pub fn to_slice(self) -> [u8; 8] {
        self.0.to_le_bytes()
    }
}

impl Display for Pointer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

thread_local! {
    pub static FREELIST: RefCell<Vec<u64>> = RefCell::new(Vec::from_iter(1..100));
    pub static PAGER: RefCell<HashMap<u64, Node>> = RefCell::new(HashMap::new());
}

/// loads page into memoory as a node
pub(crate) fn node_get(ptr: Pointer) -> Node {
    PAGER.with_borrow_mut(|x| x.get(&ptr.0).expect("couldnt get() page").clone())
}

/// finds a free spot to write node to
pub(crate) fn node_encode(node: Node) -> Pointer {
    if node.get_nkeys() > PAGE_SIZE as u16 {
        panic!("trying to encode node exceeding page size");
    }
    let free_page = FREELIST.with_borrow_mut(|v| v.pop().expect("no free page available"));
    PAGER.with_borrow_mut(|p| p.insert(free_page, node));
    Pointer(free_page)
}

/// deallocate page
pub(crate) fn node_dealloc(ptr: Pointer) {
    FREELIST.with_borrow_mut(|v| v.push(ptr.0));
    PAGER
        .with_borrow_mut(|x| x.remove(&ptr.0))
        .expect("couldnt remove() page number");
}

pub trait Paging {
    fn new() -> Self;
    fn encode(&mut self, node: Node) -> Pointer;
    fn decode(&self, ptr: Pointer) -> Node;
    fn delete(&mut self, ptr: Pointer);
}

#[allow(dead_code)]
pub struct Pager {
    path: OsString,
    database: File,
    tree: BTree,
}

#[allow(dead_code)]
impl Paging for Pager {
    fn new() -> Self {
        todo!()
    }

    fn encode(&mut self, node: Node) -> Pointer {
        todo!()
    }

    fn decode(&self, ptr: Pointer) -> Node {
        todo!()
    }

    fn delete(&mut self, ptr: Pointer) {
        todo!()
    }
}

// // retrieve page content from page number
// pub fn decode(&self, page_number: Pointer) -> Result<Node, PagerError> {
//     let mut file = std::fs::File::open("database.rdb")?;
//     let mut new_page = Node::new();
//     file.seek(io::SeekFrom::Start(PAGE_SIZE as u64 * page_number.0))?;
//     file.read_exact(&mut *new_page.0)?;
//     Ok(new_page)
// }
