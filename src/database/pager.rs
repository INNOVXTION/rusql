use rustix::fs::{self, Mode, OFlags};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Display;
use std::fs::File;
use std::hash::Hash;
use std::os::fd::OwnedFd;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{LazyLock, Mutex, OnceLock, RwLock};

use tracing::{debug, error};

use crate::database::node::Node;
use crate::database::tree::BTree;
use crate::database::types::PAGE_SIZE;
use crate::errors::{Error, PagerError};

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

pub static GLOBAL_PAGER: LazyLock<Mutex<Box<dyn Pager>>> =
    LazyLock::new(|| Mutex::new(Box::new(MemoryPager::new())));

thread_local! {
    pub static FREELIST: RefCell<Vec<u64>> = RefCell::new(Vec::from_iter(1..100));
    pub static PAGER: RefCell<HashMap<u64, Node>> = RefCell::new(HashMap::new());
}

struct MemoryPager {
    freelist: Vec<u64>,
    pages: HashMap<u64, Node>,
}

impl MemoryPager {
    fn new() -> Self {
        MemoryPager {
            freelist: Vec::from_iter(1..100),
            pages: HashMap::new(),
        }
    }
}

impl Pager for MemoryPager {
    fn encode(&mut self, node: Node) -> Pointer {
        if node.get_nkeys() > PAGE_SIZE as u16 {
            panic!("trying to encode node exceeding page size");
        }
        Pointer(self.freelist.pop().expect("no free page available"))
    }

    fn decode(&self, ptr: Pointer) -> Node {
        self.pages.get(&ptr.0).expect("couldnt get() page").clone()
    }

    fn delete(&mut self, ptr: Pointer) {
        self.freelist.push(ptr.0);
        self.pages
            .remove(&ptr.0)
            .expect("couldnt remove() page number");
    }
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

pub trait Pager: Send + Sync {
    fn encode(&mut self, node: Node) -> Pointer;
    fn decode(&self, ptr: Pointer) -> Node;
    fn delete(&mut self, ptr: Pointer);
}

#[allow(dead_code)]
pub struct KVStore {
    path: &'static str,
    database: OwnedFd,
    tree: BTree,
}

#[allow(dead_code)]
impl KVStore {
    fn open() -> Self {
        todo!()
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.tree.search(key)
    }

    pub fn insert(&mut self, key: &str, val: &str) -> Result<(), Error> {
        self.tree.insert(key, val)
    }

    pub fn delete(&mut self, key: &str) -> Result<(), Error> {
        self.tree.delete(key)
    }

    fn update_file(&mut self) -> Result<(), Error> {
        // write new node
        // sync call
        // update root
        // sync call
        todo!()
    }

    fn update_root(&mut self) -> Result<(), Error> {
        todo!()
    }
}

fn create_file_sync(file: &str) -> Result<OwnedFd, PagerError> {
    let path = PathBuf::from_str(file).unwrap();
    if let None = path.file_name() {
        error!("invalid file name");
        return Err(PagerError::FileNameError);
    }
    if let Some(parent) = path.parent() {
        debug!("creating parent directory...");
        std::fs::DirBuilder::new()
            .recursive(true)
            .create(parent)
            .expect("error when creating directory");
    } // none return can panic on dirfd open call
    debug!("opening directory fd");
    let flags = OFlags::DIRECTORY | OFlags::RDONLY; // only return directory, read only
    let mode = Mode::RUSR | Mode::WUSR | Mode::RGRP | Mode::ROTH; // owner read/write, group read, others read.
    let dirfd = fs::open(path.parent().unwrap(), flags, mode)?;
    debug!("opening or creating file");
    let flags = OFlags::RDWR | OFlags::CREATE;

    let fd = fs::openat(&dirfd, path.file_name().unwrap(), flags, mode)?;
    // fsync directory
    fs::fsync(dirfd)?;
    Ok(fd)
}

fn mmpa(fs: OwnedFd, offest: i64, length: u32) -> Result<Vec<u8>, PagerError> {
    todo!()
}

// // retrieve page content from page number
// pub fn decode(&self, page_number: Pointer) -> Result<Node, PagerError> {
//     let mut file = std::fs::File::open("database.rdb")?;
//     let mut new_page = Node::new();
//     file.seek(io::SeekFrom::Start(PAGE_SIZE as u64 * page_number.0))?;
//     file.read_exact(&mut *new_page.0)?;
//     Ok(new_page)
// }
