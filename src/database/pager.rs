use core::ffi::c_void;
use rustix::fs::{self, Mode, OFlags};
use rustix::mm::{MapFlags, ProtFlags};
use std::collections::HashMap;
use std::fmt::Display;
use std::hash::Hash;
use std::os::fd::OwnedFd;
use std::path::PathBuf;
use std::ptr;
use std::str::FromStr;
use std::sync::{LazyLock, Mutex, RwLock};

use tracing::{debug, error};

use crate::database::node::Node;
use crate::database::tree::BTree;
use crate::database::types::PAGE_SIZE;
use crate::errors::{Error, PagerError};

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Eq, Hash)]
pub(crate) struct Pointer(u64);

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

enum PagerType {
    Memory,
    Disk,
}

// change the pager with : *GLOBAL_PAGER.lock().unwrap() = Box::new(DiskPager::new());
pub static GLOBAL_PAGER: LazyLock<Mutex<Box<dyn Pager>>> =
    LazyLock::new(|| Mutex::new(Box::new(MemoryPager::new())));

pub fn init_pager(mode: PagerType) {
    match mode {
        PagerType::Disk => {
            *GLOBAL_PAGER.lock().unwrap() = Box::new(DiskPager::new());
        }
        _ => (),
    }
}

struct MemoryPager {
    freelist: Vec<u64>,
    pages: HashMap<u64, Node>,
}

impl MemoryPager {
    fn new() -> Self {
        MemoryPager {
            freelist: Vec::from_iter((1..=100).rev()),
            pages: HashMap::new(),
        }
    }
}

impl Pager for MemoryPager {
    fn decode(&self, ptr: Pointer) -> Node {
        self.pages
            .get(&ptr.0)
            .unwrap_or_else(|| {
                error!("couldnt retrieve page at ptr {}", ptr);
                panic!("page decode error")
            })
            .clone()
    }

    fn encode(&mut self, node: Node) -> Pointer {
        if node.get_nkeys() > PAGE_SIZE as u16 {
            panic!("trying to encode node exceeding page size");
        }
        let free_page = self.freelist.pop().expect("no free page available");
        debug!("encoding node at ptr {}", free_page);
        self.pages.insert(free_page, node);
        Pointer(free_page)
    }

    fn delete(&mut self, ptr: Pointer) {
        debug!("deleting node at ptr {}", ptr.0);
        self.freelist.push(ptr.0);
        self.pages
            .remove(&ptr.0)
            .expect("couldnt remove() page number");
    }
}

// thread_local! {
//     pub static FREELIST: RefCell<Vec<u64>> = RefCell::new(Vec::from_iter(1..100));
//     pub static PAGER: RefCell<HashMap<u64, Node>> = RefCell::new(HashMap::new());
// }

// /// loads page into memoory as a node
// pub(crate) fn node_get(ptr: Pointer) -> Node {
//     PAGER.with_borrow_mut(|x| x.get(&ptr.0).expect("couldnt get() page").clone())
// }

// /// finds a free spot to write node to
// pub(crate) fn node_encode(node: Node) -> Pointer {
//     if node.get_nkeys() > PAGE_SIZE as u16 {
//         panic!("trying to encode node exceeding page size");
//     }
//     let free_page = FREELIST.with_borrow_mut(|v| v.pop().expect("no free page available"));
//     PAGER.with_borrow_mut(|p| p.insert(free_page, node));
//     Pointer(free_page)
// }

// /// deallocate page
// pub(crate) fn node_dealloc(ptr: Pointer) {
//     FREELIST.with_borrow_mut(|v| v.push(ptr.0));
//     PAGER
//         .with_borrow_mut(|x| x.remove(&ptr.0))
//         .expect("couldnt remove() page number");
// }

pub trait Pager: Send + Sync {
    fn encode(&mut self, node: Node) -> Pointer;
    fn decode(&self, ptr: Pointer) -> Node;
    fn delete(&mut self, ptr: Pointer);
}

struct DiskPager {
    mmap: Mmap,
    page: Page,
}

struct Page {
    flushed: u64,
    temp: RwLock<Vec<Chunk>>,
}

struct Mmap {
    total: u32,
    chunks: RwLock<Vec<Chunk>>,
}

impl DiskPager {
    fn new() -> Self {
        todo!()
    }
}

impl Pager for DiskPager {
    fn decode(&self, ptr: Pointer) -> Node {
        let mut start: u64 = 0;
        let data = self.mmap.chunks.read().unwrap();
        for chunk in data.iter() {
            let end = start + chunk.len as u64 / PAGE_SIZE as u64;
            if ptr.0 < end {
                let offset: usize = PAGE_SIZE * (ptr.0 as usize - start as usize);
                let mut node = Node::new();
                node.0
                    .copy_from_slice(&chunk.as_slice()[offset..offset + PAGE_SIZE]);
                return node;
            }
            start = end;
        }
        error!("bad pointer: {}", ptr.0);
        panic!()
    }

    fn encode(&mut self, node: Node) -> Pointer {
        todo!()
    }

    fn delete(&mut self, ptr: Pointer) {
        todo!()
    }
}

pub struct KVStore {
    path: &'static str,
    database: OwnedFd,
    tree: BTree,
    pager: DiskPager,
}

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

fn mmap_new(db: &KVStore, offset: u64, length: usize) -> Result<Chunk, PagerError> {
    if rustix::param::page_size() != PAGE_SIZE {
        return Err(PagerError::UnsupportedOS);
    };
    if offset % PAGE_SIZE as u64 != 0 {
        return Err(PagerError::UnalignedOffset(offset));
    };
    let ptr = unsafe {
        rustix::mm::mmap(
            std::ptr::null_mut(),
            length,
            ProtFlags::READ,
            MapFlags::SHARED,
            &db.database,
            offset,
        )
        .map_err(|e| PagerError::MMapError(e))?
    };
    Ok(Chunk {
        data: ptr as *const u8,
        len: length,
    })
}

fn mmap_extend(db: &mut KVStore, size: u32) -> Result<(), PagerError> {
    if size <= db.pager.mmap.total {
        return Ok(());
    };
    let mut alloc = u32::max(db.pager.mmap.total, 64 << 20);
    while db.pager.mmap.total + alloc < size {
        alloc *= 2;
    }
    let chunk = mmap_new(db, alloc as u64, db.pager.mmap.total as usize).map_err(|e| {
        error!("error when extending mmap, size: {size}");
        e
    })?;
    db.pager.mmap.total += alloc;
    db.pager.mmap.chunks.write().unwrap().push(chunk);
    Ok(())
}

struct Chunk {
    data: *const u8,
    len: usize,
}
impl Chunk {
    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data, self.len) }
    }
}

impl Drop for Chunk {
    fn drop(&mut self) {
        unsafe {
            if let Err(e) = rustix::mm::munmap(self.data as *mut c_void, self.len) {
                error!("error when dropping with mumap {}", e);
            }
        };
    }
}

unsafe impl Send for Chunk {}
unsafe impl Sync for Chunk {}

fn create_file_sync(file: &str) -> Result<OwnedFd, PagerError> {
    let path = PathBuf::from_str(file).unwrap();
    if let None = path.file_name() {
        error!("invalid file name");
        return Err(PagerError::FileNameError);
    }
    let parent = path.parent();
    if parent.is_some() && !parent.unwrap().is_dir() {
        debug!("creating parent directory...");
        std::fs::DirBuilder::new()
            .recursive(true)
            .create(parent.unwrap())
            .expect("error when creating directory");
    } // none return can panic on dirfd open call
    debug!("opening directory fd");
    let dirfd = fs::open(
        parent.unwrap(),
        OFlags::DIRECTORY | OFlags::RDONLY,
        Mode::RUSR | Mode::WUSR | Mode::RGRP | Mode::ROTH,
    )?;
    debug!("opening or creating file");
    let fd = fs::openat(
        &dirfd,
        path.file_name().unwrap(),
        OFlags::RDWR | OFlags::CREATE,
        Mode::RUSR | Mode::WUSR | Mode::RGRP | Mode::ROTH,
    )?;
    // fsync directory
    fs::fsync(dirfd)?;
    Ok(fd)
}

// // retrieve page content from page number
// pub fn decode(&self, page_number: Pointer) -> Result<Node, PagerError> {
//     let mut file = std::fs::File::open("database.rdb")?;
//     let mut new_page = Node::new();
//     file.seek(io::SeekFrom::Start(PAGE_SIZE as u64 * page_number.0))?;
//     file.read_exact(&mut *new_page.0)?;
//     Ok(new_page)
// }
