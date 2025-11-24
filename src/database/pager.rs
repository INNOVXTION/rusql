use core::ffi::c_void;
use rustix::fs::{self, Mode, OFlags};
use rustix::mm::{MapFlags, ProtFlags};
use std::collections::HashMap;
use std::io::IoSlice;
use std::os::fd::OwnedFd;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{LazyLock, Mutex, OnceLock};

use tracing::{debug, error};

use crate::database::node::Node;
use crate::database::tree::BTree;
use crate::database::types::{PAGE_SIZE, Pager, Pointer};
use crate::errors::{Error, PagerError};

// change the pager with : *GLOBAL_PAGER.lock().unwrap() = Box::new(DiskPager::new());
pub static GLOBAL_PAGER: LazyLock<Mutex<Box<dyn Pager>>> =
    LazyLock::new(|| Mutex::new(Box::new(MemoryPager::new())));

pub static GLOBAL_PAGERTWO: OnceLock<Mutex<Box<dyn Pager>>> = OnceLock::new();

pub enum PagerType {
    Memory,
    Disk,
}

pub fn init_pager(mode: PagerType) {
    match mode {
        PagerType::Disk => {
            GLOBAL_PAGERTWO.set(Mutex::new(Box::new(DiskPager::open())));
        }
        PagerType::Memory => {
            GLOBAL_PAGERTWO.set(Mutex::new(Box::new(MemoryPager::new())));
        }
    };
    ()
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

pub struct DiskPager<'a> {
    path: &'static str,
    database: OwnedFd,
    tree: BTree,
    mmap: Mmap<'a>,
    page: Page,
}

struct Page {
    flushed: u64,    // database size in number of pages
    temp: Vec<Node>, // newly allocated pages
}

struct Mmap<'a> {
    total: usize,           // mmap size, can be larger than the file size
    chunks: Vec<Chunk<'a>>, // multiple mmaps, can be non-continuous
}

struct Chunk<'a> {
    data: &'a [u8],
    len: usize,
}

impl<'a> DiskPager<'a> {
    fn open() -> Self {
        todo!()
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.tree.search(key)
    }

    pub fn set(&mut self, key: &str, val: &str) -> Result<(), Error> {
        self.tree.insert(key, val)?;
        self.file_update().map_err(|e| {
            error!("pager error {}", e);
            Error::PagerError(e)
        })?;
        Ok(())
    }

    pub fn delete(&mut self, key: &str) -> Result<(), Error> {
        self.tree.delete(key)?;
        self.file_update().map_err(|e| {
            error!("pager error {}", e);
            Error::PagerError(e)
        })?;
        Ok(())
    }

    fn file_update(&mut self) -> Result<(), PagerError> {
        // write new node
        self.page_write()?;
        // sync call
        rustix::fs::fsync(&self.database)?;
        // update root
        self.update_root()?;
        // sync call
        rustix::fs::fsync(&self.database)?;
        Ok(())
    }
    // writePages
    fn page_write(&mut self) -> Result<(), PagerError> {
        // extend the mmap if needed
        let size = (self.page.flushed as usize + self.page.temp.len()) * PAGE_SIZE;
        mmap_extend(self, size)
            .map_err(|e| {
                error!("Error when extending mmap: {}, error {}", size, e);
            })
            .unwrap();
        // write data pages to the file
        let offset = self.page.flushed as usize * PAGE_SIZE;
        let buf: Vec<IoSlice> = self
            .page
            .temp
            .iter()
            .map(|node| rustix::io::IoSlice::new(&node.0))
            .collect();
        rustix::io::pwritev(&self.database, &buf[..], offset as u64)?;
        //discard in-memory data
        self.page.flushed += self.page.temp.len() as u64;
        self.page.temp.clear();
        Ok(())
    }

    fn update_root(&mut self) -> Result<(), PagerError> {
        todo!()
    }
}

impl<'a> Pager for DiskPager<'a> {
    // page read
    fn decode(&self, ptr: Pointer) -> Node {
        let mut start: u64 = 0;
        for chunk in self.mmap.chunks.iter() {
            let end = start + chunk.len as u64 / PAGE_SIZE as u64;
            if ptr.0 < end {
                let offset: usize = PAGE_SIZE * (ptr.0 as usize - start as usize);
                let mut node = Node::new();
                node.0
                    .copy_from_slice(&chunk.data[offset..offset + PAGE_SIZE]);
                return node;
            }
            start = end;
        }
        error!("bad pointer: {}", ptr.0);
        panic!()
    }

    // page append
    fn encode(&mut self, node: Node) -> Pointer {
        // ptr := db.page.flushed + uint64(len(db.page.temp)) // just append
        // db.page.temp = append(db.page.temp, node)
        // return ptr
        let ptr = Pointer(self.page.flushed + self.page.temp.len() as u64);
        self.page.temp.push(node);
        ptr
    }

    fn delete(&mut self, ptr: Pointer) {
        todo!()
    }
}

/// read-only
fn mmap_new<'a>(db: &DiskPager, length: usize, offset: u64) -> Result<Chunk<'a>, PagerError> {
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
        // SAFETY: non null, and page aligned pointer from mmap()
        data: unsafe { std::slice::from_raw_parts(ptr as *const u8, length) },
        len: length,
    })
}

fn mmap_extend(db: &mut DiskPager, size: usize) -> Result<(), PagerError> {
    if size <= db.mmap.total {
        return Ok(()); // enough range
    };
    let mut alloc = usize::max(db.mmap.total, 64 << 20); // double the current address space
    while db.mmap.total + alloc < size {
        alloc *= 2;
    }
    let chunk = mmap_new(db, alloc, db.mmap.total as u64).map_err(|e| {
        error!("error when extending mmap, size: {size}");
        e
    })?;
    db.mmap.total += alloc;
    db.mmap.chunks.push(chunk);
    Ok(())
}

// impl Chunk {
//     fn as_slice(&self) -> &[u8] {
//         // SAFETY: non null, and page aligned pointer from mmap()
//         unsafe { std::slice::from_raw_parts(self.data, self.len) }
//     }
// }

impl<'a> Drop for Chunk<'a> {
    fn drop(&mut self) {
        unsafe {
            if let Err(e) = rustix::mm::munmap(self.data.as_ptr() as *mut c_void, self.len) {
                error!("error when dropping with mumap {}", e);
            }
        };
    }
}
// impl Drop for Chunk {
//     fn drop(&mut self) {
//         unsafe {
//             if let Err(e) = rustix::mm::munmap(self.data as *mut c_void, self.len) {
//                 error!("error when dropping with mumap {}", e);
//             }
//         };
//     }
// }

unsafe impl<'a> Send for Chunk<'a> {}
unsafe impl<'a> Sync for Chunk<'a> {}

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
