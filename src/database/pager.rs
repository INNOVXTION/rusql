use core::ffi::c_void;
use rustix::mm::{MapFlags, ProtFlags};
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::IoSlice;
use std::ops::{Deref, DerefMut};
use std::os::fd::OwnedFd;
use std::rc::{Rc, Weak};
use std::sync::{Mutex, OnceLock};

use tracing::{debug, error, info, instrument};

use crate::database::{
    errors::{Error, PagerError},
    helper::{create_file_sync, slice_to_pointer, write_pointer},
    node::Node,
    tree::BTree,
    types::*,
};

pub static GLOBAL_PAGER: OnceLock<Mutex<MemoryPager>> = OnceLock::new();

pub struct MemoryPager {
    freelist: Vec<u64>,
    pages: HashMap<u64, Node>,
}

impl MemoryPager {
    pub fn new() -> Self {
        MemoryPager {
            freelist: Vec::from_iter((1..=100).rev()),
            pages: HashMap::new(),
        }
    }
}

pub fn mempage_tree<'a>() -> BTree<'a> {
    GLOBAL_PAGER.set(Mutex::new(MemoryPager::new()));
    BTree {
        root_ptr: None,
        decode: Box::new(decode),
        encode: Box::new(encode),
        dealloc: Box::new(dealloc),
    }
}

/// callbacks for memory pager
fn decode(ptr: &Pointer) -> Node {
    let pager = GLOBAL_PAGER.get().unwrap().lock().unwrap();
    pager
        .pages
        .get(&ptr.0)
        .unwrap_or_else(|| {
            error!("couldnt retrieve page at ptr {}", ptr);
            panic!("page decode error")
        })
        .clone()
}

fn encode(node: Node) -> Pointer {
    if node.get_nkeys() > PAGE_SIZE as u16 {
        panic!("trying to encode node exceeding page size");
    }
    let mut pager = GLOBAL_PAGER.get().unwrap().lock().unwrap();
    let free_page = pager.freelist.pop().expect("no free page available");
    debug!("encoding node at ptr {}", free_page);
    pager.pages.insert(free_page, node);
    Pointer(free_page)
}

fn dealloc(ptr: Pointer) {
    debug!("deleting node at ptr {}", ptr.0);
    let mut pager = GLOBAL_PAGER.get().unwrap().lock().unwrap();
    pager.freelist.push(ptr.0);
    pager
        .pages
        .remove(&ptr.0)
        .expect("couldnt remove() page number");
}

#[derive(Debug)]
pub struct DiskPager<'a> {
    path: &'static str,
    database: OwnedFd,
    tree: BTree<'a>,
    mmap: Mmap<'a>,
    n_pages: u64,    // database size in number of pages
    temp: Vec<Node>, // newly allocated pages to be flushed
}

#[derive(Debug)]
struct Mmap<'a> {
    total: usize,           // mmap size, can be larger than the file size
    chunks: Vec<Chunk<'a>>, // multiple mmaps, can be non-continuous
}

#[derive(Debug)]
struct Chunk<'a> {
    data: &'a [u8],
    len: usize,
}

impl<'a> DiskPager<'a> {
    fn open(path: &'static str) -> Result<Rc<RefCell<DiskPager>>, Error> {
        let pager = Rc::new(RefCell::new(DiskPager {
            path,
            database: create_file_sync(path).map_err(|e| {
                error!("Error when opening database");
                Error::PagerError(e)
            })?,
            tree: BTree {
                root_ptr: None,
                decode: Box::new(|_| panic!("not initialized")),
                encode: Box::new(|_| panic!("not initialized")),
                dealloc: Box::new(|_| panic!("not initialized")),
            },
            mmap: Mmap {
                total: 0,
                chunks: vec![],
            },
            n_pages: 0,
            temp: vec![],
        }));
        let weak = Rc::downgrade(&pager);
        let decode = {
            let weak = Weak::clone(&weak);
            Box::new(move |ptr: &Pointer| {
                weak.upgrade().expect("pager dropped").borrow().decode(*ptr)
            })
        };
        let encode = {
            let weak = Weak::clone(&weak);
            Box::new(move |node: Node| {
                weak.upgrade()
                    .expect("pager dropped")
                    .borrow_mut()
                    .encode(node)
            })
        };
        let dealloc = {
            let weak = Weak::clone(&weak);
            Box::new(move |ptr: Pointer| {
                weak.upgrade()
                    .expect("pager dropped")
                    .borrow_mut()
                    .dealloc(ptr)
            })
        };
        pager.borrow_mut().tree.decode = decode;
        pager.borrow_mut().tree.encode = encode;
        pager.borrow_mut().tree.dealloc = dealloc;

        let fd_size = rustix::fs::fstat(&pager.borrow().database)
            .map_err(|e| {
                error!("Error when getting file size");
                Error::PagerError(PagerError::FDError(e))
            })?
            .st_size as u64;

        // initialize mmap
        let map_size = (fd_size * 2) as usize + PAGE_SIZE;
        pager.borrow_mut().mmap.chunks.push(
            mmap_new(&pager.borrow().database, map_size, 0).map_err(|e| {
                error!("Error when initalizing mmap");
                Error::PagerError(e)
            })?,
        );
        pager.borrow_mut().mmap.total = map_size;
        pager.borrow_mut().root_read(fd_size);

        Ok(pager)
    }

    #[instrument]
    pub fn get(&self, key: &str) -> Option<String> {
        self.tree.search(key)
    }

    #[instrument]
    pub fn set(&mut self, key: &str, val: &str) -> Result<(), Error> {
        self.tree.insert(key, val)?;
        self.file_update().map_err(|e| {
            error!("pager error {}", e);
            Error::PagerError(e)
        })?;
        Ok(())
    }

    #[instrument]
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
        self.root_update()?;
        // sync call
        rustix::fs::fsync(&self.database)?;
        Ok(())
    }

    /// writePages, flushes the buffer
    fn page_write(&mut self) -> Result<(), PagerError> {
        // extend the mmap if needed
        // number of page plus current buffer
        let new_size = (self.n_pages as usize + self.temp.len()) * PAGE_SIZE; // amount of pages in bytes
        mmap_extend(self, new_size)
            .map_err(|e| {
                error!("Error when extending mmap: {}, error {}", new_size, e);
            })
            .unwrap();
        // write data pages to the file
        let offset = self.n_pages as usize * PAGE_SIZE;
        let buf: Vec<IoSlice> = self
            .temp
            .iter()
            .map(|node| rustix::io::IoSlice::new(&node))
            .collect();
        rustix::io::pwritev(&self.database, &buf, offset as u64)?;
        //discard in-memory data
        self.n_pages += self.temp.len() as u64;
        self.temp.clear();
        Ok(())
    }

    /// updates meta page with settings set at call time
    fn root_update(&self) -> Result<(), PagerError> {
        let r = rustix::io::pwrite(&self.database, &metapage_save(&self), 0)?;
        Ok(())
    }

    /// reads chunk and loads its data into memory, sets root_ptr and n_pages
    fn root_read(&mut self, file_size: u64) {
        if file_size == 0 {
            // empty file
            self.n_pages = 1;
            return;
        }
        metapage_load(self);
    }
    // callbacks
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

    // page append, loads node into buffer to be flushed later
    fn encode(&mut self, node: Node) -> Pointer {
        let ptr = Pointer(self.n_pages + self.temp.len() as u64);
        self.temp.push(node);
        ptr
    }

    fn dealloc(&mut self, ptr: Pointer) {
        todo!()
    }
}

/// read-only
fn mmap_new<'a>(fd: &OwnedFd, length: usize, offset: u64) -> Result<Chunk<'a>, PagerError> {
    debug!("new mmap: length {length}, offset {offset}");
    if rustix::param::page_size() != PAGE_SIZE {
        error!("OS page size doesnt work!");
        return Err(PagerError::UnsupportedOS);
    };
    if offset % PAGE_SIZE as u64 != 0 {
        error!("Invalid offset!");
        return Err(PagerError::UnalignedOffset(offset));
    };
    let ptr = unsafe {
        rustix::mm::mmap(
            std::ptr::null_mut() as *mut c_void,
            length,
            ProtFlags::READ,
            MapFlags::SHARED,
            fd,
            offset,
        )
        .map_err(|e| {
            error!("Error when calling mmap");
            PagerError::MMapError(e)
        })?
    };
    Ok(Chunk {
        // SAFETY: non null, and page aligned pointer from mmap()
        data: unsafe { std::slice::from_raw_parts(ptr as *const u8, length) },
        len: length,
    })
}

/// checks for sufficient space, exponentially extends the mmap
fn mmap_extend(db: &mut DiskPager, size: usize) -> Result<(), PagerError> {
    debug!("extending mmap: size {size}");
    if size <= db.mmap.total {
        return Ok(()); // enough range
    };
    let mut alloc = usize::max(db.mmap.total, 64 << 20); // double the current address space
    while db.mmap.total + alloc < size {
        alloc *= 2;
    }
    let chunk = mmap_new(&db.database, alloc, db.mmap.total as u64).map_err(|e| {
        error!("error when extending mmap, size: {size}");
        e
    })?;
    db.mmap.total += alloc;
    db.mmap.chunks.push(chunk);
    Ok(())
}

impl<'a> Drop for Chunk<'a> {
    fn drop(&mut self) {
        unsafe {
            if let Err(e) = rustix::mm::munmap(self.data.as_ptr() as *mut c_void, self.len) {
                error!("error when dropping with mumap {}", e);
            }
        };
    }
}

unsafe impl<'a> Send for Chunk<'a> {}
unsafe impl<'a> Sync for Chunk<'a> {}

struct MetaPage(Box<[u8; METAPAGE_SIZE]>);

impl MetaPage {
    fn new() -> Self {
        MetaPage(Box::new([0u8; METAPAGE_SIZE]))
    }
}

impl Deref for MetaPage {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}
impl DerefMut for MetaPage {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut *self.0
    }
}

//--------Meta Page Layout-------
// | sig | root_ptr | page_used |
// | 16B |    8B    |     8B    |

/// returns the metapage configured on current disk pager
fn metapage_save(pager: &DiskPager) -> MetaPage {
    let mut data = MetaPage::new();
    // write sig
    data[..SIG_SIZE].copy_from_slice(DB_SIG.as_bytes());
    // write root ptr
    write_pointer(
        &mut data,
        SIG_SIZE,
        pager.tree.root_ptr.or(Some(Pointer(0))).unwrap(),
    )
    .unwrap();
    // write n pages
    data[SIG_SIZE + PTR_SIZE..SIG_SIZE + (PTR_SIZE * 2)]
        .copy_from_slice(&pager.n_pages.to_le_bytes());
    data
}

/// reads chunk and sets diskpager root ptr and npages
fn metapage_load(pager: &mut DiskPager) {
    let data = pager.mmap.chunks[0].data;
    pager.tree.root_ptr = match slice_to_pointer(data, SIG_SIZE) {
        Ok(Pointer(0)) => None,
        Ok(n) => Some(n),
        Err(e) => {
            error!("Error when reading root ptr from meta page {}", e);
            panic!()
        }
    };
    pager.n_pages = u64::from_le_bytes(
        data[SIG_SIZE + PTR_SIZE..SIG_SIZE + (PTR_SIZE * 2)]
            .try_into()
            .unwrap(),
    );
    info!(
        "opening database: {}",
        str::from_utf8(&data[..SIG_SIZE]).unwrap()
    );
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::Once;
    use test_log::test;

    static INIT: Once = Once::new();

    // fn setup() {
    //     INIT.call_once(|| {
    //         init_pager(PagerType::Disk {
    //             path: "test-files/database.rdb",
    //         })
    //         .unwrap();
    //     });
    // }

    #[test]
    fn open_pager() {
        let pager = DiskPager::open("test-files/test-open.rdb").unwrap();
        assert_eq!(pager.borrow().n_pages, 1);
    }

    #[test]
    fn meta_page1() {
        let mut pager = DiskPager::open("test-files/metapage-test.rdb").unwrap();
        assert_eq!(pager.borrow().n_pages, 1);
        pager.borrow().root_update().unwrap();
        metapage_load(&mut *pager.borrow_mut());

        assert_eq!(pager.borrow().tree.root_ptr, None);
        assert_eq!(
            str::from_utf8(&pager.borrow().mmap.chunks[0].data[..SIG_SIZE]).unwrap(),
            DB_SIG
        );
    }
}
