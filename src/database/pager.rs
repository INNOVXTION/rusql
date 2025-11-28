use core::ffi::c_void;
use parking_lot::Mutex;
use rustix::mm::{MapFlags, ProtFlags};
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::IoSlice;
use std::ops::{Deref, DerefMut};
use std::os::fd::OwnedFd;
use std::rc::{Rc, Weak};
use std::sync::{Arc, OnceLock};

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
    let pager = GLOBAL_PAGER.get().unwrap().lock();
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
    let mut pager = GLOBAL_PAGER.get().unwrap().lock();
    let free_page = pager.freelist.pop().expect("no free page available");
    debug!("encoding node at ptr {}", free_page);
    pager.pages.insert(free_page, node);
    Pointer(free_page)
}

fn dealloc(ptr: Pointer) {
    debug!("deleting node at ptr {}", ptr.0);
    let mut pager = GLOBAL_PAGER.get().unwrap().lock();
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
    tree: RefCell<BTree<'a>>,
    state: Rc<RefCell<State<'a>>>, // newly allocated pages to be flushed
}

#[derive(Debug)]
struct State<'a> {
    mmap: Mmap<'a>,
    n_pages: u64, // database size in number of pages
    temp: Vec<Node>,
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
    #[instrument]
    fn open(path: &'static str) -> Result<Rc<Self>, Error> {
        let pager = Rc::new(DiskPager {
            path,
            database: create_file_sync(path).map_err(|e| {
                error!("Error when opening database");
                Error::PagerError(e)
            })?,
            tree: RefCell::new(BTree {
                root_ptr: None,
                decode: Box::new(|_| panic!("not initialized")),
                encode: Box::new(|_| panic!("not initialized")),
                dealloc: Box::new(|_| panic!("not initialized")),
            }),
            state: Rc::new(RefCell::new(State {
                mmap: Mmap {
                    total: 0,
                    chunks: vec![],
                },
                n_pages: 0,
                temp: vec![],
            })),
        });

        // master weak pointer pointing to the owning struct
        let weak = Rc::downgrade(&pager);
        let decode = {
            let weak = Weak::clone(&weak);
            Box::new(move |ptr: &Pointer| {
                let strong = weak.upgrade().expect("pager dropped");
                let imm_ref = strong.state.borrow();
                strong.decode(&*imm_ref, *ptr)
            })
        };
        let encode = {
            let weak = Weak::clone(&weak);
            Box::new(move |node: Node| {
                let strong = weak.upgrade().expect("pager dropped");
                let mut mut_ref = strong.state.borrow_mut();
                strong.encode(&mut *mut_ref, node)
            })
        };
        let dealloc = {
            let weak = Weak::clone(&weak);
            Box::new(move |ptr: Pointer| {
                let strong = weak.upgrade().expect("pager dropped");
                let mut mut_ref = strong.state.borrow_mut();
                strong.dealloc(&mut *mut_ref, ptr)
            })
        };

        {
            let mut ref_mut = pager.tree.borrow_mut();
            ref_mut.decode = decode;
            ref_mut.encode = encode;
            ref_mut.dealloc = dealloc;
        }
        {
            let mut pager_mut = pager.state.borrow_mut();
            let fd_size = rustix::fs::fstat(&pager.database)
                .map_err(|e| {
                    error!("Error when getting file size");
                    Error::PagerError(PagerError::FDError(e))
                })
                .unwrap()
                .st_size as u64;
            // initialize mmap
            let map_size = (fd_size * 2) as usize + PAGE_SIZE;
            let map = mmap_new(&pager.database, map_size, 0).map_err(|e| {
                error!("Error when initalizing mmap");
                Error::PagerError(e)
            })?;
            pager_mut.mmap.chunks.push(map);
            pager_mut.mmap.total = map_size;
            drop(pager_mut);
            pager.root_read(fd_size);
            debug!(
                "\npager initialized:\nmmap.total {}\nn_pages {}\nchunks.len {}",
                pager.state.borrow().mmap.total,
                pager.state.borrow().n_pages,
                pager.state.borrow().mmap.chunks.len(),
            );
        }
        Ok(pager)
    }

    #[instrument(skip(self))]
    pub fn get(&self, key: &str) -> Option<String> {
        self.tree.borrow().search(key)
    }

    // #[instrument(skip(self))]
    pub fn set(&self, key: &str, val: &str) -> Result<(), Error> {
        debug!("inserting {key}, {val}");
        self.tree.borrow_mut().insert(key, val)?;
        debug!("updating file with: {key}, {val}");
        self.file_update().map_err(|e| {
            error!("pager error {}", e);
            Error::PagerError(e)
        })?;
        Ok(())
    }

    #[instrument(skip(self))]
    pub fn delete(&mut self, key: &str) -> Result<(), Error> {
        self.tree.borrow_mut().delete(key)?;
        self.file_update().map_err(|e| {
            error!("pager error {}", e);
            Error::PagerError(e)
        })?;
        Ok(())
    }

    fn file_update(&self) -> Result<(), PagerError> {
        debug!("updating file");
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
    fn page_write(&self) -> Result<(), PagerError> {
        debug!("writing page...");
        // extend the mmap if needed
        // number of page plus current buffer
        let imm_ref = self.state.borrow();
        let new_size = (imm_ref.n_pages as usize + imm_ref.temp.len()) * PAGE_SIZE; // amount of pages in bytes
        drop(imm_ref);
        mmap_extend(self, new_size)
            .map_err(|e| {
                error!("Error when extending mmap: {}, error {}", new_size, e);
            })
            .unwrap();
        // write data pages to the file
        let mut mut_ref = self.state.borrow_mut();
        let offset = mut_ref.n_pages as usize * PAGE_SIZE;
        let buf: Vec<IoSlice> = mut_ref
            .temp
            .iter()
            .map(|node| rustix::io::IoSlice::new(&node))
            .collect();
        rustix::io::pwritev(&self.database, &buf, offset as u64)?;
        //discard in-memory data
        mut_ref.n_pages += mut_ref.temp.len() as u64;
        mut_ref.temp.clear();
        Ok(())
    }

    /// updates meta page with settings set at call time
    fn root_update(&self) -> Result<(), PagerError> {
        debug!("updating root");
        let r = rustix::io::pwrite(&self.database, &metapage_save(&self), 0)?;
        Ok(())
    }

    /// reads chunk and loads its data into memory, sets root_ptr and n_pages
    fn root_read(&self, file_size: u64) {
        if file_size == 0 {
            // empty file
            debug!("root read: file size = 0, empty file...");
            self.state.borrow_mut().n_pages = 1;
            return;
        }
        debug!("root read: loading meta page");
        metapage_load(self);
    }
    // callbacks
    // page read
    fn decode(&self, state: &State, ptr: Pointer) -> Node {
        debug!("decoding ptr: {}", ptr.0);
        let mut start: u64 = 0;
        for chunk in state.mmap.chunks.iter() {
            let end = start + chunk.len as u64 / PAGE_SIZE as u64;
            if ptr.0 < end {
                let offset: usize = PAGE_SIZE * (ptr.0 as usize - start as usize);
                let mut node = Node::new();
                node.0[..PAGE_SIZE].copy_from_slice(&chunk.data[offset..offset + PAGE_SIZE]);
                debug!("returning node at offset {offset}");
                return node;
            }
            start = end;
        }
        error!("bad pointer: {}", ptr.0);
        panic!()
    }

    // page append, loads node into buffer to be flushed later
    fn encode(&self, state: &mut State, node: Node) -> Pointer {
        debug!("encoding node...");
        let ptr = Pointer(state.n_pages + state.temp.len() as u64);
        state.temp.push(node);
        ptr
    }

    fn dealloc(&self, state: &mut State, ptr: Pointer) {
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
            std::ptr::null_mut(),
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
fn mmap_extend(db: &DiskPager, size: usize) -> Result<(), PagerError> {
    debug!("extending mmap: size {size}");
    let state_ref = db.state.borrow();
    if size <= state_ref.mmap.total {
        return Ok(()); // enough range
    };
    let mut alloc = usize::max(state_ref.mmap.total, 64 << 20); // double the current address space
    while state_ref.mmap.total + alloc < size {
        alloc *= 2;
    }
    let chunk = mmap_new(&db.database, alloc, state_ref.mmap.total as u64).map_err(|e| {
        error!("error when extending mmap, size: {size}");
        e
    })?;
    drop(state_ref);
    let mut state_ref = db.state.borrow_mut();
    state_ref.mmap.total += alloc;
    state_ref.mmap.chunks.push(chunk);
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
        pager.tree.borrow().root_ptr.or(Some(Pointer(0))).unwrap(),
    )
    .unwrap();
    // write n pages
    data[SIG_SIZE + PTR_SIZE..SIG_SIZE + (PTR_SIZE * 2)]
        .copy_from_slice(&pager.state.borrow().n_pages.to_le_bytes());
    data
}

/// reads chunk and sets diskpager root ptr and npages
fn metapage_load(pager: &DiskPager) {
    let mut pager_ref = pager.state.borrow_mut();
    let data = pager_ref.mmap.chunks[0].data;
    pager.tree.borrow_mut().root_ptr = match slice_to_pointer(data, SIG_SIZE) {
        Ok(Pointer(0)) => None,
        Ok(n) => Some(n),
        Err(e) => {
            error!("Error when reading root ptr from meta page {}", e);
            panic!()
        }
    };
    pager_ref.n_pages = u64::from_le_bytes(
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
    use crate::database::pager;
    use test_log::test;

    fn clear_test(path: &str) {
        std::fs::remove_file(path).unwrap()
    }

    #[test]
    fn open_pager() {
        let path = "test-files/open_pager.rdb";
        let pager = DiskPager::open(path).unwrap();
        assert_eq!(pager.state.borrow().n_pages, 1);
        clear_test(path);
    }

    #[test]
    fn meta_page1() {
        let path = "test-files/meta_page1.rdb";
        let pager = DiskPager::open(path).unwrap();
        assert_eq!(pager.state.borrow().n_pages, 1);
        pager.root_update().unwrap();
        metapage_load(&pager);

        assert_eq!(pager.tree.borrow().root_ptr, None);
        assert_eq!(
            str::from_utf8(&pager.state.borrow().mmap.chunks[0].data[..SIG_SIZE]).unwrap(),
            DB_SIG
        );
    }

    #[test]
    fn disk_insert() {
        let path = "test-files/disk_insert.rdb";
        let pager = DiskPager::open(path).unwrap();
        pager.set("1", "val").unwrap();
        assert_eq!(pager.get("1").unwrap(), "val".to_string());
        clear_test(path);
    }
}
