use core::ffi::c_void;
use parking_lot::Mutex;
use rustix::mm::{MapFlags, ProtFlags};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::io::IoSlice;
use std::ops::{Deref, DerefMut};
use std::os::fd::OwnedFd;
use std::rc::{Rc, Weak};
use std::sync::OnceLock;

use tracing::{debug, error, info, instrument};

use crate::database::helper::{as_mb, as_page, input_valid};
use crate::database::{
    errors::{Error, PagerError},
    helper::{create_file_sync, slice_to_pointer, write_pointer},
    node::Node,
    tree::BTree,
    types::*,
};

pub static GLOBAL_PAGER: OnceLock<Mutex<MemoryPager>> = OnceLock::new();

#[derive(Debug)]
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

#[allow(unused)]
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
    failed: Cell<bool>,
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
    /// initializes pager
    ///
    /// opens file, and sets up callbacks for the tree
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
            failed: Cell::new(false),
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
        let mut ref_mut = pager.tree.borrow_mut();
        ref_mut.decode = decode;
        ref_mut.encode = encode;
        ref_mut.dealloc = dealloc;
        let fd_size = rustix::fs::fstat(&pager.database)
            .map_err(|e| {
                error!("Error when getting file size");
                Error::PagerError(PagerError::FDError(e))
            })
            .unwrap()
            .st_size as u64;
        drop(ref_mut);
        pager.root_read(fd_size);
        debug!(
            "\npager initialized:\nmmap.total {}\nn_pages {}\nchunks.len {}",
            pager.state.borrow().mmap.total,
            pager.state.borrow().n_pages,
            pager.state.borrow().mmap.chunks.len(),
        );
        Ok(pager)
    }

    #[instrument(skip(self))]
    pub fn get(&self, key: &str) -> Result<Option<String>, Error> {
        input_valid(key, " ")?;
        Ok(self.tree.borrow().search(key))
    }

    #[instrument(skip(self))]
    pub fn set(&self, key: &str, val: &str) -> Result<(), Error> {
        input_valid(key, val)?;
        info!(key = key, value = val, "inserting");
        self.tree.borrow_mut().insert(key, val).map_err(|e| {
            error!(%e, "tree error");
            e
        })?;
        debug!("updating file");
        self.update_or_revert()?;
        // update or revert
        Ok(())
    }

    #[instrument(skip(self))]
    pub fn delete(&self, key: &str) -> Result<(), Error> {
        input_valid(key, " ")?;
        self.tree.borrow_mut().delete(key)?;
        self.update_or_revert()?;
        Ok(())
    }

    fn update_or_revert(&self) -> Result<(), Error> {
        let meta = metapage_save(self); // saving current metapage
        if self.failed.get() {
            if self.state.borrow().mmap.chunks[0].data[..METAPAGE_SIZE] == *meta.0 {
                self.failed.set(false);
            } else {
                metapage_write(self).unwrap();
                rustix::fs::fsync(&self.database).unwrap();
                self.failed.set(false);
            }
        };
        if let Err(e) = self.file_update() {
            error!(%e, "file update failed! Reverting meta page...");
            self.state.borrow_mut().temp.clear(); // discard buffer
            metapage_load(self, meta); // in case the file writing fails, we revert back to the old meta page
            self.failed.set(true);
            return Err(Error::PagerError(e));
        }
        Ok(())
    }

    /// write sequence, first nodes in buffer then root
    fn file_update(&self) -> Result<(), PagerError> {
        // flush buffer to disk
        self.page_write()?;
        rustix::fs::fsync(&self.database)?;
        // updates root and writes new metapage to disk
        metapage_write(self)?;
        rustix::fs::fsync(&self.database)?;
        Ok(())
    }

    /// helper function: writePages, flushes the buffer
    fn page_write(&self) -> Result<(), PagerError> {
        debug!("writing page...");
        // extend the mmap if needed
        // number of page plus current buffer
        let imm_ref = self.state.borrow();
        let new_size = (imm_ref.n_pages as usize + imm_ref.temp.len()) * PAGE_SIZE; // amount of pages in bytes
        drop(imm_ref);
        mmap_extend(self, new_size)
            .map_err(|e| {
                error!(%e, new_size, "Error when extending mmap");
            })
            .unwrap();
        // write data pages to the file
        let mut mut_ref = self.state.borrow_mut();
        let offset = mut_ref.n_pages as usize * PAGE_SIZE;
        let buf: Vec<IoSlice> = mut_ref
            .temp
            .iter()
            .map(|node| rustix::io::IoSlice::new(&node[..PAGE_SIZE]))
            .collect();
        debug!(
            "flushed {} nodes at offset {offset} (page: {})",
            buf.len(),
            offset / PAGE_SIZE,
        );
        let bytes_written = rustix::io::pwritev(&self.database, &buf, offset as u64)?;
        assert!(bytes_written == mut_ref.temp.len() * PAGE_SIZE);
        //discard in-memory data
        mut_ref.n_pages += mut_ref.temp.len() as u64;
        mut_ref.temp.clear();
        Ok(())
    }

    /// reads mmap chunk and loads its data into memory, sets root_ptr and n_pages
    fn root_read(&self, file_size: u64) {
        if file_size == 0 {
            // empty file
            debug!("root read: file size = 0, empty file...");
            self.state.borrow_mut().n_pages = 1;
            return;
        }
        debug!("root read: loading meta page");
        if self.state.borrow().mmap.chunks.len() == 0 {
            mmap_extend(self, PAGE_SIZE).expect("mmap extend error");
        };
        let mut meta = MetaPage::new();
        meta.copy_from_slice(self.state.borrow_mut().mmap.chunks[0].data);
        metapage_load(self, meta);
        assert!(self.state.borrow().n_pages != 0);
    }

    // callbacks
    /// page read
    fn decode(&self, state: &State, ptr: Pointer) -> Node {
        debug!(
            "decoding ptr: {}, amount of chunks {}, chunk 0 size {}",
            ptr.0,
            state.mmap.chunks.len(),
            state.mmap.chunks[0].len
        );
        let mut start: usize = 0;
        for chunk in state.mmap.chunks.iter() {
            let end = start + chunk.data.len() / PAGE_SIZE;
            if ptr.0 < end as u64 {
                let offset: usize = PAGE_SIZE * (ptr.0 as usize - start);
                let mut node = Node::new();
                node.0[..PAGE_SIZE].copy_from_slice(&chunk.data[offset..offset + PAGE_SIZE]);
                debug!("returning node at offset {offset}, {}", as_page(offset));
                return node;
            }
            start = end;
        }
        error!("bad pointer: {}", ptr.0);
        panic!()
    }

    /// page append, loads node into buffer to be flushed later
    fn encode(&self, state: &mut State, node: Node) -> Pointer {
        // empty db has n_pages = 1 (meta page)
        assert!(node.fits_page());
        let ptr = Pointer(state.n_pages + state.temp.len() as u64);
        debug!(
            "encode: adding {:?} at page: {} to buffer",
            node.get_type(),
            ptr.0
        );
        state.temp.push(node);
        assert!(ptr.0 != 0);
        ptr
    }

    fn dealloc(&self, state: &mut State, ptr: Pointer) {
        debug!("deallocating ptr {}", ptr.0)
    }
}

/// read-only
fn mmap_new<'a>(fd: &OwnedFd, offset: u64, length: usize) -> Result<Chunk<'a>, PagerError> {
    debug!(
        "requesting new mmap: length {length} {}, offset {offset}",
        as_mb(length)
    );
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
    let state_ref = db.state.borrow();
    if size <= state_ref.mmap.total {
        debug!("no mmap extension needed: size {size}, {}", as_mb(size));
        return Ok(()); // enough range
    };
    debug!("extending mmap: for file size {size}, {}", as_mb(size));
    let mut alloc = usize::max(state_ref.mmap.total, 64 << 20); // double the current address space
    while state_ref.mmap.total + alloc < size {
        alloc *= 2;
    }
    let chunk = mmap_new(&db.database, state_ref.mmap.total as u64, alloc).map_err(|e| {
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

/// returns metapage object of current pager state
fn metapage_save(pager: &DiskPager) -> MetaPage {
    let mut data = MetaPage::new();
    // write sig
    data[..SIG_SIZE].copy_from_slice(DB_SIG.as_bytes());
    // write root ptr
    debug!(
        "metapage: writing root ptr: {} to meta page",
        pager.tree.borrow().root_ptr.or(Some(Pointer(0))).unwrap()
    );
    write_pointer(
        &mut data,
        SIG_SIZE,
        pager.tree.borrow().root_ptr.or(Some(Pointer(0))).unwrap(),
    )
    .unwrap();
    // write n pages
    debug!(
        "metapage: writing n_pages: {}",
        pager.state.borrow().n_pages
    );
    data[SIG_SIZE + PTR_SIZE..SIG_SIZE + (PTR_SIZE * 2)]
        .copy_from_slice(&pager.state.borrow().n_pages.to_le_bytes());
    data
}

/// loads meta page object into pager
///
/// panics when called without initialized mmap
fn metapage_load(pager: &DiskPager, data: MetaPage) {
    let mut pager_ref = pager.state.borrow_mut();
    pager.tree.borrow_mut().root_ptr = match slice_to_pointer(&data, SIG_SIZE) {
        Ok(Pointer(0)) => None,
        Ok(n) => Some(n),
        Err(e) => {
            error!(%e, "Error when reading root ptr from meta page");
            panic!()
        }
    };
    pager_ref.n_pages = u64::from_le_bytes(
        data[SIG_SIZE + PTR_SIZE..SIG_SIZE + PTR_SIZE * 2]
            .try_into()
            .unwrap(),
    );
    info!(
        "opening database: {}",
        str::from_utf8(&data[..SIG_SIZE]).unwrap()
    );
}

/// writes currently loaded meta data to disk
fn metapage_write(pager: &DiskPager) -> Result<(), PagerError> {
    debug!("updating root...");
    let r = rustix::io::pwrite(&pager.database, &metapage_save(pager), 0)?;
    Ok(())
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use super::*;
    use rand::Rng;
    use test_log::test;

    fn cleanup_file(path: &str) {
        if Path::new(path).exists() {
            std::fs::remove_file(path).unwrap()
        }
    }

    #[test]
    fn open_pager() {
        let path = "test-files/open_pager.rdb";
        cleanup_file(path);
        let pager = DiskPager::open(path).unwrap();
        assert_eq!(pager.state.borrow().n_pages, 1);
        cleanup_file(path);
    }

    // #[test]
    // fn meta_page1() {
    //     let path = "test-files/meta_page1.rdb";
    //     cleanup_file(path);
    //     let pager = DiskPager::open(path).unwrap();
    //     assert_eq!(pager.state.borrow().n_pages, 1);
    //     pager.metapage_write().unwrap();
    //     metapage_load(&pager);

    //     assert_eq!(pager.tree.borrow().root_ptr, None);
    //     assert_eq!(
    //         str::from_utf8(&pager.state.borrow().mmap.chunks[0].data[..SIG_SIZE]).unwrap(),
    //         DB_SIG
    //     );
    //     cleanup_file(path);
    // }

    #[test]
    fn disk_insert1() {
        let path = "test-files/disk_insert1.rdb";
        cleanup_file(path);
        let pager = DiskPager::open(path).unwrap();
        pager.set("1", "val").unwrap();
        assert_eq!(pager.get("1").unwrap().unwrap(), "val".to_string());
        cleanup_file(path);
    }

    #[test]
    fn disk_insert2() {
        let path = "test-files/disk_insert2.rdb";
        cleanup_file(path);
        let pager = DiskPager::open(path).unwrap();

        for i in 1u16..=300u16 {
            pager.set(&format!("{i}"), "value").unwrap()
        }
        for i in 1u16..=300u16 {
            assert_eq!(pager.get(&format!("{i}")).unwrap().unwrap(), "value")
        }
        cleanup_file(path);
    }

    #[test]
    fn disk_delete() {
        let path = "test-files/disk_insert2.rdb";
        cleanup_file(path);
        let pager = DiskPager::open(path).unwrap();

        for i in 1u16..=300u16 {
            pager.set(&format!("{i}"), "value").unwrap()
        }
        for i in 1u16..=300u16 {
            pager.delete(&format!("{i}")).unwrap();
        }
        cleanup_file(path);
    }

    #[test]
    fn disk_random1() {
        let path = "test-files/disk_random1.rdb";
        cleanup_file(path);
        let pager = DiskPager::open(path).unwrap();

        for _ in 1u16..=1000 {
            pager
                .set(&format!("{:?}", rand::rng().random_range(1..1000)), "val")
                .unwrap()
        }
        cleanup_file(path);
    }
}
