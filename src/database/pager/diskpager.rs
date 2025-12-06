use core::ffi::c_void;
use rustix::mm::{MapFlags, ProtFlags};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::os::fd::OwnedFd;
use std::ptr;
use std::rc::{Rc, Weak};
use std::str::FromStr;

use tracing::{debug, error, info, instrument};

use crate::database::helper::{as_mb, as_page, input_valid, print_buffer};
use crate::database::pager::freelist::{FLNode, FreeList};
use crate::database::{
    errors::{Error, PagerError},
    helper::create_file_sync,
    node::TreeNode,
    tree::BTree,
    types::*,
};

/// indicates the encoding/decodin style of a node
#[derive(Debug)]
pub enum NodeFlag {
    Tree,
    Freelist,
}

#[derive(Debug)]
pub struct DiskPager {
    path: &'static str,
    database: OwnedFd,
    tree: RefCell<BTree>,
    state: RefCell<State>,
    failed: Cell<bool>,
    freelist: RefCell<FreeList>,
    buffer: RefCell<Buffer>,
}

#[derive(Debug)]
struct Buffer {
    hmap: HashMap<Pointer, Node>, // pages to be written, like temp
    nappend: u64,                 // number of pages to be appended
}

#[derive(Debug)]
struct State {
    mmap: Mmap,
    npages: u64, // database size in number of pages
}

#[derive(Debug)]
struct Mmap {
    total: usize,       // mmap size, can be larger than the file size
    chunks: Vec<Chunk>, // multiple mmaps, can be non-continuous
}

#[derive(Debug)]
struct Chunk {
    data: *const u8,
    len: usize,
}

impl DiskPager {
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
            state: RefCell::new(State {
                mmap: Mmap {
                    total: 0,
                    chunks: vec![],
                },
                npages: 0,
            }),
            failed: Cell::new(false),
            buffer: RefCell::new(Buffer {
                nappend: 0,
                hmap: HashMap::new(),
            }),
            freelist: RefCell::new(FreeList::new()),
        });

        // master weak pointer pointing to the owning struct
        // each callbacl back captures a copy of the weak pointer on creation, which is used to refer back to the pager
        let weak = Rc::downgrade(&pager);
        {
            // initializing BTree callbacks
            // pageRead
            let decode = {
                let weak = Weak::clone(&weak);
                Box::new(move |ptr: Pointer| {
                    weak.upgrade()
                        .expect("pager dropped")
                        .page_read(ptr, NodeFlag::Tree)
                        .as_tree()
                })
            };
            // alloc
            let encode = {
                let weak = Weak::clone(&weak);
                Box::new(move |node: TreeNode| {
                    weak.upgrade()
                        .expect("pager dropped")
                        .page_alloc(Node::Tree(node))
                })
            };
            // dealloc
            let dealloc = {
                let weak = Weak::clone(&weak);
                Box::new(move |ptr: Pointer| weak.upgrade().expect("pager dropped").dealloc(ptr))
            };
            let mut ref_mut = pager.tree.borrow_mut();
            ref_mut.decode = decode;
            ref_mut.encode = encode;
            ref_mut.dealloc = dealloc;
        }
        {
            // initializing freelist callbacks
            // pageRead
            let decode = {
                let weak = Weak::clone(&weak);
                Box::new(move |ptr: Pointer| {
                    weak.upgrade()
                        .expect("pager dropped")
                        .page_read(ptr, NodeFlag::Freelist)
                        .as_fl()
                })
            };
            // pageAppend
            let encode = {
                let weak = Weak::clone(&weak);
                Box::new(move |node: FLNode| {
                    weak.upgrade()
                        .expect("pager dropped")
                        .encode(Node::Freelist(node))
                })
            };
            // pageAppend
            let update = {
                let weak = Weak::clone(&weak);
                Box::new(move |ptr: Pointer| weak.upgrade().expect("pager dropped").update(ptr))
            };

            let mut ref_fl = pager.freelist.borrow_mut();
            ref_fl.decode = decode;
            ref_fl.encode = encode;
            ref_fl.update = update;
        }

        let fd_size = rustix::fs::fstat(&pager.database)
            .map_err(|e| {
                error!("Error when getting file size");
                Error::PagerError(PagerError::FDError(e))
            })
            .unwrap()
            .st_size as u64;
        pager.root_read(fd_size);
        debug!(
            "\npager initialized:\nmmap.total {}\nn_pages {}\nchunks.len {}",
            pager.state.borrow().mmap.total,
            pager.state.borrow().npages,
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
        info!("inserting");
        self.tree.borrow_mut().insert(key, val).map_err(|e| {
            error!(%e, "tree error");
            e
        })?;
        debug!("updating file");
        self.update_or_revert()?;
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
            if self.state.borrow().mmap.chunks[0].to_slice()[..METAPAGE_SIZE] == *meta.0 {
                self.failed.set(false);
            } else {
                metapage_write(self).unwrap();
                rustix::fs::fsync(&self.database).unwrap();
                self.failed.set(false);
            }
        };
        if let Err(e) = self.file_update() {
            error!(%e, "file update failed! Reverting meta page...");
            self.buffer.borrow_mut().hmap.clear(); // discard buffer
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
        // updating free list for next update
        self.freelist.borrow_mut().set_max_seq();
        assert!(self.tree.borrow().root_ptr != self.freelist.borrow().head_page);
        assert!(self.tree.borrow().root_ptr != self.freelist.borrow().tail_page);
        Ok(())
    }

    /// helper function: writePages, flushes the buffer
    ///
    fn page_write(&self) -> Result<(), PagerError> {
        debug!("writing page...");
        let mut buf = self.buffer.borrow_mut();
        let buf_len = buf.hmap.len();
        let npage = self.state.borrow().npages;

        // extend the mmap if needed
        // number of page plus current buffer
        let new_size = (npage as usize + buf_len) * PAGE_SIZE; // amount of pages in bytes
        mmap_extend(self, new_size)
            .map_err(|e| {
                error!(%e, new_size, "Error when extending mmap");
            })
            .unwrap();

        // // write data pages to the file
        // let offset = npage as usize * PAGE_SIZE;
        // let io_buf: Vec<IoSlice> = buf
        //     .iter()
        //     .map(|pair| rustix::io::IoSlice::new(&pair.1[..PAGE_SIZE]))
        //     .collect();
        // debug!(
        //     "flushed {} nodes at offset {offset} (page: {})",
        //     buf_len,
        //     offset / PAGE_SIZE,
        // );
        // let bytes_written = rustix::io::pwritev(&self.database, &io_buf, offset as u64)?;

        // iterate over buffer and write nodes to designated pages
        debug!(
            nappend = buf.nappend,
            buffer = buf_len,
            "pages to be written:"
        );
        let mut bytes_written: usize = 0;
        for pair in buf.hmap.iter() {
            assert!(pair.0.get() != 0);
            debug!("writing {:?} at {}", pair.1.get_type(), pair.0);
            let offset = pair.0.get() * PAGE_SIZE as u64;
            let io_slice = rustix::io::IoSlice::new(&pair.1[..PAGE_SIZE]);
            bytes_written +=
                rustix::io::pwrite(&self.database, &io_slice, offset).map_err(|e| {
                    error!(?e, "page writing error!");
                    PagerError::WriteFileError(e)
                })?;
        }
        debug!(bytes_written, "bytes written:");
        assert!(bytes_written == buf_len * PAGE_SIZE);
        //discard in-memory data
        self.state.borrow_mut().npages += buf.nappend;
        buf.nappend = 0;
        buf.hmap.clear();
        Ok(())
    }

    /// reads metapage from mmap chunk and loads its data into memory, sets root_ptr and n_pages
    fn root_read(&self, file_size: u64) {
        if file_size == 0 {
            // empty file
            debug!("root read: empty file...");
            self.state.borrow_mut().npages = 2; // reserved for meta page and one free list node
            self.freelist.borrow_mut().head_page = Some(Pointer::from(1u64));
            self.freelist.borrow_mut().tail_page = Some(Pointer::from(1u64));
            return;
        }
        debug!("root read: loading meta page");
        if self.state.borrow().mmap.chunks.len() == 0 {
            mmap_extend(self, PAGE_SIZE).expect("mmap extend error");
        };
        let mut meta = MetaPage::new();
        meta.copy_from_slice(self.state.borrow_mut().mmap.chunks[0].to_slice());
        metapage_load(self, meta);
        assert!(self.state.borrow().npages != 0);
    }

    // decodes a page, checks buffer before reading disk
    // `BTree.get`, reads a page possibly from buffer or disk
    fn page_read(&self, ptr: Pointer, flag: NodeFlag) -> Node {
        // check buffer first
        debug!(node=?flag, %ptr, "page read");
        if let Some(n) = self.buffer.borrow_mut().hmap.remove(&ptr) {
            debug!("page found in buffer!");
            n
        } else {
            debug!("reading from disk...");
            self.decode(ptr, flag)
        }
    }

    /// callback for free list
    /// checks buffer for allocated page and returns pointer
    fn update(&self, ptr: Pointer) -> *mut FLNode {
        let buf = &mut self.buffer.borrow_mut();
        // checking buffer, decoding a page if needed
        let entry = match buf.hmap.get_mut(&ptr) {
            Some(n) => {
                debug!("updating {} in buffer", ptr);
                n
            }
            None => {
                debug!(%ptr, "reading free list from disk...");
                buf.hmap.insert(ptr, self.decode(ptr, NodeFlag::Freelist));
                buf.hmap.get_mut(&ptr).expect("we just inserted it")
            }
        };
        // let entry = buf.hmap.entry(ptr).or_insert_with(|| {
        //     debug!("loading free list from disk...");
        //     self.decode(ptr, NodeFlag::Freelist)
        // });
        match entry {
            Node::Freelist(flnode) => {
                let ptr = ptr::from_mut(flnode);
                print_buffer(&buf.hmap);
                ptr
            }
            // only the freelist calls this function
            _ => unreachable!(),
        }
    }

    /// decodes a page from disk
    /// kv.pageRead, db.pageRead -> pagereadfile
    fn decode(&self, ptr: Pointer, node_type: NodeFlag) -> Node {
        let state = self.state.borrow();
        debug!(
            "decoding ptr: {}, amount of chunks {}, chunk 0 size {}",
            ptr.0,
            state.mmap.chunks.len(),
            state.mmap.chunks[0].len
        );

        let mut start: usize = 0;
        for chunk in state.mmap.chunks.iter() {
            let end = start + chunk.len() / PAGE_SIZE;
            if ptr.0 < end as u64 {
                let offset: usize = PAGE_SIZE * (ptr.0 as usize - start);
                let mut node = match node_type {
                    NodeFlag::Tree => Node::Tree(TreeNode::new()),
                    NodeFlag::Freelist => Node::Freelist(FLNode::new()),
                };
                node[..PAGE_SIZE].copy_from_slice(&chunk[offset..offset + PAGE_SIZE]);
                debug!("returning node at offset {offset}, {}", as_page(offset));
                return node;
            }
            start = end;
        }
        error!("bad pointer: {}", ptr.0);
        panic!()
    }

    /// allocates a new page to be encoded, checks freelist first before appending to disk
    ///
    /// pageAlloc, new
    fn page_alloc(&self, node: Node) -> Pointer {
        // check freelist first
        debug!("allocating ...");
        if let Some(ptr) = self.freelist.borrow_mut().get() {
            self.buffer.borrow_mut().hmap.insert(ptr, node);
            print_buffer(&self.buffer.borrow().hmap);
            ptr
        } else {
            // increment append?
            debug!("appending to disk...");
            self.encode(node)
        }
    }

    /// adds pages to buffer to be encoded to disk later (append)
    /// does not check if node exists in buffer!
    /// pageAppend
    fn encode(&self, node: Node) -> Pointer {
        let state = self.state.borrow();
        let buf = &mut self.buffer.borrow_mut();
        // empty db has n_pages = 1 (meta page)
        assert!(node.fits_page());
        buf.nappend += 1;
        let ptr = Pointer(state.npages + buf.nappend);
        debug!(
            "encode: adding {:?} at page: {} to buffer",
            node.get_type(),
            ptr.0
        );
        buf.hmap.insert(ptr, node);
        print_buffer(&buf.hmap);
        assert!(ptr.0 != 0);
        ptr
    }

    /// PushTail
    fn dealloc(&self, ptr: Pointer) {
        self.freelist
            .borrow_mut()
            .append(ptr)
            .expect("dealloc error");
        ()
    }
}

/// read-only
fn mmap_new(fd: &OwnedFd, offset: u64, length: usize) -> Result<Chunk, PagerError> {
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
        data: ptr as *const u8,
        len: length,
    })
}

/// checks for sufficient space, exponentially extends the mmap
fn mmap_extend(db: &DiskPager, size: usize) -> Result<(), PagerError> {
    let state_ref = db.state.borrow();
    if size <= state_ref.mmap.total {
        debug!(
            "no mmap extension needed: mmap_size {size} ({}), file_size {}",
            as_mb(size),
            as_mb(state_ref.npages as usize * PAGE_SIZE)
        );
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

impl Deref for Chunk {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.to_slice()
    }
}

impl Chunk {
    fn to_slice(&self) -> &[u8] {
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

//--------Meta Page Layout-------
// | sig | root_ptr | page_used |
// | 16B |    8B    |     8B    |
//
// new
// | sig | root_ptr | page_used | head_page | head_seq | tail_page | tail_seq |
// | 16B |    8B    |     8B    |     8B    |    8B    |     8B    |    8B    |

// offsets
enum MpField {
    Sig = 0,
    RootPtr = 16,
    Npages = 16 + 8,
    HeadPage = 16 + (8 * 2),
    HeadSeq = 16 + (8 * 3),
    TailPage = 16 + (8 * 4),
    TailSeq = 16 + (8 * 5),
}

struct MetaPage(Box<[u8; METAPAGE_SIZE]>);

impl MetaPage {
    fn new() -> Self {
        MetaPage(Box::new([0u8; METAPAGE_SIZE]))
    }

    fn set_sig(&mut self, sig: &str) {
        self[..16].copy_from_slice(sig.as_bytes());
    }

    fn read_sig(&self) -> String {
        String::from_str(str::from_utf8(&self[..16]).unwrap()).unwrap()
    }

    fn set_ptr(&mut self, ptr: Option<Pointer>, field: MpField) {
        let offset = field as usize;
        let ptr = match ptr {
            Some(ptr) => ptr,
            None => Pointer::from(0u64),
        };
        self[offset..offset + PTR_SIZE].copy_from_slice(&ptr.as_slice());
    }

    fn read_ptr(&self, field: MpField) -> Pointer {
        let offset = field as usize;
        Pointer::from(u64::from_le_bytes(
            self[offset..offset + PTR_SIZE].try_into().unwrap(),
        ))
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

/// returns metapage object of current pager state
fn metapage_save(pager: &DiskPager) -> MetaPage {
    let fl_ref = pager.freelist.borrow();
    let mut data = MetaPage::new();
    debug!(
        sig = DB_SIG,
        root_ptr = ?pager.tree.borrow().root_ptr.unwrap(),
        npages = pager.state.borrow().npages,
        fl_head_ptr = ?fl_ref.head_page,
        fl_head_seq = fl_ref.head_seq,
        fl_tail_ptr = ?fl_ref.tail_page,
        fl_tail_seq = fl_ref.tail_seq,
        "saving meta page:"
    );
    data.set_sig(DB_SIG);
    data.set_ptr(pager.tree.borrow().root_ptr, MpField::RootPtr);
    data.set_ptr(Some(pager.state.borrow().npages.into()), MpField::Npages);

    data.set_ptr(fl_ref.head_page, MpField::HeadPage);
    data.set_ptr(Some(fl_ref.head_seq.into()), MpField::HeadSeq);
    data.set_ptr(fl_ref.tail_page, MpField::TailPage);
    data.set_ptr(Some(fl_ref.tail_seq.into()), MpField::TailSeq);
    data
    // // write sig
    // data[..SIG_SIZE].copy_from_slice(DB_SIG.as_bytes());
    // // write root ptr
    // debug!(
    //     "metapage: writing root ptr: {} to meta page",
    //     pager.tree.borrow().root_ptr.or(Some(Pointer(0))).unwrap()
    // );
    // write_pointer(
    //     &mut data,
    //     SIG_SIZE,
    //     pager.tree.borrow().root_ptr.or(Some(Pointer(0))).unwrap(),
    // )
    // .unwrap();
    // // write n pages
    // debug!("metapage: writing n_pages: {}", pager.state.borrow().npages);
    // data[SIG_SIZE + PTR_SIZE..SIG_SIZE + (PTR_SIZE * 2)]
    //     .copy_from_slice(&pager.state.borrow().npages.to_le_bytes());
    // data
}

/// loads meta page object into pager
///
/// panics when called without initialized mmap
fn metapage_load(pager: &DiskPager, data: MetaPage) {
    debug!("loading metapage");
    let mut pager_ref = pager.state.borrow_mut();
    pager.tree.borrow_mut().root_ptr = match data.read_ptr(MpField::RootPtr) {
        Pointer(0) => None,
        n => Some(n),
    };
    pager_ref.npages = data.read_ptr(MpField::Npages).get();

    let mut fl_ref = pager.freelist.borrow_mut();
    fl_ref.head_page = Some(data.read_ptr(MpField::HeadPage));
    fl_ref.head_seq = data.read_ptr(MpField::HeadSeq).get() as usize;
    fl_ref.tail_page = Some(data.read_ptr(MpField::TailPage));
    fl_ref.tail_seq = data.read_ptr(MpField::TailSeq).get() as usize;

    // pager.tree.borrow_mut().root_ptr = match read_pointer(&data, SIG_SIZE) {
    //     Ok(Pointer(0)) => None,
    //     Ok(n) => Some(n),
    //     Err(e) => {
    //         error!(%e, "Error when reading root ptr from meta page");
    //         panic!()
    //     }
    // };
    // pager_ref.npages = u64::from_le_bytes(
    //     data[SIG_SIZE + PTR_SIZE..SIG_SIZE + PTR_SIZE * 2]
    //         .try_into()
    //         .unwrap(),
    // );
    // info!(
    //     "opening database: {}",
    //     str::from_utf8(&data[..SIG_SIZE]).unwrap()
    // );
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
        assert_eq!(pager.state.borrow().npages, 2);
        cleanup_file(path);
    }

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
    fn disk_delete1() {
        let path = "test-files/disk_delete1.rdb";
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

    #[test]
    fn disk_delete2() {
        let path = "test-files/disk_delete2.rdb";
        cleanup_file(path);
        let pager = DiskPager::open(path).unwrap();

        for k in 1u16..=1000 {
            pager.set(&format!("{}", k), "val").unwrap()
        }
        for k in 1u16..=1000 {
            pager.delete(&format!("{}", k)).unwrap()
        }
        cleanup_file(path);
    }
}
