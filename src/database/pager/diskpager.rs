use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::os::fd::OwnedFd;
use std::ptr;
use std::rc::{Rc, Weak};
use std::str::FromStr;

use tracing::{debug, error, info, instrument, warn};

use crate::database::helper::{as_page, input_valid, print_buffer};
use crate::database::pager::freelist::{FLNode, FreeList};
use crate::database::pager::mmap::*;
use crate::database::{
    btree::{BTree, TreeNode},
    errors::{Error, PagerError},
    helper::create_file_sync,
    types::*,
};

/// indicates the encoding/decoding style of a node
#[derive(Debug)]
pub enum NodeFlag {
    Tree,
    Freelist,
}

#[derive(Debug)]
pub struct DiskPager {
    path: &'static str,
    pub database: OwnedFd,
    tree: RefCell<BTree>,
    failed: Cell<bool>,
    freelist: RefCell<FreeList>,
    pub buffer: RefCell<Buffer>,
    pub mmap: RefCell<Mmap>,
}

#[derive(Debug)]
pub struct Buffer {
    pub hmap: HashMap<Pointer, Node>, // pages to be written
    pub nappend: u64,                 // number of pages to be appended
    pub npages: u64,                  // database size in number of pages
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
            mmap: RefCell::new(Mmap {
                total: 0,
                chunks: vec![],
            }),
            failed: Cell::new(false),
            buffer: RefCell::new(Buffer {
                nappend: 0,
                hmap: HashMap::new(),
                npages: 0,
            }),
            freelist: RefCell::new(FreeList::new()),
        });

        // master weak pointer pointing to the owning struct
        // each callback captures a copy of the weak pointer on creation, which is used to refer back to the pager
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
        mmap_extend(&pager, PAGE_SIZE).expect("mmap extend error");
        metapage_read(&pager, fd_size);
        debug!(
            "\npager initialized:\nmmap.total {}\nn_pages {}\nchunks.len {}",
            pager.mmap.borrow().total,
            pager.buffer.borrow().npages,
            pager.mmap.borrow().chunks.len(),
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
        let recov_page = metapage_save(self); // saving current metapage for possible rollback
        info!("inserting");
        self.tree.borrow_mut().insert(key, val).map_err(|e| {
            error!(%e, "tree error");
            e
        })?;
        debug!("updating file");
        self.update_or_revert(&recov_page)
    }

    #[instrument(skip(self))]
    pub fn delete(&self, key: &str) -> Result<(), Error> {
        input_valid(key, " ")?;
        let recov_page = metapage_save(self); // saving current metapage for possible rollback
        self.tree.borrow_mut().delete(key)?;
        self.update_or_revert(&recov_page)
    }

    fn update_or_revert(&self, recov_page: &MetaPage) -> Result<(), Error> {
        if self.failed.get() {
            debug!("failed update detected...");
            // checking after previous error to see if the meta page on disk fits with page in memory
            if self.mmap.borrow().chunks[0].to_slice()[..METAPAGE_SIZE] == **recov_page {
                debug!("meta page intact!");
                self.failed.set(false);
            // reverting to in memory meta page
            } else {
                debug!("meta page corrupted, reverting state...");
                metapage_write(self, recov_page).expect("meta page recovery write error");
                rustix::fs::fsync(&self.database).expect("fsync metapage for restoration failed");
                self.failed.set(false);
            }
        };
        if let Err(e) = self.file_update() {
            warn!(%e, "file update failed! Reverting meta page...");
            metapage_load(self, recov_page); // in case the file writing fails, we revert back to the old meta page
            self.buffer.borrow_mut().hmap.clear(); // discard buffer
            self.failed.set(true);
            return Err(Error::PagerError(e));
        }
        Ok(())
    }

    /// write sequence
    fn file_update(&self) -> Result<(), PagerError> {
        // updating free list for next update
        self.freelist.borrow_mut().set_max_seq();
        // flush buffer to disk
        self.page_write()?;
        rustix::fs::fsync(&self.database)?;
        // write currently loaded metapage to disk
        metapage_write(self, &metapage_save(self))?;
        rustix::fs::fsync(&self.database)?;
        assert!(self.tree.borrow().root_ptr != self.freelist.borrow().head_page);
        assert!(self.tree.borrow().root_ptr != self.freelist.borrow().tail_page);
        Ok(())
    }

    /// helper function: writePages, flushes the buffer
    ///
    fn page_write(&self) -> Result<(), PagerError> {
        debug!("writing page...");
        let buf = self.buffer.borrow();
        let buf_len = buf.hmap.len();
        let npage = buf.npages;

        // extend the mmap if needed
        // number of page plus current buffer
        let new_size = (npage as usize + buf_len) * PAGE_SIZE; // amount of pages in bytes
        mmap_extend(self, new_size)
            .map_err(|e| {
                error!(%e, new_size, "Error when extending mmap");
            })
            .unwrap();
        debug!(
            nappend = buf.nappend,
            buffer = buf_len,
            "pages to be written:"
        );
        // iterate over buffer and write nodes to designated pages
        let mut bytes_written: usize = 0;
        for pair in buf.hmap.iter() {
            debug!("writing {:?} at {}", pair.1.get_type(), pair.0);
            assert!(pair.0.get() != 0); // never write to the meta page

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
        drop(buf);
        let mut buf = self.buffer.borrow_mut();
        buf.npages += buf.nappend;
        buf.nappend = 0;
        buf.hmap.clear();
        Ok(())
    }

    // decodes a page, checks buffer before reading disk
    // `BTree.get`, reads a page possibly from buffer or disk
    fn page_read(&self, ptr: Pointer, flag: NodeFlag) -> Node {
        // check buffer first
        debug!(node=?flag, %ptr, "page read");
        if let Some(n) = self.buffer.borrow_mut().hmap.remove(&ptr) {
            debug!("page found in buffer!");
            // re-adding to freelist to prevent memory leak
            self.freelist
                .borrow_mut()
                .append(ptr)
                .expect("page_read adding to freelist error");
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
        let mmap_ref = self.mmap.borrow();
        debug!(
            "decoding ptr: {}, amount of chunks {}, chunk 0 size {}",
            ptr.0,
            mmap_ref.chunks.len(),
            mmap_ref.chunks[0].len()
        );

        let mut start: usize = 0;
        for chunk in mmap_ref.chunks.iter() {
            let end = start + chunk.len / PAGE_SIZE;
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
            assert_ne!(ptr.0, 0);
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
        let buf_ref = &mut self.buffer.borrow_mut();
        // empty db has n_pages = 1 (meta page)
        assert!(node.fits_page());
        let ptr = Pointer(buf_ref.npages + buf_ref.nappend);
        buf_ref.nappend += 1;
        debug!(
            "encode: adding {:?} at page: {} to buffer",
            node.get_type(),
            ptr.0
        );
        buf_ref.hmap.insert(ptr, node);
        print_buffer(&buf_ref.hmap);
        assert_ne!(ptr.0, 0);
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

    // WIP
    fn cleanup(&self) {
        let list = self.freelist.borrow().collect_ptr();
        let npages = self.buffer.borrow().npages;
        ()
    }
}

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

    fn set_ptr(&mut self, field: MpField, ptr: Option<Pointer>) {
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
        root_ptr = ?pager.tree.borrow().root_ptr,
        npages = pager.buffer.borrow().npages,
        fl_head_ptr = ?fl_ref.head_page,
        fl_head_seq = fl_ref.head_seq,
        fl_tail_ptr = ?fl_ref.tail_page,
        fl_tail_seq = fl_ref.tail_seq,
        "saving meta page:"
    );
    use MpField as M;
    data.set_sig(DB_SIG);
    data.set_ptr(M::RootPtr, pager.tree.borrow().root_ptr);
    data.set_ptr(M::Npages, Some(pager.buffer.borrow().npages.into()));

    data.set_ptr(M::HeadPage, fl_ref.head_page);
    data.set_ptr(M::HeadSeq, Some(fl_ref.head_seq.into()));
    data.set_ptr(M::TailPage, fl_ref.tail_page);
    data.set_ptr(M::TailSeq, Some(fl_ref.tail_seq.into()));
    data
}

/// loads meta page object into pager
///
/// panics when called without initialized mmap
fn metapage_load(pager: &DiskPager, meta: &MetaPage) {
    debug!("loading metapage");
    let mut buf_ref = pager.buffer.borrow_mut();
    pager.tree.borrow_mut().root_ptr = match meta.read_ptr(MpField::RootPtr) {
        Pointer(0) => None,
        n => Some(n),
    };
    buf_ref.npages = meta.read_ptr(MpField::Npages).get();

    let mut fl_ref = pager.freelist.borrow_mut();
    fl_ref.head_page = Some(meta.read_ptr(MpField::HeadPage));
    fl_ref.head_seq = meta.read_ptr(MpField::HeadSeq).get() as usize;
    fl_ref.tail_page = Some(meta.read_ptr(MpField::TailPage));
    fl_ref.tail_seq = meta.read_ptr(MpField::TailSeq).get() as usize;
}

/// loads meta page from disk,
/// formerly root_read
fn metapage_read(pager: &DiskPager, file_size: u64) {
    if file_size == 0 {
        // empty file
        debug!("root read: empty file...");
        pager.buffer.borrow_mut().npages = 2; // reserved for meta page and one free list node
        pager.freelist.borrow_mut().head_page = Some(Pointer::from(1u64));
        pager.freelist.borrow_mut().tail_page = Some(Pointer::from(1u64));
        return;
    }
    debug!("root read: loading meta page");
    let mut meta = MetaPage::new();
    meta.copy_from_slice(&pager.mmap.borrow().chunks[0].to_slice()[..PAGE_SIZE]);
    metapage_load(pager, &meta);
    assert!(pager.buffer.borrow().npages != 0);
}

/// writes meta page to disk
fn metapage_write(pager: &DiskPager, meta: &MetaPage) -> Result<(), PagerError> {
    debug!("writing metapage to disk...");
    let r = rustix::io::pwrite(&pager.database, meta, 0)?;
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
        assert_eq!(pager.buffer.borrow().npages, 2);
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
