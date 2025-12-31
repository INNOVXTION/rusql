use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::collections::hash_map::Keys;
use std::ops::{Deref, DerefMut};
use std::os::fd::OwnedFd;
use std::ptr;
use std::rc::Rc;
use std::str::FromStr;

use rustix::fs::{fstat, fsync, ftruncate};
use tracing::{debug, error, info, instrument, warn};

use crate::create_file_sync;
use crate::database::BTree;
use crate::database::btree::{ScanIter, ScanMode};
use crate::database::errors::FLError;
use crate::database::helper::{as_page, debug_print_buffer};
use crate::database::pager::freelist::{FLConfig, FLNode, FreeList, GC};
use crate::database::pager::mmap::*;
use crate::database::tables::{Key, Record, Value};
use crate::database::{
    btree::{SetFlag, Tree, TreeNode},
    errors::{Error, PagerError},
    types::*,
};
/// outward facing api
pub(crate) trait KVEngine {
    fn get(&self, key: Key) -> Result<Value, Error>;
    fn scan(&self, mode: ScanMode) -> Result<Vec<Record>, Error>;
    fn set(&self, key: Key, val: Value, flag: SetFlag) -> Result<(), Error>;
    fn delete(&self, key: Key) -> Result<(), Error>;
}

/// wrapper struct
pub(crate) struct Envoy {
    envoy: Rc<EnvoyV1>,
}

impl Envoy {
    pub(crate) fn new(path: &'static str) -> Self {
        Envoy {
            envoy: EnvoyV1::open(path).expect("unexpected error"),
        }
    }
}

impl KVEngine for Envoy {
    fn get(&self, key: Key) -> Result<Value, Error> {
        self.envoy.get(key)
    }

    fn scan(&self, mode: ScanMode) -> Result<Vec<Record>, Error> {
        self.envoy.scan(mode)
    }

    fn set(&self, key: Key, val: Value, flag: SetFlag) -> Result<(), Error> {
        self.envoy.set(key, val, flag)
    }

    fn delete(&self, key: Key) -> Result<(), Error> {
        self.envoy.delete(key)
    }
}

/// indicates the encoding/decoding style of a node
#[derive(Debug)]
pub(crate) enum NodeFlag {
    Tree,
    Freelist,
}

pub(crate) struct EnvoyV1 {
    path: &'static str,
    pub database: OwnedFd,
    pub buffer: RefCell<Buffer>,
    pub mmap: RefCell<Mmap>,

    failed: Cell<bool>,

    tree: Rc<RefCell<dyn Tree<Codec = Self>>>,
    freelist: Rc<RefCell<dyn GC<Codec = Self>>>,
    // WIP
    // clean factor
    // counter after deletion for cleanup
}

#[derive(Debug)]
pub(crate) struct Buffer {
    pub hmap: HashMap<Pointer, Node>, // pages to be written
    pub nappend: u64,                 // number of pages to be appended
    pub npages: u64,                  // database size in number of pages
}

/// internal callback API
pub(crate) trait Pager {
    // tree callbacks
    fn page_read(&self, ptr: Pointer, flag: NodeFlag) -> Node; //tree decode
    fn page_alloc(&self, node: Node) -> Pointer; //tree encode
    fn dealloc(&self, ptr: Pointer); // tree dealloc/del

    // FL callbacks
    fn encode(&self, node: Node) -> Pointer; // FL encode
    fn update(&self, ptr: Pointer) -> *mut FLNode; // FL update
}

impl Pager for EnvoyV1 {
    /// decodes a page, checks buffer before reading disk
    ///
    /// `BTree.get`, reads a page possibly from buffer or disk
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

    /// allocates a new page to be encoded, checks freelist first before appending to disk
    ///
    /// pageAlloc, new
    fn page_alloc(&self, node: Node) -> Pointer {
        // check freelist first
        debug!("allocating ...");
        if let Some(ptr) = self.freelist.borrow_mut().get() {
            // loading page into buffer
            self.buffer.borrow_mut().hmap.insert(ptr, node);
            debug_print_buffer(&self.buffer.borrow().hmap);
            assert_ne!(ptr.0, 0);
            ptr
        } else {
            // increment append?
            debug!("appending to disk...");
            self.encode(node)
        }
    }

    /// PushTail
    fn dealloc(&self, ptr: Pointer) {
        self.freelist
            .borrow_mut()
            .append(ptr)
            .expect("dealloc error");
        ()
    }

    /// adds pages to buffer to be encoded to disk later (append)
    ///
    /// does not check if node exists in buffer!
    ///
    /// pageAppend
    fn encode(&self, node: Node) -> Pointer {
        let buf_ref = &mut self.buffer.borrow_mut();
        let ptr = Pointer(buf_ref.npages + buf_ref.nappend);

        // empty db has n_pages = 1 (meta page)
        assert!(node.fits_page());

        debug!(
            "encode: adding {:?} at page: {} to buffer",
            node.get_type(),
            ptr.0
        );

        buf_ref.nappend += 1;
        buf_ref.hmap.insert(ptr, node);
        debug_print_buffer(&buf_ref.hmap);

        assert_ne!(ptr.0, 0);
        ptr
    }

    /// callback for free list
    ///
    /// checks buffer for allocated page and returns pointer
    fn update(&self, ptr: Pointer) -> *mut FLNode {
        let buf = &mut self.buffer.borrow_mut();

        // checking buffer,...
        let entry = match buf.hmap.get_mut(&ptr) {
            Some(n) => {
                debug!("updating {} in buffer", ptr);
                n
            }
            None => {
                // decoding page from disk and loading it into buffer
                debug!(%ptr, "reading free list from disk...");
                buf.hmap.insert(ptr, self.decode(ptr, NodeFlag::Freelist));
                buf.hmap.get_mut(&ptr).expect("we just inserted it")
            }
        };

        match entry {
            Node::Freelist(flnode) => {
                let ptr = ptr::from_mut(flnode);
                debug_print_buffer(&buf.hmap);
                ptr
            }
            // only the freelist calls this function
            _ => unreachable!(),
        }
    }
}

impl EnvoyV1 {
    /// initializes pager
    ///
    /// opens file, and sets up callbacks for the tree
    pub fn open(path: &'static str) -> Result<Rc<Self>, Error> {
        let mut pager = Rc::new_cyclic(|w| EnvoyV1 {
            path,
            database: create_file_sync(path).expect("file open error"),
            failed: Cell::new(false),
            buffer: RefCell::new(Buffer {
                hmap: HashMap::<Pointer, Node>::new(),
                nappend: 0,
                npages: 0,
            }),
            mmap: RefCell::new(Mmap {
                total: 0,
                chunks: vec![],
            }),
            tree: Rc::new(RefCell::new(BTree::<Self>::new(w.clone()))),
            freelist: Rc::new(RefCell::new(FreeList::<Self>::new(w.clone()))),
        });

        let fd_size = fstat(&pager.database)
            .map_err(|e| {
                error!("Error when getting file size");
                Error::PagerError(PagerError::FDError(e))
            })
            .unwrap()
            .st_size as u64;

        mmap_extend(&pager, PAGE_SIZE).expect("mmap extend error");
        metapage_read(&mut pager, fd_size);

        #[cfg(test)]
        {
            debug!(
                "\npager initialized:\nmmap.total {}\nn_pages {}\nchunks.len {}",
                pager.mmap.borrow().total,
                pager.buffer.borrow().npages,
                pager.mmap.borrow().chunks.len(),
            );
        }

        Ok(pager)
    }

    #[instrument(name = "pager get", skip_all)]
    fn get(&self, key: Key) -> Result<Value, Error> {
        info!("getting...");

        self.tree
            .borrow()
            .search(key)
            .ok_or(Error::SearchError("value not found".to_string()))
    }

    #[instrument(name = "pager scan", skip_all)]
    fn scan(&self, mode: ScanMode) -> Result<Vec<Record>, Error> {
        info!("scanning...");

        Ok(self
            .tree
            .borrow()
            .scan(mode)
            .map_err(|e| Error::SearchError(format!("scan error {e}")))?
            .collect_records())
    }

    #[instrument(name = "pager set", skip_all)]
    fn set(&self, key: Key, val: Value, flag: SetFlag) -> Result<(), Error> {
        info!("inserting...");

        let recov_page = metapage_save(self); // saving current metapage for possible rollback
        self.tree.borrow_mut().set(key, val, flag).map_err(|e| {
            error!(%e, "tree error");
            e
        })?;
        self.update_or_revert(&recov_page)
    }

    #[instrument(name = "pager delete", skip_all)]
    fn delete(&self, key: Key) -> Result<(), Error> {
        info!("deleting...");

        let recov_page = metapage_save(self); // saving current metapage for possible rollback
        self.tree.borrow_mut().delete(key)?;
        self.update_or_revert(&recov_page)
    }

    fn check_recovery() {}

    #[instrument(name = "pager update file", skip_all)]
    fn update_or_revert(&self, recov_page: &MetaPage) -> Result<(), Error> {
        debug!("tree operation complete, updating file");

        // making sure the meta page is a known good state after a potential write error
        if self.failed.get() {
            debug!("failed update detected, restoring meta page...");

            metapage_write(self, recov_page).expect("meta page recovery write error");
            fsync(&self.database).expect("fsync metapage for restoration failed");
            self.failed.set(false);
        };

        // in case the file writing fails, we revert back to the old meta page
        if let Err(e) = self.file_update() {
            warn!(%e, "file update failed! Reverting meta page...");

            // save the pager from before the current operation to be rewritten later
            metapage_load(self, recov_page);

            // discard buffer
            self.buffer.borrow_mut().hmap.clear();
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
        fsync(&self.database)?;

        // write currently loaded metapage to disk
        metapage_write(self, &metapage_save(self))?;
        fsync(&self.database)?;

        Ok(())
    }

    /// helper function: writePages, flushes the buffer
    fn page_write(&self) -> Result<(), PagerError> {
        debug!("writing page...");

        let buf = self.buffer.borrow();
        let buf_len = buf.hmap.len();
        let npage = buf.npages;

        // extend the mmap if needed
        let new_size = (npage as usize + buf_len) * PAGE_SIZE; // amount of pages in bytes
        mmap_extend(self, new_size).map_err(|e| {
            error!(%e, new_size, "Error when extending mmap");
            e
        })?;
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
        if bytes_written != buf_len * PAGE_SIZE {
            return Err(PagerError::PageWriteError(
                "wrong amount of bytes written".to_string(),
            ));
        };

        //discard in-memory data
        drop(buf);
        let mut buf = self.buffer.borrow_mut();
        buf.npages += buf.nappend;
        buf.nappend = 0;
        buf.hmap.clear();
        Ok(())
    }

    /// decodes a page from disk
    ///
    /// kv.pageRead, db.pageRead -> pagereadfile
    fn decode(&self, ptr: Pointer, node_type: NodeFlag) -> Node {
        let mmap_ref = self.mmap.borrow();

        #[cfg(test)]
        {
            debug!(
                "decoding ptr: {}, amount of chunks {}, chunk 0 size {}",
                ptr.0,
                mmap_ref.chunks.len(),
                mmap_ref.chunks[0].len()
            );
        }

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

    /// triggers truncation logic once the freelist exceeds TRUNC_THRESHOLD entries
    fn cleanup_check(&self) -> Result<(), Error> {
        let list: Vec<Pointer> =
            self.freelist
                .borrow()
                .peek_ptr()
                .ok_or(FLError::TruncateError(
                    "could not retrieve pointer from FL".to_string(),
                ))?;

        if list.len() > TRUNC_THRESHOLD {
            self.truncate(list)
        } else {
            Ok(())
        }
    }

    /// attempts to truncate the file. Makes call to and modifies freelist. This function should therefore be called
    /// after tree operations. Truncation amount is based on count_trunc_pages() algorithm
    #[instrument(skip_all)]
    fn truncate(&self, list: Vec<Pointer>) -> Result<(), Error> {
        let npages = self.buffer.borrow().npages;
        if npages <= 2 {
            return Err(
                FLError::TruncateError("cant truncate from empty database".to_string()).into(),
            );
        }

        match count_trunc_pages(npages, &list) {
            Some(count) => {
                for i in 0..count {
                    // removing items from freelist
                    let ptr = self
                        .freelist
                        .borrow_mut()
                        .get()
                        .ok_or(FLError::PopError("couldnt pop from freelist".to_string()))?;

                    debug_assert_eq!(list[i as usize], ptr);

                    if list[i as usize] != ptr {
                        return Err(FLError::TruncateError(format!(
                            "pointer {}, doesnt match {}",
                            list[i as usize], ptr
                        ))
                        .into());
                    }
                }
                let new_npage = npages - count;

                self.buffer.borrow_mut().npages = new_npage;
                metapage_write(self, &metapage_save(self))?;
                fsync(&self.database)?;

                ftruncate(&self.database, new_npage * PAGE_SIZE as u64)?;
                fsync(&self.database)?;

                Ok(())
            }
            None => Ok(()),
        }
    }
}

/// returns the number of pages that can be safely truncated, by evaluating a contiguous sequence at the end of the freelist. This function has O(n²) performance.
fn count_trunc_pages(npages: u64, list: &[Pointer]) -> Option<u64> {
    if list.is_empty() || npages <= 2 {
        return None;
    }

    let pages: Vec<u64> = list.iter().map(|p| p.get()).collect();

    // Start checking from the maximum possible and work down
    let max_possible = pages.len().min((npages - 2) as usize);
    assert!(max_possible > 0);

    for count in (1..=max_possible).rev() {
        let mut prefix: Vec<u64> = pages[0..count].to_vec();
        prefix.sort_unstable();

        // Check if these pages are exactly [npages-count, ..., npages-1]
        let tail_start = npages - count as u64;
        let is_valid_tail = prefix
            .iter()
            .enumerate()
            .all(|(i, &page)| page == tail_start + i as u64);

        if is_valid_tail {
            return Some(count as u64);
        }
    }

    None
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
fn metapage_save(pager: &EnvoyV1) -> MetaPage {
    let flc = pager.freelist.borrow().get_config();
    let tr_ref = pager.tree.borrow();
    let mut data = MetaPage::new();

    debug!(
        sig = DB_SIG,
        root_ptr = ?tr_ref.get_root(),
        npages = pager.buffer.borrow().npages,
        fl_head_ptr = ?flc.head_page,
        fl_head_seq = flc.head_seq,
        fl_tail_ptr = ?flc.tail_page,
        fl_tail_seq = flc.tail_seq,
        "saving meta page:"
    );

    use MpField as M;
    data.set_sig(DB_SIG);
    data.set_ptr(M::RootPtr, tr_ref.get_root());
    data.set_ptr(M::Npages, Some(pager.buffer.borrow().npages.into()));

    data.set_ptr(M::HeadPage, flc.head_page);
    data.set_ptr(M::HeadSeq, Some(flc.head_seq.into()));
    data.set_ptr(M::TailPage, flc.tail_page);
    data.set_ptr(M::TailSeq, Some(flc.tail_seq.into()));

    data
}

/// loads meta page object into pager
///
/// panics when called without initialized mmap
fn metapage_load(pager: &EnvoyV1, meta: &MetaPage) {
    debug!("loading metapage");

    let mut tr_ref = pager.tree.borrow_mut();

    match meta.read_ptr(MpField::RootPtr) {
        Pointer(0) => tr_ref.set_root(None),
        n => tr_ref.set_root(Some(n)),
    };

    pager.buffer.borrow_mut().npages = meta.read_ptr(MpField::Npages).get();

    let flc = FLConfig {
        head_page: Some(meta.read_ptr(MpField::HeadPage)),
        head_seq: meta.read_ptr(MpField::HeadSeq).get() as usize,

        tail_page: Some(meta.read_ptr(MpField::TailPage)),
        tail_seq: meta.read_ptr(MpField::TailSeq).get() as usize,
    };

    pager.freelist.borrow_mut().set_config(&flc);
}

/// loads meta page from disk,
/// formerly root_read
fn metapage_read(pager: &EnvoyV1, file_size: u64) {
    if file_size == 0 {
        // empty file
        debug!("root read: empty file...");
        pager.buffer.borrow_mut().npages = 2; // reserved for meta page and one free list node
        let flc = FLConfig {
            head_page: Some(Pointer::from(1u64)),
            head_seq: 0,
            tail_page: Some(Pointer::from(1u64)),
            tail_seq: 0,
        };
        pager.freelist.borrow_mut().set_config(&flc);
        return;
    }
    debug!("root read: loading meta page");

    let mut meta = MetaPage::new();
    meta.copy_from_slice(&pager.mmap.borrow().chunks[0].to_slice()[..METAPAGE_SIZE]);
    metapage_load(pager, &meta);

    assert!(pager.buffer.borrow().npages != 0);
}

/// writes meta page to disk
fn metapage_write(pager: &EnvoyV1, meta: &MetaPage) -> Result<(), PagerError> {
    debug!("writing metapage to disk...");
    let r = rustix::io::pwrite(&pager.database, meta, 0)?;
    Ok(())
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::database::helper::cleanup_file;
    use rand::Rng;
    use test_log::test;

    #[test]
    fn open_pager() {
        let path = "test-files/open_pager.rdb";
        cleanup_file(path);
        let pager = EnvoyV1::open(path).unwrap();
        assert_eq!(pager.buffer.borrow().npages, 2);
        cleanup_file(path);
    }

    #[test]
    fn disk_insert1() {
        let path = "test-files/disk_insert1.rdb";
        cleanup_file(path);
        let pager = EnvoyV1::open(path).unwrap();
        pager
            .set(
                "1".into(),
                Value::from_unencoded_str("val"),
                SetFlag::UPSERT,
            )
            .unwrap();
        assert_eq!(
            pager.get("1".into()).unwrap(),
            Value::from_unencoded_str("val")
        );
        cleanup_file(path);
    }

    #[test]
    fn disk_insert2() {
        let path = "test-files/disk_insert2.rdb";
        cleanup_file(path);
        let pager = EnvoyV1::open(path).unwrap();

        for i in 1u16..=300u16 {
            pager
                .set(format!("{i}").into(), "value".into(), SetFlag::UPSERT)
                .unwrap()
        }
        for i in 1u16..=300u16 {
            assert_eq!(pager.get(format!("{i}").into()).unwrap(), "value".into())
        }
        cleanup_file(path);
    }

    #[test]
    fn disk_delete1() {
        let path = "test-files/disk_delete1.rdb";
        cleanup_file(path);
        let pager = EnvoyV1::open(path).unwrap();

        for i in 1u16..=300u16 {
            pager
                .set(format!("{i}").into(), "value".into(), SetFlag::UPSERT)
                .unwrap()
        }
        for i in 1u16..=300u16 {
            pager.delete(format!("{i}").into()).unwrap();
        }
        cleanup_file(path);
    }

    #[test]
    fn disk_random1() {
        let path = "test-files/disk_random1.rdb";
        cleanup_file(path);
        let pager = EnvoyV1::open(path).unwrap();

        for _ in 1u16..=1000 {
            pager
                .set(
                    format!("{:?}", rand::rng().random_range(1..1000)).into(),
                    Value::from_unencoded_str("val"),
                    SetFlag::UPSERT,
                )
                .unwrap()
        }
        cleanup_file(path);
    }

    #[test]
    fn disk_delete2() {
        let path = "test-files/disk_delete2.rdb";
        cleanup_file(path);
        let pager = EnvoyV1::open(path).unwrap();

        for k in 1u16..=1000 {
            pager
                .set(
                    format!("{}", k).into(),
                    Value::from_unencoded_str("val"),
                    SetFlag::UPSERT,
                )
                .unwrap()
        }
        for k in 1u16..=1000 {
            pager.delete(format!("{}", k).into()).unwrap()
        }
        cleanup_file(path);
    }

    #[test]
    fn cleanup_helper1() {
        let list: Vec<Pointer> = vec![Pointer(6), Pointer(9), Pointer(8), Pointer(7), Pointer(4)];
        let res = count_trunc_pages(10, &list);
        assert_eq!(res, Some(4));

        let list: Vec<Pointer> = vec![Pointer(6), Pointer(9), Pointer(8), Pointer(4), Pointer(7)];
        let res = count_trunc_pages(10, &list);
        assert_eq!(res, None);

        let list: Vec<Pointer> = vec![Pointer(9), Pointer(8), Pointer(7), Pointer(6), Pointer(5)];
        let res = count_trunc_pages(10, &list);
        assert_eq!(res, Some(5));

        let list: Vec<Pointer> = vec![Pointer(9), Pointer(4), Pointer(7), Pointer(6), Pointer(5)];
        let res = count_trunc_pages(10, &list);
        assert_eq!(res, Some(1));

        let list: Vec<Pointer> = vec![Pointer(1), Pointer(4), Pointer(7), Pointer(6), Pointer(5)];
        let res = count_trunc_pages(10, &list);
        assert_eq!(res, None);
    }

    // #[test]
    // fn cleanup_test() {
    //     let path = "test-files/disk_cleanup.rdb";
    //     cleanup_file(path);
    //     let pager = EnvoyV1::open(path).unwrap();
    //     // assert_eq!(fd_size, PAGE_SIZE as u64 * 2);
    //     for k in 1u16..=500 {
    //         pager
    //             .set(format!("{}", k).into(), Value::from_unencoded_str("val"))
    //             .unwrap()
    //     }
    //     for k in 1u16..=500 {
    //         pager.delete(format!("{}", k).into()).unwrap()
    //     }
    //     pager.truncate();
    //     let fd_size = fs::fstat(&pager.database)
    //         .map_err(|e| {
    //             error!("Error when getting file size");
    //             Error::PagerError(PagerError::FDError(e))
    //         })
    //         .unwrap()
    //         .st_size as u64;
    //     cleanup_file(path);
    //     assert_eq!(fd_size, PAGE_SIZE as u64 * 2);
    // }
}

#[cfg(test)]
mod truncate {
    use super::*;

    fn ptr(page: u64) -> Pointer {
        Pointer::from(page)
    }

    #[test]
    fn test_cleanup_check_empty_list() {
        let result = count_trunc_pages(100, &[]);
        assert_eq!(result, None);
    }

    #[test]
    fn test_cleanup_check_npages_too_small() {
        let list = vec![ptr(0), ptr(1)];
        let result = count_trunc_pages(2, &list);
        assert_eq!(result, None);
    }

    #[test]
    fn test_cleanup_check_small_tail_sequence() {
        // Only 5 pages at tail - function should still return it
        let list = vec![ptr(99), ptr(98), ptr(97), ptr(96), ptr(95)];
        let result = count_trunc_pages(100, &list);
        assert_eq!(result, Some(5), "Should return even small sequences");
    }

    #[test]
    fn test_cleanup_check_single_page() {
        let list = vec![ptr(99)];
        let result = count_trunc_pages(100, &list);
        assert_eq!(result, Some(1), "Single tail page should be detected");
    }

    #[test]
    fn test_cleanup_check_exactly_100_pages() {
        let list: Vec<Pointer> = (900..1000).map(ptr).collect();
        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, Some(100));
    }

    #[test]
    fn test_cleanup_check_tail_sequence_unordered() {
        let mut list: Vec<Pointer> = (900..1000).map(ptr).collect();
        list.reverse();

        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, Some(100), "Order shouldn't matter");
    }

    #[test]
    fn test_cleanup_check_large_tail_sequence() {
        let list: Vec<Pointer> = (800..1000).map(ptr).collect();
        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, Some(200));
    }

    #[test]
    fn test_cleanup_check_gap_in_sequence() {
        // Missing page 98
        let list = vec![ptr(99), ptr(97), ptr(96), ptr(95)];
        let result = count_trunc_pages(100, &list);
        assert_eq!(result, Some(1), "Gap breaks the tail sequence");
    }

    #[test]
    fn test_cleanup_check_not_at_tail() {
        let list: Vec<Pointer> = (400..500).map(ptr).collect();
        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, None, "Pages not at tail should return None");
    }

    #[test]
    fn test_cleanup_check_first_element_breaks_pattern() {
        let mut list = vec![ptr(500)];
        list.extend((900..1000).map(ptr));

        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, None, "First non-tail element breaks pattern");
    }

    #[test]
    fn test_cleanup_check_entire_file_nearly_free() {
        let list: Vec<Pointer> = (2..1000).map(ptr).collect();
        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, Some(998));
    }

    #[test]
    fn test_cleanup_check_shuffled_valid_tail() {
        let mut list: Vec<Pointer> = (850..1000).map(ptr).collect();
        let len = list.len();

        for i in 0..list.len() / 2 {
            list.swap(i, len - 1 - i);
        }

        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, Some(150));
    }

    #[test]
    fn test_cleanup_check_duplicate_pages() {
        // claude got confused, this test might not make sense
        let list = vec![ptr(999), ptr(999), ptr(998), ptr(997)];

        let result = count_trunc_pages(1000, &list);
        assert_eq!(
            result,
            Some(1),
            "Finds largest valid tail despite duplicate"
        );
    }

    #[test]
    fn test_cleanup_check_realistic_fragmentation() {
        let mut list = vec![];

        // 150 tail pages in reverse order
        for i in (850..1000).rev() {
            list.push(ptr(i));
        }

        // Some middle pages
        list.extend(vec![ptr(500), ptr(501), ptr(502)]);

        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, Some(150));
    }

    #[test]
    fn test_cleanup_check_alternating_gaps() {
        // Even numbers only: 990, 992, 994, 996, 998
        let list = vec![ptr(998), ptr(996), ptr(994), ptr(992), ptr(990)];
        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, None, "Gaps make it non-consecutive");
    }

    #[test]
    fn test_cleanup_check_max_possible_calculation() {
        // Test that max_possible = min(list.len(), npages-2)

        // Case 1: list.len() < npages-2
        let list: Vec<Pointer> = (990..1000).map(ptr).collect(); // 10 items
        let result = count_trunc_pages(1000, &list); // npages-2 = 998
        assert_eq!(result, Some(10), "Limited by list length");

        // Case 2: list.len() > npages-2
        let list2: Vec<Pointer> = (0..100).map(ptr).collect(); // 100 items
        let result2 = count_trunc_pages(50, &list2); // npages-2 = 48
        // Can only check up to 48 pages
        assert_eq!(result2, None, "Not a valid tail for npages=50");
    }
}
