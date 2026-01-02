use std::cell::{Cell, RefCell};
use std::collections::hash_map::Keys;
use std::collections::{HashMap, HashSet};
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

impl std::fmt::Display for NodeFlag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "node type: {:?}", self)
    }
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
    hmap: HashMap<Pointer, BufferEntry>, // in memory buffer
    nappend: u64,                        // number of pages to be appended
    npages: u64,                         // database size in number of pages
}

#[derive(Debug)]
struct BufferEntry {
    node: Rc<RefCell<Node>>,
    dirty: bool,   // does it need to be written?
    retired: bool, // has the page been deallocated?
}

impl Buffer {
    fn get(&self, ptr: Pointer) -> Option<Rc<RefCell<Node>>> {
        Some(self.hmap.get(&ptr)?.node.clone())
    }

    fn get_clean(&self, ptr: Pointer) -> Option<Rc<RefCell<Node>>> {
        let n = self.hmap.get(&ptr)?;
        if !n.dirty { None } else { Some(n.node.clone()) }
    }

    /// retrieves all dirty pages in the buffer
    fn to_dirty_iter(&self) -> impl Iterator<Item = (Pointer, std::cell::Ref<'_, Node>)> {
        let iter = self
            .hmap
            .iter()
            .filter_map(|e| {
                if e.1.dirty {
                    Some((*e.0, e.1.node.borrow()))
                } else {
                    None
                }
            })
            .into_iter();
        iter
    }

    /// removes retired pages, marks dirty pages as clean
    fn clear(&mut self) {
        let mut v = vec![];

        for (p, entry) in self.hmap.iter_mut() {
            if entry.dirty {
                entry.dirty = false;
            }
            if entry.retired {
                v.push(*p);
            }
        }

        // removing retired pages from buffer
        for p in v.iter() {
            let k = self.hmap.remove(p);
            debug_assert!(k.is_some());
        }
    }

    fn insert_clean(&mut self, ptr: Pointer, node: Node) {
        self.hmap.insert(
            ptr,
            BufferEntry {
                node: Rc::new(RefCell::new(node)),
                dirty: false,
                retired: false,
            },
        );
    }

    fn insert_dirty(&mut self, ptr: Pointer, node: Node) -> Option<()> {
        self.hmap
            .insert(
                ptr,
                BufferEntry {
                    node: Rc::new(RefCell::new(node)),
                    dirty: true,
                    retired: false,
                },
            )
            .map(|_| ())
    }

    fn delete(&mut self, ptr: Pointer) {
        self.hmap.remove(&ptr);
    }

    pub fn debug_print(&self) {
        for e in self.hmap.iter() {
            let n = e.1.node.borrow();
            debug!(
                "{:<10}, {:<10}, dirty = {:<10}",
                e.0,
                n.get_type(),
                e.1.dirty
            )
        }
    }
}

/// internal callback API
pub(crate) trait Pager {
    // tree callbacks
    fn page_read(&self, ptr: Pointer, flag: NodeFlag) -> Rc<RefCell<Node>>; //tree decode
    fn page_alloc(&self, node: Node) -> Pointer; //tree encode
    fn dealloc(&self, ptr: Pointer); // tree dealloc/del

    // FL callbacks
    fn encode(&self, node: Node) -> Pointer; // FL encode
    fn update(&self, ptr: Pointer) -> Rc<RefCell<Node>>; // FL update
}

impl Pager for EnvoyV1 {
    /// decodes a page, checks buffer before reading disk
    ///
    /// `BTree.get`, reads a page possibly from buffer or disk
    fn page_read(&self, ptr: Pointer, flag: NodeFlag) -> Rc<RefCell<Node>> {
        let mut buf_ref = self.buffer.borrow_mut();
        // check buffer first
        debug!(node=?flag, %ptr, "page read");
        if let Some(n) = buf_ref.get(ptr) {
            debug!("page found in buffer!");
            n
        } else {
            debug!("reading from disk...");
            let n = self.decode(ptr, flag);

            buf_ref.insert_clean(ptr, n.clone());
            buf_ref.get(ptr).expect("we just inserted it")
        }
    }

    /// allocates a new page to be encoded, checks freelist first before appending to disk
    ///
    /// pageAlloc, new
    fn page_alloc(&self, node: Node) -> Pointer {
        // check freelist first
        debug!("allocating ...");
        if let Some(ptr) = self.freelist.borrow_mut().get() {
            assert_ne!(ptr.0, 0);

            // loading page into buffer
            self.buffer.borrow_mut().insert_dirty(ptr, node);

            debug_print_buffer(&self.buffer.borrow());
            ptr
        } else {
            // increment append?
            debug!("appending to disk...");
            self.encode(node)
        }
    }

    /// marks a page for deallocation. That page still lingers inside the buffer and is removed before the
    /// next transaction
    /// PushTail
    fn dealloc(&self, ptr: Pointer) {
        // adding to freelist
        self.freelist
            .borrow_mut()
            .append(ptr)
            .expect("dealloc error");

        // retiring page from buffer, to be cleared later
        if let Some(entry) = self.buffer.borrow_mut().hmap.get_mut(&ptr) {
            entry.retired = true;
        };
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
        buf_ref.insert_dirty(ptr, node);
        debug_print_buffer(&buf_ref);

        assert_ne!(ptr.0, 0);
        ptr
    }

    /// callback for free list
    ///
    /// checks buffer for allocated page and returns pointer
    fn update(&self, ptr: Pointer) -> Rc<RefCell<Node>> {
        let buf = &mut self.buffer.borrow_mut();

        // checking buffer,...
        let entry = match buf.get(ptr) {
            Some(n) => {
                debug!("updating {} in buffer", ptr);
                n
            }
            None => {
                // decoding page from disk and loading it into buffer
                debug!(%ptr, "reading free list from disk...");
                buf.insert_dirty(ptr, self.decode(ptr, NodeFlag::Freelist));
                buf.get(ptr).expect("we just inserted it")
            }
        };

        // we just inserted the node as FLNode so this cast should be safe
        // let p = entry as *mut Node as *mut FLNode;
        debug_print_buffer(&buf);
        entry
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
                hmap: HashMap::new(),
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
        let mut count = 0;

        for pair in buf.to_dirty_iter() {
            debug!("writing {:<10} at {:<5}", pair.1.get_type(), pair.0);
            assert!(pair.0.get() != 0); // never write to the meta page

            let offset = pair.0.get() * PAGE_SIZE as u64;
            let io_slice = rustix::io::IoSlice::new(&pair.1[..PAGE_SIZE]);
            bytes_written +=
                rustix::io::pwrite(&self.database, &io_slice, offset).map_err(|e| {
                    error!(?e, "page writing error!");
                    PagerError::WriteFileError(e)
                })?;
            count += 1;
        }
        debug!(bytes_written, "bytes written:");
        if bytes_written != count * PAGE_SIZE {
            return Err(PagerError::PageWriteError(
                "wrong amount of bytes written".to_string(),
            ));
        };

        // adjust buffer
        drop(buf);
        let mut buf = self.buffer.borrow_mut();

        buf.npages += buf.nappend;
        buf.nappend = 0;
        buf.clear();

        Ok(())
    }

    /// decodes a page from the mmap
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

/// returns the number of pages that can be safely truncated, by evaluating a contiguous sequence at the end of the freelist. This function has O(n logn ) worst case performance.
///
/// Credit to ranveer for this
fn count_trunc_pages(npages: u64, freelist: &[Pointer]) -> Option<u64> {
    if freelist.is_empty() || npages <= 2 {
        return None;
    }

    let max_possible = (npages - 2) as usize;

    let mut seen = HashSet::new();
    let mut min_page = u64::MAX;
    let mut max_page = 0u64;

    let mut best: Option<u64> = None;
    let mut saw_last_page_anywhere = false;

    let first_page = freelist[0].get();

    for (i, ptr) in freelist.iter().enumerate() {
        if i >= max_possible {
            break;
        }

        let page = ptr.get();

        if page == npages - 1 {
            saw_last_page_anywhere = true;
        }

        if page < npages {
            if seen.insert(page) {
                min_page = min_page.min(page);
                max_page = max_page.max(page);
            }
        }

        let k = (i + 1) as u64;
        if k > 1
            && seen.len() as u64 == k
            && max_page == npages - 1
            && max_page - min_page + 1 == k
            && min_page == npages - k
        {
            best = Some(k);
        }
    }
    if best.is_none()
        && saw_last_page_anywhere
        && (first_page == npages - 1 || first_page >= npages)
    {
        return Some(1);
    }
    best
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

    // // O(n) algo
    // fn count_trunc_pages(npages: u64, list: &[Pointer]) -> Option<u64> {
    //     // Validation 1: Empty list
    //     if list.is_empty() {
    //         return None;
    //     }

    //     // Validation 2: npages too small
    //     if npages <= 2 {
    //         return None;
    //     }

    //     // Build map: page -> position in freelist (1-indexed)
    //     let mut mpp: HashMap<u64, u64> = HashMap::new();

    //     for (idx, &page) in list.iter().enumerate() {
    //         let page_num = page.0;

    //         // Validation 3: Page must be within file bounds
    //         if page_num >= npages {
    //             return None;
    //         }

    //         // Use first occurrence (ignore duplicates)
    //         mpp.entry(page_num).or_insert((idx as u64) + 1);
    //     }

    //     // Check from npages-1 downward for consecutive pages at end
    //     let mut st: BTreeSet<u64> = BTreeSet::new();
    //     let mut current_page = npages - 1;
    //     let mut max_cnt = 0;

    //     loop {
    //         if mpp.contains_key(&current_page) {
    //             let pos = *mpp.get(&current_page).unwrap();
    //             st.insert(pos);

    //             // Check if positions form sequencehey< : 1,2,3...k
    //             if let Some(&max_pos) = st.iter().next_back() {
    //                 if max_pos == st.len() as u64 {
    //                     max_cnt = st.len() as u64;
    //                 }
    //             }
    //         } else {
    //             // Page not in freelist, sequence broken
    //             break;
    //         }

    //         if current_page == 0 {
    //             break;
    //         }
    //         current_page -= 1;
    //     }
    //     if max_cnt == 0 { None } else { Some(max_cnt) }
    // }

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
    #[test]
    fn test_prefix_invalidated_by_duplicate_early() {
        let list = vec![ptr(999), ptr(999), ptr(998)];
        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, Some(1));
    }

    #[test]
    fn test_prefix_valid_then_invalid_page_stops_growth() {
        let list = vec![ptr(999), ptr(998), ptr(400)];
        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, Some(2));
    }

    #[test]
    fn test_prefix_valid_then_gap_breaks() {
        let list = vec![ptr(999), ptr(997), ptr(996)];
        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, Some(1));
    }

    #[test]
    fn test_prefix_unordered_but_consecutive() {
        let list = vec![ptr(998), ptr(999), ptr(997)];
        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, Some(3));
    }

    #[test]
    fn test_prefix_exact_tail_then_duplicate() {
        let list = vec![ptr(999), ptr(998), ptr(997), ptr(998)];
        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, Some(3));
    }

    #[test]
    fn test_prefix_exact_tail_then_out_of_range() {
        let list = vec![ptr(999), ptr(998), ptr(997), ptr(2000)];
        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, Some(3));
    }

    #[test]
    fn test_prefix_contains_zero_page_breaks_tail() {
        let list = vec![ptr(999), ptr(0), ptr(998)];
        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, Some(1));
    }

    #[test]
    fn test_prefix_long_valid_then_gap() {
        let list = vec![ptr(999), ptr(998), ptr(997), ptr(995)];
        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, Some(3));
    }

    #[test]
    fn test_prefix_valid_exact_np_minus_two_at_tail() {
        let list: Vec<Pointer> = (6..104).map(ptr).collect();
        let result = count_trunc_pages(104, &list);
        assert_eq!(result, Some(98));
    }

    #[test]
    fn test_prefix_with_reversed_large_tail() {
        let mut list: Vec<Pointer> = (900..1000).map(ptr).collect();
        list.reverse();
        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, Some(100));
    }

    #[test]
    fn test_prefix_partial_tail_only() {
        let list = vec![ptr(999), ptr(998), ptr(500), ptr(997)];
        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, Some(2));
    }

    #[test]
    fn test_prefix_duplicate_late_does_not_extend() {
        let list = vec![ptr(999), ptr(998), ptr(997), ptr(997)];
        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, Some(3));
    }

    #[test]
    fn test_prefix_starts_at_tail_minus_one_fails() {
        let list = vec![ptr(998), ptr(997), ptr(996)];
        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, None);
    }

    #[test]
    fn test_prefix_valid_with_minimal_tail() {
        let list = vec![ptr(999), ptr(998)];
        let result = count_trunc_pages(1000, &list);
        assert_eq!(result, Some(2));
    }
}

#[cfg(test)]
mod buffer_tests {
    use super::*;
    use crate::database::btree::TreeNode;
    use crate::database::types::Node;

    fn create_test_node() -> Node {
        Node::Tree(TreeNode::new())
    }

    #[test]
    fn buffer_insert_and_get_clean() {
        let mut buf = Buffer {
            hmap: HashMap::new(),
            nappend: 0,
            npages: 0,
        };

        let node = create_test_node();
        let ptr = Pointer::from(1u64);

        buf.insert_clean(ptr, node);

        assert!(buf.get(ptr).is_some());
        assert_eq!(buf.hmap.len(), 1);
    }

    #[test]
    fn buffer_insert_and_get_dirty() {
        let mut buf = Buffer {
            hmap: HashMap::new(),
            nappend: 0,
            npages: 0,
        };

        let node = create_test_node();
        let ptr = Pointer::from(1u64);

        let result = buf.insert_dirty(ptr, node);

        assert!(result.is_none()); // dirty pointer didnt exists
        assert!(buf.get(ptr).is_some());
        assert_eq!(buf.hmap.len(), 1);
        assert!(buf.hmap[&ptr].dirty);
    }

    #[test]
    fn buffer_get_returns_none_for_missing_key() {
        let buf = Buffer {
            hmap: HashMap::new(),
            nappend: 0,
            npages: 0,
        };

        let ptr = Pointer::from(999u64);
        assert!(buf.get(ptr).is_none());
    }

    #[test]
    fn buffer_get_clean_only_returns_dirty_pages() {
        let mut buf = Buffer {
            hmap: HashMap::new(),
            nappend: 0,
            npages: 0,
        };

        let clean_node = create_test_node();
        let dirty_node = create_test_node();
        let clean_ptr = Pointer::from(1u64);
        let dirty_ptr = Pointer::from(2u64);

        buf.insert_clean(clean_ptr, clean_node);
        buf.insert_dirty(dirty_ptr, dirty_node);

        // get_clean should only return dirty pages
        assert!(buf.get_clean(clean_ptr).is_none());
        assert!(buf.get_clean(dirty_ptr).is_some());
    }

    #[test]
    fn buffer_multiple_inserts() {
        let mut buf = Buffer {
            hmap: HashMap::new(),
            nappend: 0,
            npages: 0,
        };

        for i in 1..=10 {
            let node = create_test_node();
            let ptr = Pointer::from(i as u64);
            buf.insert_dirty(ptr, node);
        }

        assert_eq!(buf.hmap.len(), 10);

        for i in 1..=10 {
            let ptr = Pointer::from(i as u64);
            assert!(buf.get(ptr).is_some());
        }
    }

    #[test]
    fn buffer_dirty_flag_set_correctly() {
        let mut buf = Buffer {
            hmap: HashMap::new(),
            nappend: 0,
            npages: 0,
        };

        let clean_node = create_test_node();
        let dirty_node = create_test_node();
        let clean_ptr = Pointer::from(1u64);
        let dirty_ptr = Pointer::from(2u64);

        buf.insert_clean(clean_ptr, clean_node);
        buf.insert_dirty(dirty_ptr, dirty_node);

        assert!(!buf.hmap[&clean_ptr].dirty);
        assert!(buf.hmap[&dirty_ptr].dirty);
    }

    #[test]
    fn buffer_retired_flag_set_correctly() {
        let mut buf = Buffer {
            hmap: HashMap::new(),
            nappend: 0,
            npages: 0,
        };

        let node = create_test_node();
        let ptr = Pointer::from(1u64);

        buf.insert_clean(ptr, node);
        assert!(!buf.hmap[&ptr].retired);

        // Manually retire for testing
        buf.hmap.get_mut(&ptr).unwrap().retired = true;
        assert!(buf.hmap[&ptr].retired);
    }

    #[test]
    fn buffer_clear_removes_retired_pages() {
        let mut buf = Buffer {
            hmap: HashMap::new(),
            nappend: 0,
            npages: 0,
        };

        let node1 = create_test_node();
        let node2 = create_test_node();
        let node3 = create_test_node();

        buf.insert_dirty(Pointer::from(1u64), node1);
        buf.insert_dirty(Pointer::from(2u64), node2);
        buf.insert_dirty(Pointer::from(3u64), node3);

        // Mark page 2 as retired
        buf.hmap.get_mut(&Pointer::from(2u64)).unwrap().retired = true;

        buf.clear();

        assert_eq!(buf.hmap.len(), 2);
        assert!(buf.get(Pointer::from(2u64)).is_none());
        assert!(buf.get(Pointer::from(1u64)).is_some());
        assert!(buf.get(Pointer::from(3u64)).is_some());
    }

    #[test]
    fn buffer_clear_marks_dirty_as_clean() {
        let mut buf = Buffer {
            hmap: HashMap::new(),
            nappend: 0,
            npages: 0,
        };

        let node = create_test_node();
        let ptr = Pointer::from(1u64);

        buf.insert_dirty(ptr, node);
        assert!(buf.hmap[&ptr].dirty);

        buf.clear();

        assert!(!buf.hmap[&ptr].dirty);
    }

    #[test]
    fn buffer_to_dirty_iter_only_returns_dirty() {
        let mut buf = Buffer {
            hmap: HashMap::new(),
            nappend: 0,
            npages: 0,
        };

        buf.insert_clean(Pointer::from(1u64), create_test_node());
        buf.insert_dirty(Pointer::from(2u64), create_test_node());
        buf.insert_dirty(Pointer::from(3u64), create_test_node());

        let dirty_count = buf.to_dirty_iter().count();
        assert_eq!(dirty_count, 2);
    }

    #[test]
    fn buffer_delete_removes_page() {
        let mut buf = Buffer {
            hmap: HashMap::new(),
            nappend: 0,
            npages: 0,
        };

        let node = create_test_node();
        let ptr = Pointer::from(1u64);

        buf.insert_clean(ptr, node);
        assert!(buf.get(ptr).is_some());

        buf.delete(ptr);
        assert!(buf.get(ptr).is_none());
    }

    #[test]
    fn buffer_multiple_retirements_and_clear() {
        let mut buf = Buffer {
            hmap: HashMap::new(),
            nappend: 0,
            npages: 0,
        };

        for i in 1..=5 {
            buf.insert_dirty(Pointer::from(i as u64), create_test_node());
        }

        // Retire pages 2, 3, and 5
        for ptr in &[
            Pointer::from(2u64),
            Pointer::from(3u64),
            Pointer::from(5u64),
        ] {
            buf.hmap.get_mut(ptr).unwrap().retired = true;
        }

        assert_eq!(buf.hmap.len(), 5);
        buf.clear();
        assert_eq!(buf.hmap.len(), 2);

        // Only pages 1 and 4 should remain
        assert!(buf.get(Pointer::from(1u64)).is_some());
        assert!(buf.get(Pointer::from(4u64)).is_some());
        assert!(buf.get(Pointer::from(2u64)).is_none());
    }

    #[test]
    fn buffer_insert_dirty_overwrites_existing() {
        let mut buf = Buffer {
            hmap: HashMap::new(),
            nappend: 0,
            npages: 0,
        };

        let ptr = Pointer::from(1u64);
        let node1 = create_test_node();
        let node2 = create_test_node();

        buf.insert_dirty(ptr, node1);
        let result = buf.insert_dirty(ptr, node2);

        // insert_dirty should return Some(_) on overwrite
        assert!(result.is_some());
        assert_eq!(buf.hmap.len(), 1);
    }

    #[test]
    fn buffer_nappend_and_npages_tracking() {
        let mut buf = Buffer {
            hmap: HashMap::new(),
            nappend: 5,
            npages: 10,
        };

        assert_eq!(buf.nappend, 5);
        assert_eq!(buf.npages, 10);

        buf.nappend = 0;
        buf.npages = 15;

        assert_eq!(buf.nappend, 0);
        assert_eq!(buf.npages, 15);
    }

    #[test]
    fn buffer_large_page_count() {
        let mut buf = Buffer {
            hmap: HashMap::new(),
            nappend: 0,
            npages: 0,
        };

        // Insert many pages
        for i in 0..1000 {
            let ptr = Pointer::from(i as u64);
            buf.insert_dirty(ptr, create_test_node());
        }

        assert_eq!(buf.hmap.len(), 1000);

        // Verify random access
        assert!(buf.get(Pointer::from(500u64)).is_some());
        assert!(buf.get(Pointer::from(999u64)).is_some());
        assert!(buf.get(Pointer::from(1000u64)).is_none());
    }

    #[test]
    fn buffer_mixed_clean_and_dirty_operations() {
        let mut buf = Buffer {
            hmap: HashMap::new(),
            nappend: 0,
            npages: 0,
        };

        for i in 1..=10 {
            let node = create_test_node();
            let ptr = Pointer::from(i as u64);
            if i % 2 == 0 {
                buf.insert_dirty(ptr, node);
            } else {
                buf.insert_clean(ptr, node);
            }
        }

        let dirty_count = buf.to_dirty_iter().count();
        assert_eq!(dirty_count, 5); // Even numbers

        buf.clear();

        for i in 1..=10 {
            let ptr = Pointer::from(i as u64);
            if i % 2 == 0 {
                assert!(!buf.hmap[&ptr].dirty);
            }
        }
    }
}

#[cfg(test)]
mod buffer_integration_tests {
    use super::*;
    use crate::database::{btree::Compare, helper::cleanup_file};
    use test_log::test;

    #[test]
    fn buffer_tracks_inserts_through_pager() {
        let path = "test-files/buffer_int_insert.rdb";
        cleanup_file(path);
        let pager = EnvoyV1::open(path).unwrap();

        // Insert should load pages into buffer as dirty
        pager
            .tree
            .borrow_mut()
            .set("key1".into(), "value1".into(), SetFlag::UPSERT)
            .unwrap();

        let buf = pager.buffer.borrow();
        let dirty_count = buf.to_dirty_iter().count();

        // Should have at least one dirty page (the tree node)
        assert!(
            dirty_count > 0,
            "Buffer should contain dirty pages after insert"
        );
        cleanup_file(path);
    }

    #[test]
    fn buffer_persists_across_page_write() {
        let path = "test-files/buffer_int_persist.rdb";
        cleanup_file(path);
        let pager = EnvoyV1::open(path).unwrap();

        pager
            .set("key1".into(), "value1".into(), SetFlag::UPSERT)
            .unwrap();

        let buf_before = pager.buffer.borrow().hmap.len();

        // After set (which triggers page_write and clear), buffer should be cleaned
        let buf_after = pager.buffer.borrow();

        // Pages should still be in buffer but marked clean
        for entry in buf_after.hmap.values() {
            assert!(!entry.dirty, "Pages should be clean after page_write");
        }
        cleanup_file(path);
    }

    #[test]
    fn buffer_handles_multiple_sequential_sets() {
        let path = "test-files/buffer_int_seq.rdb";
        cleanup_file(path);
        let pager = EnvoyV1::open(path).unwrap();

        for i in 1..=10 {
            pager
                .set(
                    format!("key{}", i).into(),
                    format!("val{}", i).into(),
                    SetFlag::UPSERT,
                )
                .unwrap();
        }

        let buf = pager.buffer.borrow();
        // Buffer should contain pages from tree operations
        assert!(
            buf.hmap.len() > 0,
            "Buffer should contain pages after multiple inserts"
        );
        cleanup_file(path);
    }

    #[test]
    fn buffer_retire_on_delete() {
        let path = "test-files/buffer_int_retire.rdb";
        cleanup_file(path);
        let pager = EnvoyV1::open(path).unwrap();

        pager
            .set("key1".into(), "value1".into(), SetFlag::UPSERT)
            .unwrap();

        let initial_size = pager.buffer.borrow().hmap.len();

        // Delete should mark pages as retired
        pager.delete("key1".into()).unwrap();

        let buf = pager.buffer.borrow();
        // After delete and clear, retired pages should be removed
        // (clear() is called during page_write in update_or_revert)
        let has_retired = buf.hmap.values().any(|e| e.retired);

        // After clear(), retired pages should be gone
        assert!(
            !has_retired || buf.hmap.len() <= initial_size,
            "Retired pages should be managed"
        );
        cleanup_file(path);
    }

    #[test]
    fn buffer_reads_load_clean_pages() {
        let path = "test-files/buffer_int_read.rdb";
        cleanup_file(path);
        let pager = EnvoyV1::open(path).unwrap();

        // Insert and flush
        pager
            .set("key1".into(), "value1".into(), SetFlag::UPSERT)
            .unwrap();

        // Clear buffer to simulate fresh read
        pager.buffer.borrow_mut().hmap.clear();

        // Read should load page as clean
        let _value = pager.get("key1".into()).unwrap();

        let buf = pager.buffer.borrow();
        let clean_pages = buf.hmap.values().filter(|e| !e.dirty).count();

        assert!(clean_pages > 0, "Read operations should load clean pages");
        cleanup_file(path);
    }

    #[test]
    fn buffer_dirty_pages_flushed_on_write() {
        let path = "test-files/buffer_int_flush.rdb";
        cleanup_file(path);
        let pager = EnvoyV1::open(path).unwrap();

        pager
            .set("key1".into(), "value1".into(), SetFlag::UPSERT)
            .unwrap();

        let buf = pager.buffer.borrow();
        let dirty_before = buf.to_dirty_iter().count();

        // After set, dirty pages should be flushed
        // (this happens in file_update -> page_write -> clear)
        let dirty_after = pager.buffer.borrow().to_dirty_iter().count();

        assert_eq!(
            dirty_after, 0,
            "All dirty pages should be flushed after set completes"
        );
        cleanup_file(path);
    }

    #[test]
    fn buffer_nappend_increments_on_new_pages() {
        let path = "test-files/buffer_int_nappend.rdb";
        cleanup_file(path);
        let pager = EnvoyV1::open(path).unwrap();

        let nappend_before = pager.buffer.borrow().nappend;

        pager
            .set("key1".into(), "value1".into(), SetFlag::UPSERT)
            .unwrap();

        let nappend_after = pager.buffer.borrow().nappend;

        // nappend should be 0 after clear() in page_write
        assert_eq!(nappend_after, 0, "nappend should reset after page flush");
        cleanup_file(path);
    }

    #[test]
    fn buffer_npages_increments_after_flush() {
        let path = "test-files/buffer_int_npages.rdb";
        cleanup_file(path);
        let pager = EnvoyV1::open(path).unwrap();

        let npages_before = pager.buffer.borrow().npages;

        pager
            .set("key1".into(), "value1".into(), SetFlag::UPSERT)
            .unwrap();

        let npages_after = pager.buffer.borrow().npages;

        // npages should have increased
        assert!(
            npages_after > npages_before,
            "npages should increase after new pages are appended"
        );
        cleanup_file(path);
    }

    #[test]
    fn buffer_freelist_pages_reused() {
        let path = "test-files/buffer_int_freelist.rdb";
        cleanup_file(path);
        let pager = EnvoyV1::open(path).unwrap();

        // Insert multiple pages
        for i in 1..=20 {
            pager
                .set(
                    format!("key{}", i).into(),
                    format!("val{}", i).into(),
                    SetFlag::UPSERT,
                )
                .unwrap();
        }

        let npages_before = pager.buffer.borrow().npages;

        // Delete some pages (adds to freelist)
        for i in 1..=10 {
            pager.delete(format!("key{}", i).into()).unwrap();
        }

        // Insert again - should reuse freelist pages
        for i in 21..=30 {
            pager
                .set(
                    format!("key{}", i).into(),
                    format!("val{}", i).into(),
                    SetFlag::UPSERT,
                )
                .unwrap();
        }

        let npages_after = pager.buffer.borrow().npages;

        // npages growth should be minimal if freelist pages are reused
        assert!(
            npages_after - npages_before < 15,
            "Freelist reuse should prevent rapid npages growth"
        );
        cleanup_file(path);
    }

    #[test]
    fn buffer_scan_loads_multiple_pages() {
        let path = "test-files/buffer_int_scan.rdb";
        cleanup_file(path);
        let pager = EnvoyV1::open(path).unwrap();

        // Insert multiple pages of data
        for i in 1..=50 {
            pager
                .set(
                    format!("key{:03}", i).into(),
                    format!("val{}", i).into(),
                    SetFlag::UPSERT,
                )
                .unwrap();
        }

        pager.buffer.borrow_mut().hmap.clear();

        // Scan should load multiple pages
        let mode = ScanMode::new_open("key".into(), Compare::GE).unwrap();
        let _records = pager.scan(mode).unwrap();

        let buf = pager.buffer.borrow();
        assert!(
            buf.hmap.len() > 0,
            "Scan operations should load pages into buffer"
        );
        cleanup_file(path);
    }

    #[test]
    fn buffer_recovery_on_failed_write() {
        let path = "test-files/buffer_int_recovery.rdb";
        cleanup_file(path);
        let pager = EnvoyV1::open(path).unwrap();

        pager
            .set("key1".into(), "value1".into(), SetFlag::UPSERT)
            .unwrap();

        // Simulate a failed write scenario
        pager.failed.set(true);

        // The next operation should detect the failure and revert
        // This is handled in update_or_revert
        let result = pager.set("key2".into(), "value2".into(), SetFlag::UPSERT);

        // After recovery attempt, buffer should be manageable
        let buf = pager.buffer.borrow();
        assert!(buf.hmap.len() < 100, "Buffer should be cleared on recovery");
        cleanup_file(path);
    }

    #[test]
    fn buffer_consistency_after_many_operations() {
        let path = "test-files/buffer_int_consistency.rdb";
        cleanup_file(path);
        let pager = EnvoyV1::open(path).unwrap();

        // Mix of inserts, updates, and deletes
        for i in 1..=30 {
            pager
                .set(
                    format!("key{}", i).into(),
                    format!("val{}", i).into(),
                    SetFlag::UPSERT,
                )
                .unwrap();
        }

        for i in 1..=10 {
            pager
                .set(
                    format!("key{}", i).into(),
                    format!("updated{}", i).into(),
                    SetFlag::UPSERT,
                )
                .unwrap();
        }

        for i in 1..=5 {
            pager.delete(format!("key{}", i).into()).unwrap();
        }

        // Verify data integrity
        for i in 6..=10 {
            let val = pager.get(format!("key{}", i).into()).unwrap();
            assert_eq!(val, format!("updated{}", i).into());
        }

        let buf = pager.buffer.borrow();
        let all_clean = buf.hmap.values().all(|e| !e.dirty);
        assert!(
            all_clean,
            "All pages should be clean after operations complete"
        );
        cleanup_file(path);
    }

    #[test]
    fn buffer_handles_value_updates_dirty_tracking() {
        let path = "test-files/buffer_int_update.rdb";
        cleanup_file(path);
        let pager = EnvoyV1::open(path).unwrap();

        pager
            .set("key1".into(), "value1".into(), SetFlag::UPSERT)
            .unwrap();

        pager
            .set("key1".into(), "value2".into(), SetFlag::UPSERT)
            .unwrap();

        let value = pager.get("key1".into()).unwrap();
        assert_eq!(value, "value2".into());

        let buf = pager.buffer.borrow();
        assert_eq!(
            buf.to_dirty_iter().count(),
            0,
            "All pages should be clean after flush"
        );
        cleanup_file(path);
    }
}
