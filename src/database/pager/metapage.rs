use std::{
    ops::{Deref, DerefMut},
    str::FromStr,
    sync::atomic::Ordering,
};

use tracing::debug;

use crate::database::{
    errors::PagerError,
    pager::{
        DiskPager,
        freelist::{FLConfig, GC},
    },
    types::{PTR_SIZE, Pointer},
};

//--------Meta Page Layout-------
// | sig | root_ptr | page_used |
// | 16B |    8B    |     8B    |
//
// new
// | sig | root_ptr | page_used | head_page | head_seq | tail_page | tail_seq | version |
// | 16B |    8B    |     8B    |     8B    |    8B    |     8B    |    8B    |    8B   |
// | nfl_pages |
// |    8B     |

pub const DB_SIG: &'static str = "BuildYourOwnDB06";
pub const METAPAGE_SIZE: usize = 16 + (8 * 8); // sig plus 6 eight byte values
pub const SIG_SIZE: usize = 16;

// offsets
enum MpField {
    Sig = 0,
    RootPtr = 16,
    Npages = 16 + 8,
    HeadPage = 16 + (8 * 2),
    HeadSeq = 16 + (8 * 3),
    TailPage = 16 + (8 * 4),
    TailSeq = 16 + (8 * 5),
    Version = 16 + (8 * 6),
    Nflpages = 16 + (8 * 7),
}

pub(crate) struct MetaPage(Box<[u8; METAPAGE_SIZE]>);

impl MetaPage {
    pub fn new() -> Self {
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
pub fn metapage_save(pager: &DiskPager) -> MetaPage {
    let fl = pager.freelist.read();
    let flc = fl.get_config();
    let npages = pager.npages.load(Ordering::Relaxed);
    let mut data = MetaPage::new();

    debug!(
        "sig: {}\nroot: {:?}\nnpage: {}\nhead page: {:?}\nhead seq: {}\ntail page: {:?}\ntail seq: {}\nmax_seq: {}\nversion: {}\n",
        DB_SIG,
        pager.tree.read(),
        npages,
        flc.head_page,
        flc.head_seq,
        flc.tail_page,
        flc.tail_seq,
        fl.max_seq,
        pager.version.load(Ordering::Relaxed),
    );

    use MpField as M;
    data.set_sig(DB_SIG);
    data.set_ptr(M::RootPtr, *pager.tree.read());
    data.set_ptr(M::Npages, Some(npages.into()));

    data.set_ptr(M::HeadPage, flc.head_page);
    data.set_ptr(M::HeadSeq, Some(flc.head_seq.into()));
    data.set_ptr(M::TailPage, flc.tail_page);
    data.set_ptr(M::TailSeq, Some(flc.tail_seq.into()));
    data.set_ptr(M::Nflpages, Some(flc.npages.into()));

    data.set_ptr(
        M::Version,
        Some(Pointer::from(pager.version.load(Ordering::Relaxed))),
    );
    data
}

/// loads meta page object into pager
///
/// panics when called without initialized mmap
pub fn metapage_load(pager: &DiskPager, meta: &MetaPage) {
    debug!("loading metapage");
    let mut t = pager.tree.write();

    match meta.read_ptr(MpField::RootPtr) {
        Pointer(0) => *t = None,
        n => *t = Some(n),
    };

    pager
        .version
        .store(meta.read_ptr(MpField::Version).get(), Ordering::Relaxed);

    pager
        .npages
        .store(meta.read_ptr(MpField::Npages).get(), Ordering::Relaxed);

    let flc = FLConfig {
        head_page: Some(meta.read_ptr(MpField::HeadPage)),
        head_seq: meta.read_ptr(MpField::HeadSeq).get() as usize,

        tail_page: Some(meta.read_ptr(MpField::TailPage)),
        tail_seq: meta.read_ptr(MpField::TailSeq).get() as usize,

        max_ver: meta.read_ptr(MpField::Version).get(),
        cur_ver: meta.read_ptr(MpField::Version).get(),
        npages: meta.read_ptr(MpField::Nflpages).get(),
    };

    pager.freelist.write().set_config(&flc);
}

/// loads meta page from disk,
/// formerly root_read
pub fn metapage_read(pager: &DiskPager, file_size: u64) {
    if file_size == 0 {
        // empty file
        debug!("root read: empty file...");
        pager.npages.store(2, Ordering::Relaxed); // reserved for meta page and one free list node
        let flc = FLConfig {
            head_page: Some(Pointer::from(1u64)),
            head_seq: 0,
            tail_page: Some(Pointer::from(1u64)),
            tail_seq: 0,

            max_ver: 1,
            cur_ver: 1,
            npages: 0,
        };
        pager.freelist.write().set_config(&flc);
        return;
    }
    debug!("root read: loading meta page");

    let mut meta = MetaPage::new();
    meta.copy_from_slice(&pager.mmap.read().chunks[0].to_slice()[..METAPAGE_SIZE]);
    metapage_load(pager, &meta);

    assert!(pager.npages.load(Ordering::Relaxed) != 0);
}

/// writes meta page to disk
pub fn metapage_write(pager: &DiskPager, meta: &MetaPage) -> Result<(), PagerError> {
    debug!("writing metapage to disk...");
    let r = rustix::io::pwrite(&pager.database, meta, 0)?;
    Ok(())
}
