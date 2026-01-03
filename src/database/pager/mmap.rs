use std::{ffi::c_void, ops::Deref, os::fd::OwnedFd};

use rustix::mm::{MapFlags, ProtFlags};
use tracing::{debug, error};

use crate::database::{
    errors::PagerError, helper::as_mb, pager::diskpager::DiskPager, types::PAGE_SIZE,
};

#[derive(Debug)]
pub struct Mmap {
    pub total: usize,       // mmap size, can be larger than the file size
    pub chunks: Vec<Chunk>, // multiple mmaps, can be non-continuous
}

#[derive(Debug)]
pub struct Chunk {
    data: *const u8, // pointer to start of memory mapping
    pub len: usize,  // len of memory mapping
}

impl Deref for Chunk {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.to_slice()
    }
}

impl Chunk {
    pub fn to_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data, self.len) }
    }
}

impl Drop for Chunk {
    fn drop(&mut self) {
        // SAFETY: non null, and page aligned pointer from mmap()
        unsafe {
            if let Err(e) = rustix::mm::munmap(self.data as *mut c_void, self.len) {
                error!("error when dropping with mumap {}", e);
            }
        };
    }
}

unsafe impl Send for Chunk {}
unsafe impl Sync for Chunk {}

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
        data: ptr as *const u8,
        len: length,
    })
}

/// checks for sufficient space, exponentially extends the mmap
pub fn mmap_extend(db: &DiskPager, size: usize) -> Result<(), PagerError> {
    let mmap_ref = db.mmap.borrow();
    let buf_ref = db.buffer.borrow();

    // do we need to extend?
    if size <= mmap_ref.total {
        return Ok(()); // enough range
    };

    // extending the mmap
    debug!("extending mmap: for file size {size}, {}", as_mb(size));
    let mut alloc = 64 << 20; // allocating 64 MiB
    while mmap_ref.total + alloc < size {
        // doubling if needed
        alloc *= 2;
    }
    let chunk = mmap_new(&db.database, mmap_ref.total as u64, alloc).map_err(|e| {
        error!("error when extending mmap, size: {size}");
        e
    })?;
    drop(mmap_ref);

    // updating values
    let mut mmap_ref = db.mmap.borrow_mut();
    mmap_ref.total += alloc;
    mmap_ref.chunks.push(chunk);

    Ok(())
}
