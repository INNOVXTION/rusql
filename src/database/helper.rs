use crate::database::errors::{Error, PagerError};
use crate::database::types::*;
use rustix::fs::{self, Mode, OFlags};
use std::os::fd::OwnedFd;
use std::path::PathBuf;
use std::str::FromStr;
use tracing::{debug, error};

/// assumes little endian
///
/// reads a [u8] slice to u16
pub(crate) fn slice_to_u16(data: &[u8], pos: usize) -> Result<u16, Error> {
    if pos > NODE_SIZE {
        error!("slice_to_u16: pos idx {} exceeded node size", pos);
        return Err(Error::IndexError);
    }
    data.get(pos..pos + U16_SIZE)
        .and_then(|x| x.try_into().ok())
        .map(|buf: [u8; U16_SIZE]| u16::from_le_bytes(buf))
        .ok_or(Error::IntCastError(None))
}

/// assumes little endian
///
/// reads a [u8] slice to u64 starting at offset (pos)
pub(crate) fn slice_to_pointer(data: &[u8], pos: usize) -> Result<Pointer, Error> {
    if pos > NODE_SIZE {
        error!("slice_to_u64: pos idx {} exceeded node size", pos);
        return Err(Error::IndexError);
    }
    data.get(pos..pos + PTR_SIZE)
        .and_then(|x| x.try_into().ok())
        .map(|buf: [u8; PTR_SIZE]| Pointer::from(u64::from_le_bytes(buf)))
        .ok_or(Error::IntCastError(None))
}

/// writes u16 to node
pub(crate) fn write_u16(data: &mut [u8], pos: usize, value: u16) -> Result<(), Error> {
    if pos > NODE_SIZE {
        error!("pos idx {} exceeded node size", pos);
        return Err(Error::IndexError);
    }
    data[pos..pos + U16_SIZE].copy_from_slice(&value.to_le_bytes());
    Ok(())
}
/// writes Pointer to node
pub(crate) fn write_pointer(data: &mut [u8], pos: usize, ptr: Pointer) -> Result<(), Error> {
    if pos > NODE_SIZE {
        error!("pos idx {} exceeded node size", pos);
        return Err(Error::IndexError);
    }
    data[pos..pos + PTR_SIZE].copy_from_slice(&ptr.to_slice());
    Ok(())
}

/// casts usize to u16
pub(crate) fn from_usize(n: usize) -> u16 {
    if n > u16::MAX as usize {
        error!("casting error");
        panic!();
    }
    n as u16
}

/// creates or opens a .rdb file
pub fn create_file_sync(file: &str) -> Result<OwnedFd, PagerError> {
    let path = PathBuf::from_str(file).unwrap();
    if let None = path.file_name() {
        error!("invalid file name");
        return Err(PagerError::FileNameError);
    }
    let parent = path.parent();
    // checking if directory exists
    if parent.is_some() && !parent.unwrap().is_dir() {
        debug!("creating parent directory {:?}", parent.unwrap());
        std::fs::create_dir_all(parent.unwrap()).expect("error when creating directory");
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

#[cfg(test)]
mod test {
    use super::*;
    use std::error::Error;
    use test_log::test;

    #[test]
    fn create_file() -> Result<(), Box<dyn Error>> {
        let path = PathBuf::from("./test-files/database.rdb");
        create_file_sync(path.to_str().unwrap())?;
        assert!(path.is_file());
        assert!(path.parent().unwrap().is_dir());
        Ok(())
    }
}
