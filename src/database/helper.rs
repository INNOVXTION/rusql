use crate::database::errors::{Error, PagerError};
use crate::database::types::*;
use rustix::fs::{self, Mode, OFlags};
use std::os::fd::OwnedFd;
use std::path::PathBuf;
use std::str::FromStr;
use tracing::{debug, error};

pub trait DecodeSlice {
    fn read_u16(&self, offset: usize) -> u16;
}

// pub trait EncodeSlice {
//     fn write_ptr(&mut self, ptr: Pointer, offset: usize);
// }

// impl<T> EncodeSlice for T
// where
//     T: DerefMut<Target = [u8]>,
// {
//     fn write_ptr(&mut self, ptr: Pointer, offset: usize) {
//         if offset > NODE_SIZE {
//             error!("offset idx {} exceeded node size", offset);
//             panic!()
//         }
//         self[offset..offset + PTR_SIZE].copy_from_slice(&ptr.as_slice());
//     }
// }

/// assumes little endian
///
/// reads a [u8] slice to u16 starting at offset
pub(crate) fn read_u16(data: &[u8], offset: usize) -> Result<u16, Error> {
    if offset > NODE_SIZE {
        error!("slice_to_u16: offset idx {} exceeded node size", offset);
        return Err(Error::IndexError);
    }
    data.get(offset..offset + U16_SIZE)
        .and_then(|x| x.try_into().ok())
        .map(|buf: [u8; U16_SIZE]| u16::from_le_bytes(buf))
        .ok_or(Error::IntCastError(None))
}

/// assumes little endian
///
/// reads a [u8] slice to Pointer starting at offset
pub(crate) fn read_pointer(data: &[u8], offset: usize) -> Result<Pointer, Error> {
    if offset > NODE_SIZE {
        error!("slice_to_u64: offset idx {} exceeded node size", offset);
        return Err(Error::IndexError);
    }
    data.get(offset..offset + PTR_SIZE)
        .and_then(|x| x.try_into().ok())
        .map(|buf: [u8; PTR_SIZE]| Pointer::from(u64::from_le_bytes(buf)))
        .ok_or(Error::IntCastError(None))
}

/// writes u16 to slice at offset
pub(crate) fn write_u16(data: &mut [u8], offset: usize, value: u16) -> Result<(), Error> {
    if offset > NODE_SIZE {
        error!("offset idx {} exceeded node size", offset);
        return Err(Error::IndexError);
    }
    data[offset..offset + U16_SIZE].copy_from_slice(&value.to_le_bytes());
    Ok(())
}
/// writes Pointer to slice
pub(crate) fn write_pointer(data: &mut [u8], offset: usize, ptr: Pointer) -> Result<(), Error> {
    if offset > NODE_SIZE {
        error!("offset idx {} exceeded node size", offset);
        return Err(Error::IndexError);
    }
    data[offset..offset + PTR_SIZE].copy_from_slice(&ptr.as_slice());
    Ok(())
}

/// casts usize to u16
pub(crate) fn as_usize(n: usize) -> u16 {
    if n > u16::MAX as usize {
        error!("casting error");
        panic!();
    }
    n as u16
}

/// converting byte size to megabyte
pub(crate) fn as_mb(bytes: usize) -> String {
    format!("{} MB", bytes / 2usize.pow(10))
}

/// converting page offset to page number
pub(crate) fn as_page(offset: usize) -> String {
    format!("page {}", offset / PAGE_SIZE)
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
    debug!("opening file");
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

/// input validator
pub(crate) fn input_valid(key: &str, value: &str) -> Result<(), Error> {
    if key.len() > BTREE_MAX_KEY_SIZE {
        error!(
            key.len = key.len(),
            max = { BTREE_MAX_KEY_SIZE },
            "key size exceeds maximum!"
        );
        return Err(Error::InvalidInput("key size exceeds maximum!"));
    };
    if value.len() > BTREE_MAX_VAL_SIZE {
        error!(
            val.len = value.len(),
            max = { BTREE_MAX_VAL_SIZE },
            "value size exceeds maximum!"
        );
        return Err(Error::InvalidInput("value size exceeds maximum!"));
    };
    if key.is_empty() {
        error!("key cant be empty");
        return Err(Error::InvalidInput("key cant be empty"));
    }
    let key_num = key.parse::<u64>().map_err(|e| {
        error!(%e, key, "key parse error");
        Error::InvalidInput("key parse error")
    })?;
    if key_num <= 0 {
        error!("key cant be zero or negative!");
        return Err(Error::InvalidInput("key cant be zero or negative!"));
    };
    Ok(())
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
