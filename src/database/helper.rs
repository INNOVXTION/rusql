use super::errors::PagerError;
use super::types::*;
use rustix::fs::{self, Mode, OFlags};
use std::os::fd::{AsFd, OwnedFd};
use std::path::Path;
use tracing::{debug, error};

use crate::database::btree::TreeNode;

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
    assert!(!file.is_empty());
    let path = Path::new(file);

    if let None = path.file_name() {
        error!("invalid file name");
        return Err(PagerError::FileNameError);
    }

    // checking if directory exists
    let parent = path.parent();
    let parent_exists = parent != Some(Path::new(""));

    if parent_exists && !parent.unwrap().is_dir() {
        debug!("creating parent directory {:?}", parent.unwrap());
        std::fs::create_dir_all(parent.unwrap()).expect("error when creating directory");
    }

    let dirfd: Box<dyn std::os::fd::AsFd> = if parent_exists {
        debug!("opening directory fd");
        Box::new(fs::open(
            parent.unwrap(),
            OFlags::DIRECTORY | OFlags::RDONLY,
            Mode::RUSR | Mode::WUSR | Mode::RGRP | Mode::ROTH,
        )?)
    } else {
        debug!("using CWD");
        Box::new(rustix::fs::CWD.as_fd())
    };

    debug!("opening file");
    let fd = fs::openat(
        &dirfd,
        path.file_name().unwrap(),
        OFlags::RDWR | OFlags::CREATE,
        Mode::RUSR | Mode::WUSR | Mode::RGRP | Mode::ROTH,
    )?;
    // fsync directory
    if parent_exists {
        fs::fsync(&dirfd)?;
    }
    Ok(fd)
}

/// helper function for debugging purposes
pub fn debug_print_tree(node: &TreeNode, idx: u16) {
    #[cfg(test)]
    {
        if let Ok("debug") = std::env::var("RUSQL_LOG_TREE").as_deref() {
            debug!("found idx = {} in {:?}...", idx, node.get_type());
            debug!("---------");
            for i in 0..node.get_nkeys() {
                debug!(
                    idx = i,
                    key = node.get_key(i).unwrap().to_string(),
                    val = node.get_val(i).unwrap().to_string(),
                    "keys: "
                );
            }
            debug!("---------");
        }
    }
}

/// testing function to clean up old dbs
pub fn cleanup_file(path: &str) {
    if Path::new(path).exists() {
        std::fs::remove_file(path).unwrap()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::error::Error;
    use test_log::test;

    #[test]
    fn create_file() -> Result<(), Box<dyn Error>> {
        let path_str = "./test-files/database.rdb";
        let path = std::path::PathBuf::from(path_str);
        cleanup_file(path_str);
        create_file_sync(path_str)?;
        assert!(path.is_file());
        assert!(path.parent().unwrap().is_dir());
        cleanup_file(path_str);
        Ok(())
    }
}
