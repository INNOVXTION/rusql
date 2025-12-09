#[cfg(test)]
mod disk_tests;

pub mod diskpager;
pub mod freelist;
pub mod mempager;
mod mmap;
mod pager;

// exporting to database module
pub(crate) use diskpager::DiskPager;
pub(crate) use mempager::mempage_tree;
