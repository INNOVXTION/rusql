pub mod diskpager;
pub mod freelist;
pub mod mempager;

// exporting to database module
pub(crate) use mempager::mempage_tree;
