mod btree;
mod codec;
mod errors;
mod helper;
mod pager;
mod tables;
mod types;

// example of reexport
pub use helper::create_file_sync;

// can be imported by all modules inside database with super::BTree
// lib.rs cant see it
use btree::BTree;

// Error is pub and gets reexported
// lib.rs can see it
// pub(super) use errors::Error;
