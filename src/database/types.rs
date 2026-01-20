/*
 * higher level types, wrappers and constants used throughout the database
 */

use std::{
    fmt::{Debug, Display},
    ops::{Deref, DerefMut},
};

use crate::database::{
    btree::TreeNode,
    pager::{diskpager::NodeFlag, freelist::FLNode},
};
use tracing::error;

pub const BTREE_MAX_KEY_SIZE: usize = 1000;
pub const BTREE_MAX_VAL_SIZE: usize = 3000;
/// determines when nodes should be merged, higher number = less merges
pub const MERGE_FACTOR: usize = PAGE_SIZE / 4;

/// size of one page on disk
pub const PAGE_SIZE: usize = 4096; // 4096 bytes
pub const TRUNC_THRESHOLD: usize = 1000; // amount of free list entries which trigger a truncation call

/// maximum size for nodes inside memory
pub const NODE_SIZE: usize = PAGE_SIZE * 2;
pub const PTR_SIZE: usize = 8;
pub const VER_SIZE: usize = std::mem::size_of::<u64>();
pub const FREE_PAGE: u64 = 0; // sentinel value for pages that are free to be reused, unreachable from any committed root

pub const LRU_BUFFER_SIZE: usize = 10;
pub const HISTORY_BUFFER_SIZE: usize = 10;

pub const U16_SIZE: usize = 2;

/// implements deref to get to the underlying array
pub enum Node {
    Tree(TreeNode),
    Freelist(FLNode),
}

impl Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tree(arg0) => f.debug_tuple("tree node").field(arg0).finish(),
            Self::Freelist(arg0) => f.debug_tuple("freelist node").field(arg0).finish(),
        }
    }
}

impl Node {
    /// deconstructs node to tree node, will panic if used on a FL node!
    pub fn as_tn(&self) -> &TreeNode {
        let Node::Tree(n) = self else {
            error!("Tree node deconstructor used on FL node!");
            panic!("Tree node deconstructor used on FL node!")
        };
        n
    }

    /// deconstructs node to FL node, will panic if used on a tree node!
    pub fn as_fl(&self) -> &FLNode {
        let Node::Freelist(n) = self else {
            error!("FL node deconstructor used on tree node!");
            panic!("FL node deconstructor used on tree node!")
        };
        n
    }

    /// deconstructs node to FL node, will panic if used on a tree node!
    pub fn as_fl_mut(&mut self) -> &mut FLNode {
        let Node::Freelist(n) = self else {
            error!("FL node deconstructor used on tree node!");
            panic!("FL node deconstructor used on tree node!")
        };
        n
    }
    pub fn fits_page(&self) -> bool {
        if let Node::Tree(n) = self {
            n.fits_page()
        } else {
            // FL nodes always fits
            true
        }
    }
    pub fn get_type(&self) -> NodeFlag {
        match self {
            Node::Tree(_) => NodeFlag::Tree,
            Node::Freelist(_) => NodeFlag::Freelist,
        }
    }
}

impl Deref for Node {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            Node::Tree(tree_node) => tree_node,
            Node::Freelist(flnode) => flnode,
        }
    }
}

impl DerefMut for Node {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Node::Tree(tree_node) => tree_node,
            Node::Freelist(flnode) => flnode,
        }
    }
}

impl Clone for Node {
    fn clone(&self) -> Self {
        match self {
            Self::Tree(arg0) => Self::Tree(arg0.clone()),
            Self::Freelist(arg0) => Self::Freelist(arg0.clone()),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Eq, Hash, Ord)]
pub(crate) struct Pointer(pub u64);

impl Pointer {
    pub fn as_slice(self) -> [u8; 8] {
        self.0.to_le_bytes()
    }
    pub fn get(self) -> u64 {
        self.0
    }
    pub fn set(&mut self, val: u64) {
        self.0 = val
    }
    pub fn from(val: u64) -> Self {
        Pointer(val)
    }
}

impl From<u64> for Pointer {
    fn from(value: u64) -> Self {
        Pointer(value)
    }
}
impl From<usize> for Pointer {
    fn from(value: usize) -> Self {
        Pointer(value as u64)
    }
}

impl Display for Pointer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "page: {}", self.0)
    }
}

/// simple wrapper for a cell
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub(crate) enum DataCell {
    Str(String),
    Int(i64),
}

pub(crate) trait InputData {
    fn into_cell(self) -> DataCell;
}

impl InputData for DataCell {
    fn into_cell(self) -> DataCell {
        self
    }
}
impl InputData for String {
    fn into_cell(self) -> DataCell {
        DataCell::Str(self)
    }
}

impl InputData for &str {
    fn into_cell(self) -> DataCell {
        DataCell::Str(self.to_string())
    }
}

impl InputData for i64 {
    fn into_cell(self) -> DataCell {
        DataCell::Int(self)
    }
}

impl InputData for i32 {
    fn into_cell(self) -> DataCell {
        DataCell::Int(self as i64)
    }
}

impl InputData for i16 {
    fn into_cell(self) -> DataCell {
        DataCell::Int(self as i64)
    }
}

impl InputData for i8 {
    fn into_cell(self) -> DataCell {
        DataCell::Int(self as i64)
    }
}

impl InputData for u32 {
    fn into_cell(self) -> DataCell {
        DataCell::Int(self as i64)
    }
}

impl InputData for u16 {
    fn into_cell(self) -> DataCell {
        DataCell::Int(self as i64)
    }
}

impl InputData for u8 {
    fn into_cell(self) -> DataCell {
        DataCell::Int(self as i64)
    }
}
