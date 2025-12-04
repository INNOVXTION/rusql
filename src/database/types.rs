use std::{
    fmt::Display,
    ops::{Deref, DerefMut},
};

use crate::database::{node::TreeNode, pager::freelist::FLNode};
use tracing::error;

pub const BTREE_MAX_KEY_SIZE: usize = 1000;
pub const BTREE_MAX_VAL_SIZE: usize = 3000;
/// determines when nodes should be merged, higher number = less merges
pub const MERGE_FACTOR: usize = PAGE_SIZE / 4;

/// size of one page on disk
pub const PAGE_SIZE: usize = 4096; // 4096 bytes

/// maximum size for nodes inside memory
pub const NODE_SIZE: usize = PAGE_SIZE * 2;

pub const DB_SIG: &'static str = "BuildYourOwnDB06";
pub const METAPAGE_SIZE: usize = 32;
pub const SIG_SIZE: usize = 16;
pub const PTR_SIZE: usize = 8;
pub const U16_SIZE: usize = 2;

#[derive(Debug)]
pub enum Node {
    Tree(TreeNode),
    Freelist(FLNode),
}

impl Node {
    /// deconstructs node to tree node, will panic if used on a FL node!
    pub fn as_tree(self) -> TreeNode {
        let Node::Tree(n) = self else {
            error!("Tree node deconstructor used on FL node!");
            panic!("Tree node deconstructor used on FL node!")
        };
        n
    }
    /// deconstructs node to FL node, will panic if used on a tree node!
    pub fn as_fl(self) -> FLNode {
        let Node::Freelist(n) = self else {
            error!("FL node deconstructor used on tree node!");
            panic!("FL node deconstructor used on tree node!")
        };
        n
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

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Eq, Hash)]
pub(crate) struct Pointer(pub u64);

impl From<u64> for Pointer {
    fn from(value: u64) -> Self {
        Pointer(value)
    }
}

impl Pointer {
    pub fn as_slice(self) -> [u8; 8] {
        self.0.to_le_bytes()
    }
}

impl Display for Pointer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
