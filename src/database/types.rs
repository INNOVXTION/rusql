use std::fmt::Display;

use crate::database::node::Node;

pub const BTREE_MAX_KEY_SIZE: usize = 1000;
pub const BTREE_MAX_VAL_SIZE: usize = 3000;
pub const MERGE_FACTOR: usize = PAGE_SIZE / 4;
pub const PAGE_SIZE: usize = 4096; // 4096 bytes
pub const NODE_SIZE: usize = PAGE_SIZE * 2; // in memory buffer

pub trait Pager: Send + Sync {
    fn encode(&mut self, node: Node) -> Pointer;
    fn decode(&self, ptr: Pointer) -> Node;
    fn delete(&mut self, ptr: Pointer);
}

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Eq, Hash)]
pub(crate) struct Pointer(pub u64);

impl From<u64> for Pointer {
    fn from(value: u64) -> Self {
        Pointer(value)
    }
}

impl Pointer {
    pub fn to_slice(self) -> [u8; 8] {
        self.0.to_le_bytes()
    }
}

impl Display for Pointer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
