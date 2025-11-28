use std::fmt::Display;

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
