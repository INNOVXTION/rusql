pub const BTREE_MAX_KEY_SIZE: usize = 1000;
pub const BTREE_MAX_VAL_SIZE: usize = 3000;
pub const MERGE_FACTOR: usize = PAGE_SIZE / 4;
pub const PAGE_SIZE: usize = 4096; // 4096 bytes
pub const NODE_SIZE: usize = 4096; // PAGE_SIZE * 2; // in memory buffer
