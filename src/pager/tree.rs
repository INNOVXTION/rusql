use crate::pager::node::*;
use crate::helper::*;
use crate::errors::Error;

struct BTree {
    root_ptr: Option<u64>,
}
 