use tracing::debug;

use crate::database::{
    BTree,
    btree::TreeNode,
    errors::Result,
    pager::diskpager::Pager,
    tables::{Key, Query, Record, Value},
};

pub(crate) enum ScanMode {
    Single(Key),
    Open(Key, Compare),
    Closed {
        lo: (Key, Compare),
        hi: (Key, Compare),
    },
}

impl ScanMode {
    fn valid() {}
}

/*
path = [R, N2, L2] Nodes to be read
pos  = [1, 1 , 2]   indices correspond to an idx to a given key

in this case, the key would be located at:
idx 1 in root, index 1 in Node 2 and index 2 in Leaf 2
*/

pub(crate) struct Cursor<'a, P: Pager> {
    pub tree: &'a BTree<P>,
    pub path: Vec<TreeNode>, // from root to leaf
    pub pos: Vec<u16>,       // indices
}

impl<'a, P: Pager> Cursor<'a, P> {
    pub fn new(tree: &'a BTree<P>) -> Self {
        Cursor {
            tree: tree,
            path: vec![],
            pos: vec![],
        }
    }

    // retrieves the key value pair at current position
    pub fn deref(&self) -> (Key, Value) {
        let node = &self.path[self.path.len() - 1];
        let idx = self.pos[self.path.len() - 1];

        let key = node.get_key(idx).unwrap();
        let val = node.get_val(idx).unwrap();
        (key, val)
    }

    // pre condition for get
    pub fn valid(&self) -> bool {
        todo!()
    }

    // moves the path one idx forward
    pub fn next(&mut self) -> Option<()> {
        self.iter_next(self.path.len() - 1)
    }

    fn iter_next(&mut self, level: usize) -> Option<()> {
        if self.pos[level] + 1 < self.path[level].get_nkeys() {
            // move within node
            self.pos[level] += 1;
        } else if level > 0 {
            // we reached the last key of the node, so we go up one level to access the sibling
            self.iter_next(level - 1);
        } else {
            // past last key
            return None;
        }
        if level + 1 < self.pos.len() {
            // we are in a non leaf node and need to retrieve the next sibling
            let node = &self.path[level];
            let kid = self.tree.decode(node.get_ptr(self.pos[level]));

            self.path[level + 1] = kid;
            self.pos[level + 1] = 0;
        }
        Some(())
    }

    // moves the path one idx backwards
    pub fn prev(&mut self) -> Option<()> {
        self.iter_prev(self.path.len() - 1)
    }

    fn iter_prev(&mut self, level: usize) -> Option<()> {
        if self.pos[level] > 0 {
            // move within node
            self.pos[level] -= 1;
        } else if level > 0 {
            // we reached the last key of the node, so we go up one level to access the sibling
            self.iter_next(level - 1);
        } else {
            // past last key
            return None;
        }
        if level + 1 < self.pos.len() {
            // we are in a non leaf node and need to retrieve the next sibling
            let node = &self.path[level];
            let kid = self.tree.decode(node.get_ptr(self.pos[level]));

            self.pos[level + 1] = kid.get_nkeys() - 1;
            self.path[level + 1] = kid;
        }
        Some(())
    }
}

pub(super) fn node_lookup(node: &TreeNode, key: &Key, flag: &Compare) -> Option<u16> {
    if node.get_nkeys() == 0 {
        return None;
    }
    match flag {
        Compare::LT => cmp_le(node, key),
        Compare::LE => cmp_lt(node, key),
        Compare::GT => cmp_gt(node, key),
        Compare::GE => cmp_ge(node, key),
        Compare::EQ => cmp_eq(node, key),
    }
}

pub(crate) enum Compare {
    LT, // <
    LE, // <=
    GT, // >
    GE, // >=
    EQ, // ==
}

fn cmp_lt(node: &TreeNode, key: &Key) -> Option<u16> {
    let nkeys = node.get_nkeys();
    let mut lo: u16 = 0;
    let mut hi: u16 = nkeys;

    debug!("cmp_lt in {:?} nkeys {}", node.get_type(), nkeys);
    while hi > lo {
        let m = (hi + lo) / 2;
        let v = node.get_key(m).ok()?;

        // if v == *key {
        //     return None; // key already exists
        // };
        if v > *key {
            hi = m;
        } else {
            lo = m + 1;
        }
    }
    if lo == 0 { None } else { Some(lo - 1) }
}

fn cmp_le(node: &TreeNode, key: &Key) -> Option<u16> {
    let nkeys = node.get_nkeys();
    let mut lo: u16 = 0;
    let mut hi: u16 = nkeys;

    debug!("cmp_le in {:?} nkeys {}", node.get_type(), nkeys);
    while hi > lo {
        let m = (hi + lo) / 2; // mid point
        let v = node.get_key(m).ok()?; // key at m

        if v == *key {
            return Some(m as u16);
        };
        if v > *key {
            hi = m;
        } else {
            lo = m + 1;
        }
    }
    if lo == 0 { Some(0) } else { Some(lo - 1) }
}

fn cmp_gt(node: &TreeNode, key: &Key) -> Option<u16> {
    let nkeys = node.get_nkeys();
    let mut lo: u16 = 0;
    let mut hi: u16 = nkeys;

    debug!("cmp_gt in {:?} nkeys {}", node.get_type(), nkeys);
    while hi > lo {
        let m = (hi + lo) / 2; // mid point
        let v = node.get_key(m).ok()?; // key at m

        // if v == *key {
        //     return None; // key already exists
        // };
        if v > *key {
            hi = m;
        } else {
            lo = m + 1;
        }
    }
    if lo == nkeys { None } else { Some(lo) }
}

fn cmp_ge(node: &TreeNode, key: &Key) -> Option<u16> {
    let nkeys = node.get_nkeys();
    let mut lo: u16 = 0;
    let mut hi: u16 = nkeys;

    debug!("cmp_ge in {:?} nkeys {}", node.get_type(), nkeys);
    while hi > lo {
        let m = (hi + lo) / 2; // mid point
        let v = node.get_key(m).ok()?; // key at m

        if v == *key {
            return Some(m as u16);
        };
        if v > *key {
            hi = m;
        } else {
            lo = m + 1;
        }
    }
    if lo == nkeys { None } else { Some(lo) }
}

fn cmp_eq(node: &TreeNode, key: &Key) -> Option<u16> {
    let nkeys = node.get_nkeys();
    let mut lo: u16 = 0;
    let mut hi: u16 = nkeys;

    debug!("cmp_eq in {:?} nkeys {}", node.get_type(), nkeys);
    while hi > lo {
        let m = (hi + lo) / 2; // mid point
        let v = node.get_key(m).ok()?; // key at m

        if v == *key {
            return Some(m as u16);
        };
        if v > *key {
            hi = m;
        } else {
            lo = m + 1;
        }
    }
    None
}
