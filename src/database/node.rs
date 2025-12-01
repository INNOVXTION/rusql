use std::ops::{Deref, DerefMut};

use crate::database::{
    helper::*,
    tree::BTree,
    types::{MERGE_FACTOR, NODE_SIZE, PAGE_SIZE, Pointer},
};
use tracing::{debug, error, instrument, warn};

use crate::database::errors::Error;

const HEADER_OFFSET: usize = 4;
const POINTER_OFFSET: usize = 8;
const OFFSETARR_OFFSET: usize = 2;

/*
-----------------------------------Node Layout----------------------------------
| type | nkeys | pointers | offsets |            key-values           | unused |
|   2  |   2   | nil nil  |  8 19   | 2 2 "k1" "hi"  2 5 "k3" "hello" |        |
|  2B  |  2B   |   2×8B   |  2×2B   | 4B + 2B + 2B + 4B + 2B + 5B     |        |

----------Key-Value Layout---------
| key_size | val_size | key | val |
|    2B    |    2B    | ... | ... |
*/

#[derive(PartialEq, Debug)]
pub(crate) enum NodeType {
    Node,
    Leaf,
}
/// which sibling we need to merge with
pub enum MergeDirection {
    Left(Node),
    Right(Node),
}
#[derive(Debug)]
pub(crate) struct Node(pub Box<[u8; NODE_SIZE]>);

impl Node {
    /// new empty node
    pub fn new() -> Self {
        Node(Box::new([0; NODE_SIZE]))
    }

    /// receive the total node size
    pub fn nbytes(&self) -> u16 {
        from_usize(self.kv_pos(self.get_nkeys()).unwrap())
    }

    pub fn fits_page(&self) -> bool {
        if self.nbytes() > PAGE_SIZE as u16 {
            return false;
        }
        true
    }

    pub fn get_type(&self) -> NodeType {
        match slice_to_u16(self, 0).expect("error when reading node type") {
            1 => NodeType::Node,
            2 => NodeType::Leaf,
            _ => {
                error!("corrupted node type");
                panic!("invaild node type, possibly corrupted")
            }
        }
    }
    /// receive number of keys, this function doesnt check if KV amount aligns with nkeys!
    pub fn get_nkeys(&self) -> u16 {
        slice_to_u16(self, 2).unwrap()
    }

    pub fn set_header(&mut self, nodetype: NodeType, nkeys: u16) {
        let nodetype: u16 = match nodetype {
            NodeType::Node => 1,
            NodeType::Leaf => 2,
        };
        write_u16(self, 0, nodetype).expect("panic when setting header");
        write_u16(self, 2, nkeys).expect("panic when setting header");
    }

    /// retrieves child pointer(page number) from pointer array: 8 bytes
    pub fn get_ptr(&self, idx: u16) -> Pointer {
        if idx >= self.get_nkeys() {
            error!("invalid index");
            panic!("invalid index")
        };
        let pos: usize = HEADER_OFFSET + 8 * idx as usize;
        slice_to_pointer(self, pos).expect("error when getting pointer")
    }
    /// sets pointer at index in pointer array, does not increase nkeys!
    pub fn set_ptr(&mut self, idx: u16, ptr: Pointer) {
        if idx >= self.get_nkeys() {
            error!("invalid index");
            panic!("invalid index")
        };
        let pos: usize = HEADER_OFFSET + 8 * idx as usize;
        write_pointer(self, pos, ptr).expect("error when setting pointer")
    }

    /// inserts pointer when splitting or adding new child nodes, encodes nodes
    ///
    /// updates nkeys and header
    pub fn insert_nkids(
        &mut self,
        tree: &mut BTree,
        old_node: Node,
        idx: u16,
        new_kids: (u16, Vec<Node>),
    ) -> Result<(), Error> {
        debug!("inserting new kids...");
        let old_nkeys = old_node.get_nkeys();
        self.set_header(NodeType::Node, old_nkeys + new_kids.0 - 1);
        // copy range before new idx
        self.append_from_range(&old_node, 0, 0, idx).map_err(|e| {
            error!("append error before idx");
            e
        })?;
        // insert new ptr at idx, consuming the split array
        for (i, node) in new_kids.1.into_iter().enumerate() {
            let key = node.get_key(0)?.to_string();
            let ptr = (tree.encode)(node);
            debug!(
                "appending new ptr: {ptr} with {key} at idx {}",
                idx + i as u16
            );
            self.kvptr_append(idx + (i as u16), ptr, &key, "")
                .map_err(|e| {
                    error!("error when appending split array");
                    e
                })?;
        }
        // copy from range after idx
        if old_nkeys > (idx + 1) {
            self.append_from_range(&old_node, idx + new_kids.0, idx + 1, old_nkeys - (idx + 1))
                .map_err(|e| {
                    error!("append error after idx");
                    e
                })?;
        }
        // debug
        debug!("nkids after insertion:");
        for i in 0..self.get_nkeys() {
            debug!(
                "idx: {}, key {} ptr {}",
                i,
                self.get_key(i).unwrap(),
                self.get_ptr(i)
            )
        }
        Ok(())
    }
    /// reads the value from the offset array for a given index, 0 has no offset
    ///
    /// the offset is the last byte of the nth KV relative to the first KV
    fn get_offset(&self, idx: u16) -> Result<u16, Error> {
        if idx == 0 {
            return Ok(0);
        }
        if idx > self.get_nkeys() {
            error!(
                "get_offset: index {} out of key range {}",
                idx,
                self.get_nkeys()
            );
            return Err(Error::IndexError);
        }
        let pos = HEADER_OFFSET + (8 * self.get_nkeys() as usize) + 2 * (idx as usize - 1);
        slice_to_u16(self, pos)
    }

    /// writes a new offset into the array 2 Bytes
    fn set_offset(&mut self, idx: u16, size: u16) {
        if idx == 0 {
            error!("set_offset: set offset idx cant be zero");
            panic!()
        }
        let pos = HEADER_OFFSET + (8 * self.get_nkeys() as usize) + 2 * (idx as usize - 1);
        write_u16(self, pos, size).expect("error when setting offset")
    }

    /// kv position relative to node
    fn kv_pos(&self, idx: u16) -> Result<usize, Error> {
        if idx > self.get_nkeys() {
            error!("kvpos: index {} out of key range {}", idx, self.get_nkeys());
            return Err(Error::IndexError);
        };
        Ok((HEADER_OFFSET as u16
            + (8 * self.get_nkeys())
            + 2 * self.get_nkeys()
            + self.get_offset(idx)?) as usize)
    }

    /// retrieves key as byte array
    pub fn get_key(&self, idx: u16) -> Result<&str, Error> {
        if idx >= self.get_nkeys() {
            error!(
                "get_key: index {} out of key range {}",
                idx,
                self.get_nkeys()
            );
            return Err(Error::IndexError);
        };
        let kvpos = self.kv_pos(idx)?;
        let key_len = slice_to_u16(self, kvpos)? as usize;

        Ok(
            str::from_utf8(&self.0[kvpos + 4..kvpos + 4 + key_len]).map_err(|e| {
                error!("reading key error when casting from slice idx {idx} with kvpos {kvpos}");
                e
            })?,
        )
    }

    /// retrieves value as byte array
    pub fn get_val(&self, idx: u16) -> Result<&str, Error> {
        if let NodeType::Node = self.get_type() {
            return Ok(" ");
        }
        if idx >= self.get_nkeys() {
            error!("index {} out of key range {}", idx, self.get_nkeys());
            return Err(Error::IndexError);
        };
        let kvpos = self.kv_pos(idx)?;
        let key_len = slice_to_u16(self, kvpos)? as usize;
        let val_len = slice_to_u16(self, kvpos + 2)? as usize;

        Ok(
            str::from_utf8(&self.0[kvpos + 4 + key_len..kvpos + 4 + key_len + val_len]).map_err(
                |e| {
                    error!(
                        "reading value error when casting from slice idx {idx} with kvpos {kvpos}"
                    );
                    e
                },
            )?,
        )
    }

    /// appends key value and pointer at index
    ///
    /// does not update nkeys!
    pub fn kvptr_append(
        &mut self,
        idx: u16,
        ptr: Pointer,
        key: &str,
        val: &str,
    ) -> Result<(), Error> {
        self.set_ptr(idx, ptr);
        let kvpos = self.kv_pos(idx)?;
        let klen: u16 = key.len().try_into()?;
        let vlen: u16 = val.len().try_into()?;

        //inserting klen and vlen = 4 bytes
        self.0[kvpos..kvpos + 2].copy_from_slice(&klen.to_le_bytes());
        self.0[kvpos + 2..kvpos + 4].copy_from_slice(&vlen.to_le_bytes());

        //inserting key and value
        self.0[kvpos + 4..kvpos + 4 + key.len()].copy_from_slice(key.as_bytes());
        self.0[kvpos + 4 + key.len()..kvpos + 4 + key.len() + val.len()]
            .copy_from_slice(val.as_bytes());

        // updating offset for next KV
        self.set_offset(idx + 1, self.get_offset(idx)? + klen + vlen + 4);
        Ok(())
    }

    /// helper function: appends range to self starting at dst_idx from source Node starting at src_idx for n elements
    ///
    /// does not update nkeys!
    #[instrument(skip(self, src))]
    pub fn append_from_range(
        &mut self,
        src: &Node,
        dst_idx: u16,
        src_idx: u16,
        n: u16,
    ) -> Result<(), Error> {
        if dst_idx >= self.get_nkeys() || src_idx >= src.get_nkeys() {
            error!(
                "indexing error when appending from range, dst idx: {}, src idx {}, dst nkeys: {}, n: {n}",
                dst_idx,
                src_idx,
                self.get_nkeys()
            );
            return Err(Error::IndexError);
        };
        for i in 0..n {
            self.kvptr_append(
                dst_idx + i,
                src.get_ptr(src_idx + i),
                src.get_key(src_idx + i)?,
                src.get_val(src_idx + i)?,
            )?;
        }
        Ok(())
    }
    /// linear searching for key, for indexing use "lookupidx"
    pub fn searchidx(&self, key: &str) -> Option<u16> {
        debug!("searching for key in leaf...");
        // (0..self.get_nkeys()).find(|i| key == self.get_key(*i).unwrap())
        let n: usize = match key.is_empty() {
            true => 0,
            false => key.parse().unwrap(),
        };
        let mut lo: usize = 0;
        let mut hi = self.get_nkeys() as usize;
        while hi > lo {
            let m = (hi + lo) / 2;
            // let v: usize = self.get_key(m as u16).unwrap().parse().unwrap();
            let v = match self.get_key(m as u16).unwrap().is_empty() {
                // handling edge case for empty key
                true => 0,
                false => self.get_key(m as u16).unwrap().parse().unwrap(),
            };
            if v == n {
                return Some(m as u16);
            };
            if v > n {
                hi = m;
            } else {
                lo = m + 1;
            }
        }
        None
    }

    /// find the last index that is less than or equal to the key
    /// if the key is not found, returns the nkeys - 1
    ///
    /// TODO: binary search
    pub fn lookupidx(&self, key: &str) -> u16 {
        let nkeys = self.get_nkeys();
        debug!("lookupidx in {:?} nkeys {}", self.get_type(), nkeys);
        if nkeys == 0 | 1 {
            return 0;
        }

        // let key: u32 = key.parse().unwrap();
        // let mut idx: u16 = 0;
        // while idx < nkeys {
        //     let cur_key = self.get_key(idx).unwrap();
        //     let cur_as_num = match cur_key.is_empty() {
        //         // handling edge case for empty key
        //         true => 0,
        //         false => cur_key.parse().expect("cur key parse error"),
        //     };
        //     // debug!("checking {key} against {cuGr_as_num} at idx {idx}");
        //     if cur_as_num == key {
        //         debug!(
        //             "key found, returning idx {} for key {}, cur_as_num {}",
        //             idx, key, cur_as_num,
        //         );
        //         return idx;
        //     }
        //     if cur_as_num > key {
        //         debug!(
        //             "larger than key found, returning idx {} for key {}, cur_as_num {}, ",
        //             idx - 1,
        //             key,
        //             cur_as_num,
        //         );
        //         return idx - 1;
        //     }
        //     idx += 1;
        // }
        // debug!(
        //     "neither larger nor matching key found, returning idx {} for key {}",
        //     idx - 1,
        //     key,
        // );
        // idx - 1;

        // converts idx to key as usize
        let key_num = |idx| {
            let key = self.get_key(idx as u16).unwrap();
            match key.is_empty() {
                // handling edge case for empty key
                true => 0,
                false => key.parse().unwrap(),
            }
        };
        let n: usize = match key.is_empty() {
            true => 0,
            false => key.parse().unwrap(),
        };
        let nkeys = self.get_nkeys();
        let mut lo: usize = 0;
        let mut hi = nkeys as usize;
        while hi > lo {
            let m = (hi + lo) / 2;
            let v = key_num(m);
            debug!(key, lo, hi, v = key_num(m), m);
            if v == n {
                return m as u16;
            };
            if v > n {
                hi = m;
            } else {
                lo = m + 1;
            }
        }
        // linear search for first key larger
        for idx in 0..nkeys {
            if key_num(idx as usize) > n {
                return idx - 1;
            }
        }
        // no matching or larger key found
        nkeys - 1
    }

    /// abstracted API over leaf_kvinsert and leaf_kvupdate
    pub fn insert(&mut self, node: Node, key: &str, val: &str, idx: u16) {
        if node.get_key(idx).unwrap() == key {
            debug!("upating in leaf at idx: {}", idx);
            self.leaf_kvupdate(node, idx, key, val).unwrap();
        } else {
            debug!("inserting in leaf at idx: {}", idx + 1);
            self.leaf_kvinsert(node, idx + 1, key, val).unwrap();
        }
    }

    /// helper function: inserts new KV into leaf node copies content from old node
    ///
    /// updates nkeys, sets node to leaf
    #[instrument(skip(self, src))]
    pub fn leaf_kvinsert(
        &mut self,
        src: Node,
        idx: u16,
        key: &str,
        val: &str,
    ) -> Result<(), Error> {
        let src_nkeys = src.get_nkeys();
        self.set_header(NodeType::Leaf, src_nkeys + 1);
        debug!("insert new header: {}", src_nkeys + 1);
        // copy kv before idx
        self.append_from_range(&src, 0, 0, idx).map_err(|e| {
            error!("insertion error when appending before idx");
            e
        })?;
        // insert new kv
        self.kvptr_append(idx, Pointer::from(0), key, val)?;
        // copy kv after idx
        if src_nkeys > (idx + 1) {
            self.append_from_range(&src, idx + 1, idx, src_nkeys - idx)
                .map_err(|e| {
                    error!("insertion error when appending after idx");
                    e
                })?;
        }
        Ok(())
    }

    /// helper function: updates existing KV in leaf node copies content from old node, this function assumes the key exists and needs to be updated!
    ///
    /// updates nkeys, sets node to leaf
    #[instrument(skip(self, src))]
    pub fn leaf_kvupdate(
        &mut self,
        src: Node,
        idx: u16,
        key: &str,
        val: &str,
    ) -> Result<(), Error> {
        let src_nkeys = src.get_nkeys();
        self.set_header(NodeType::Leaf, src_nkeys);
        // copy kv before idx
        self.append_from_range(&src, 0, 0, idx).map_err(|err| {
            error!("kv update error: when appending before idx");
            err
        })?;
        // insert new kv
        self.kvptr_append(idx, Pointer::from(0), key, val)?;
        // copy kv after idx
        if src_nkeys > idx + 1 {
            // in case the updated key is the last key
            self.append_from_range(&src, idx + 1, idx + 1, src_nkeys - (idx + 1))
                .map_err(|err| {
                    error!("kv update error: when appending after idx");
                    err
                })?;
        };
        Ok(())
    }
    /// updates node with source node with kv at idx omitted
    ///
    /// updates nkeys, sets node to leaf
    pub fn leaf_kvdelete(&mut self, src: &Node, idx: u16) -> Result<(), Error> {
        let src_nkeys = src.get_nkeys();
        if (src_nkeys - 1) == 0 {
            return Ok(());
        }
        self.set_header(NodeType::Leaf, src_nkeys - 1);
        self.append_from_range(src, 0, 0, idx).map_err(|err| {
            error!("deletion error when appending before idx");
            err
        })?;
        if src_nkeys > (idx + 1) {
            self.append_from_range(src, idx, idx + 1, src_nkeys - 1 - idx)
                .map_err(|err| {
                    error!("deletion error when appending after idx");
                    err
                })?;
        }
        Ok(())
    }

    /// helper function: consumes node and splits it in two
    #[instrument(skip(self))]
    pub fn split_node(self) -> Result<(Node, Node), Error> {
        let mut left = Node::new();
        let mut right = Node::new();
        // splitting node in the middle as first guess
        let nkeys = self.get_nkeys();
        if nkeys < 2 {
            return Err(Error::IndexError);
        }
        // trying to fit the left half, making sure the new node is not oversized
        let mut nkeys_left = (nkeys / 2) as usize;
        let left_bytes = |n| -> usize {
            HEADER_OFFSET
                + POINTER_OFFSET * n
                + OFFSETARR_OFFSET * n
                + self.get_offset(from_usize(n)).unwrap() as usize
        };
        // incremently decreasing amount of keys for new node until it fits
        while left_bytes(nkeys_left) > PAGE_SIZE && nkeys_left > 1 {
            nkeys_left -= 1;
        }
        assert!(nkeys_left >= 1);
        // fitting right node
        let right_bytes =
            |n: usize| -> usize { self.nbytes() as usize - left_bytes(n) + HEADER_OFFSET };
        while right_bytes(nkeys_left) > PAGE_SIZE {
            nkeys_left += 1;
        }
        assert!(nkeys_left > 0);
        assert!(nkeys_left < nkeys as usize);

        // config new nodes
        let nkeys_left = from_usize(nkeys_left);
        left.set_header(self.get_type(), nkeys_left);
        left.append_from_range(&self, 0, 0, nkeys_left)
            .map_err(|err| Error::SplitError(format!("append error during left split, {err}")))?;
        let nkeys_right = nkeys - nkeys_left;
        right.set_header(self.get_type(), nkeys_right);
        right
            .append_from_range(&self, 0, nkeys_left, nkeys_right)
            .map_err(|err| Error::SplitError(format!("append error during right split: {err}")))?;

        assert!(right.fits_page());
        assert!(left.fits_page());
        debug!("left node first key: {}", left.get_key(0).unwrap());
        debug!(
            "left node last key: {}",
            left.get_key(nkeys_left - 1).unwrap()
        );
        debug!("right node first key: {}", right.get_key(0).unwrap());
        debug!(
            "right node last key: {}",
            right.get_key(nkeys_right - 1).unwrap()
        );
        Ok((left, right))
    }

    /// consumes node and splits it potentially three ways, returns number of splits and array of split off nodes
    pub fn split(self) -> Result<(u16, Vec<Node>), Error> {
        // no split
        let mut arr = Vec::with_capacity(3);
        if self.fits_page() {
            debug!("no split needed: {}", self.nbytes());
            arr.push(self);
            return Ok((1, arr)); // no split necessary
        };
        // two way split
        debug!("splitting node...");
        let (left, right) = self.split_node().map_err(|err| {
            error!("Could not split node once: {}", err);
            err
        })?;
        if left.fits_page() {
            warn!(
                "two way split: left = {} bytes, right = {} bytes",
                left.nbytes(),
                right.nbytes()
            );
            arr.push(left);
            arr.push(right);
            return Ok((2, arr));
        };
        // three way split
        let (leftleft, middle) = left.split_node().map_err(|err| {
            error!("Could not split node twice: {}", err);
            err
        })?;
        warn!(
            "three way split: leftleft = {} bytes, middle = {} bytes, right = {}",
            leftleft.nbytes(),
            middle.nbytes(),
            right.nbytes()
        );
        assert!(leftleft.fits_page());
        assert!(middle.fits_page());
        assert!(right.fits_page());
        arr.push(leftleft);
        arr.push(middle);
        arr.push(right);
        Ok((3, arr))
    }

    /// consumes two nodes and returns merged node
    ///
    /// updates nkeys
    pub fn merge(&mut self, left: Node, right: Node, ntype: NodeType) -> Result<(), Error> {
        let left_nkeys = left.get_nkeys();
        let right_nkeys = right.get_nkeys();
        self.set_header(ntype, left_nkeys + right_nkeys);
        self.append_from_range(&left, 0, 0, left_nkeys)
            .map_err(|err| {
                error!("Error when merging first half left_nkeys {}", left_nkeys);
                Error::MergeError(format!("{}", err))
            })?;
        self.append_from_range(&right, left_nkeys, 0, right_nkeys)
            .map_err(|err| {
                error!(
                    "Error when merging second half left_nkeys {}, right_nkeys {}",
                    left_nkeys, right_nkeys
                );
                Error::MergeError(format!("{}", err))
            })?;
        Ok(())
    }

    /// checks if new node needs merging
    ///
    /// returns sibling node to merge with
    pub fn merge_check(&self, tree: &BTree, new: &Node, idx: u16) -> Option<MergeDirection> {
        if new.nbytes() > MERGE_FACTOR as u16 {
            return None; // no merge necessary
        }
        let new_size = new.nbytes() - crate::database::node::HEADER_OFFSET as u16;
        // check left
        if idx > 0 {
            let sibling = (tree.decode)(&self.get_ptr(idx - 1));
            let sibling_size = sibling.nbytes();
            if sibling_size + new_size < PAGE_SIZE as u16 {
                return Some(MergeDirection::Left(sibling));
            }
        }
        // check right
        if idx + 1 < self.get_nkeys() {
            let sibling = (tree.decode)(&self.get_ptr(idx + 1));
            let sibling_size = sibling.nbytes();
            if sibling_size + new_size < PAGE_SIZE as u16 {
                return Some(MergeDirection::Right(sibling));
            }
        }
        debug!("no merge possible");
        None
    }

    pub fn merge_setptr(
        &mut self,
        tree: &mut BTree,
        src: Node,
        merged_node: Node,
        idx: u16, // idx of node that got merged away
    ) -> Result<(), Error> {
        let src_nkeys = src.get_nkeys();
        self.set_header(NodeType::Node, src_nkeys - 1);

        let merge_ptr_key = merged_node.get_key(0).unwrap().to_string();
        let merge_node_ptr = (tree.encode)(merged_node);

        self.append_from_range(&src, 0, 0, idx).map_err(|err| {
            error!("merge error when appending before idx");
            err
        })?;
        self.kvptr_append(idx, merge_node_ptr, &merge_ptr_key, "")?;
        if src_nkeys > (idx + 2) {
            self.append_from_range(&src, idx + 1, idx + 2, src_nkeys - idx - 2)
                .map_err(|err| {
                    error!("merge error when appending after idx");
                    err
                })?;
        }
        Ok(())
    }
}

impl Clone for Node {
    fn clone(&self) -> Self {
        Node(self.0.clone())
    }
}

impl Deref for Node {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0[..]
    }
}

impl DerefMut for Node {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0[..]
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use test_log::test;

    #[test]
    fn setting_header() {
        let mut page = Node::new();
        page.set_header(NodeType::Node, 5);

        assert_eq!(page.get_type(), NodeType::Node);
        assert_eq!(page.get_nkeys(), 5);
    }

    #[test]
    fn setting_ptr() {
        let mut page = Node::new();
        page.set_header(NodeType::Node, 5);

        page.set_ptr(1, Pointer::from(10));
        page.set_ptr(2, Pointer::from(20));
        assert_eq!(page.get_ptr(1), Pointer::from(10));
        assert_eq!(page.get_ptr(2), Pointer::from(20));
    }

    #[test]
    fn kv_append() -> Result<(), Error> {
        let mut node = Node::new();
        node.set_header(NodeType::Leaf, 2);
        node.kvptr_append(0, Pointer::from(0), "k1", "hi")?;
        node.kvptr_append(1, Pointer::from(0), "k3", "hello")?;

        assert_eq!(node.get_key(0).unwrap(), "k1");
        assert_eq!(node.get_val(0).unwrap(), "hi");
        assert_eq!(node.get_key(1).unwrap(), "k3");
        assert_eq!(node.get_val(1).unwrap(), "hello");
        Ok(())
    }

    #[test]
    fn kv_append_range() -> Result<(), Error> {
        let mut n1 = Node::new();
        let mut n2 = Node::new();

        n2.set_header(NodeType::Leaf, 2);
        n1.set_header(NodeType::Leaf, 2);
        n1.kvptr_append(0, Pointer::from(0), "k1", "hi")?;
        n1.kvptr_append(1, Pointer::from(0), "k3", "hello")?;
        n2.append_from_range(&n1, 0, 0, n1.get_nkeys())?;

        assert_eq!(n2.get_key(0).unwrap(), "k1");
        assert_eq!(n2.get_val(0).unwrap(), "hi");
        assert_eq!(n2.get_key(1).unwrap(), "k3");
        assert_eq!(n2.get_val(1).unwrap(), "hello");
        Ok(())
    }

    #[test]
    fn kv_delete() -> Result<(), Error> {
        let mut n1 = Node::new();
        let mut n2 = Node::new();

        n1.set_header(NodeType::Leaf, 3);
        n1.kvptr_append(0, Pointer::from(0), "k1", "hi")?;
        n1.kvptr_append(1, Pointer::from(0), "k2", "bonjour")?;
        n1.kvptr_append(2, Pointer::from(0), "k3", "hello")?;

        n2.leaf_kvdelete(&n1, 1)?;

        assert_eq!(n2.get_key(0).unwrap(), "k1");
        assert_eq!(n2.get_val(0).unwrap(), "hi");
        assert_eq!(n2.get_key(1).unwrap(), "k3");
        assert_eq!(n2.get_val(1).unwrap(), "hello");
        Ok(())
    }

    #[test]
    #[should_panic]
    fn kv_delete_panic() -> () {
        let mut n1 = Node::new();
        n1.set_header(NodeType::Leaf, 3);
        n1.kvptr_append(0, Pointer::from(0), "k1", "hi")
            .map_err(|_| ())
            .expect("unexpected panic");
        n1.kvptr_append(1, Pointer::from(0), "k2", "bonjour")
            .map_err(|_| ())
            .expect("unexpected panic");
        n1.kvptr_append(2, Pointer::from(0), "k3", "hello")
            .map_err(|_| ())
            .expect("unexpected panic");

        let mut n2 = Node::new();

        n2.leaf_kvdelete(&n1, 3).expect("index error");
        ()
    }
}
