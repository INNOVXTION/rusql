use crate::helper::*;
use tracing::{debug, error, info, instrument};
use tracing_subscriber;

use crate::errors::Error;

pub const PAGE_SIZE: usize = 4096; // 4096 bytes
pub const NODE_SIZE: usize = 50; // PAGE_SIZE * 2; // in memory buffer
const HEADER_OFFSET: usize = 4;
const POINTER_OFFSET: usize = 8;
const OFFSETARR_OFFSET: usize = 2;

/*
-----------------------------------Node Layout----------------------------------
| type | nkeys | pointers | offsets |            key-values           | unused |
|   2  |   2   | nil nil  |  8 19   | 2 2 "k1" "hi"  2 5 "k3" "hello" |        |
|  2B  |  2B   |   2×8B   |  2×2B   | 4B + 2B + 2B + 4B + 2B + 5B     |        |

| key_size | val_size | key | val |
|    2B    |    2B    | ... | ... |
*/

#[derive(Debug)]
pub struct Node(pub Box<[u8]>);

#[derive(PartialEq, Debug)]
pub enum NodeType {
    Node,
    Leaf,
}

#[allow(dead_code)]
impl Node {
    /// new empty node
    pub fn new() -> Self {
        Node(Box::new([0u8; NODE_SIZE]))
    }

    /// receive the total node size
    fn nbytes(&self) -> u16 {
        from_usize(self.kv_pos(self.get_nkeys()).unwrap())
    }

    pub fn fits_page(&self) -> bool {
        if self.nbytes() > PAGE_SIZE as u16 {
            return false;
        }
        true
    }

    #[instrument(skip(self))]
    pub fn get_type(&self) -> Result<NodeType, Error> {
        match slice_to_u16(self, 0)? {
            1 => Ok(NodeType::Node),
            2 => Ok(NodeType::Leaf),
            _ => {
                error!("corrupted node type");
                Err(Error::IndexError)
            }
        }
    }

    pub fn get_nkeys(&self) -> u16 {
        slice_to_u16(self, 2).unwrap()
    }

    #[instrument(skip(self))]
    pub fn set_header(&mut self, nodetype: NodeType, nkeys: u16) {
        let nodetype: u16 = match nodetype {
            NodeType::Node => 1,
            NodeType::Leaf => 2,
        };
        self.0[..2].copy_from_slice(&nodetype.to_le_bytes());
        self.0[2..4].copy_from_slice(&nkeys.to_le_bytes());
    }

    /// retrieves child pointer(page number) from pointer array: 8 bytes
    #[instrument(skip(self))]
    pub fn get_ptr(&self, idx: u16) -> Result<u64, Error> {
        if idx > self.get_nkeys() {
            error!("invalid index");
            return Err(Error::IndexError);
        };
        let pos: usize = HEADER_OFFSET + 8 * idx as usize;
        slice_to_u64(self, pos)
    }
    /// sets points in array, does not increase nkeys!
    #[instrument(skip(self))]
    pub fn set_ptr(&mut self, idx: u16, ptr: u64) -> Result<(), Error> {
        if idx > self.get_nkeys() {
            error!("invalid index");
            return Err(Error::IndexError);
        };
        let pos: usize = HEADER_OFFSET + 8 * idx as usize;
        self.0[pos..pos + 8].copy_from_slice(&ptr.to_le_bytes());
        Ok(())
    }
    /// inserts ptr when splitting or adding new leaf nodes, updates nkeys
    #[instrument(skip(self))]
    pub fn insert_ptr(
        &mut self,
        old_node: Node,
        idx: u16,
        new_kids: (u16, Vec<Node>),
    ) -> Result<(), Error> {
        self.set_header(
            old_node.get_type().unwrap(),
            old_node.get_nkeys() + new_kids.0 - 1,
        );
        // copy range before new idx
        self.append_from_range(&old_node, 0, 0, idx)?;
        // insert new ptr at idx, consuming the split array
        for (i, node) in new_kids.1.into_iter().enumerate() {
            let key = {
                let key_ref = node.get_key(0)?;
                str::from_utf8(key_ref)?
            }
            .to_string();
            self.append_kvptr(
                idx + (i as u16),
                crate::pager::tree::node_encode(node),
                &key,
                "",
            )?;
        }
        // copy from range after idx
        self.append_from_range(
            &old_node,
            idx + new_kids.0,
            idx + 1,
            old_node.get_nkeys() - (idx + 1),
        )?;
        Ok(())
    }
    /// reads the value from the offset array for a given index, 0 has no offset
    ///
    /// the offset is the last byte of the nth KV relative to the first KV
    #[instrument(skip(self))]
    fn get_offset(&self, idx: u16) -> Result<u16, Error> {
        if idx == 0 {
            return Ok(0);
        }
        if idx > self.get_nkeys() {
            error!("index out of key range");
            return Err(Error::IndexError);
        }
        let pos = HEADER_OFFSET + (8 * self.get_nkeys() as usize) + 2 * (idx as usize - 1);
        slice_to_u16(self, pos)
    }

    /// writes a new offset into the array 2 Bytes
    #[instrument(skip(self))]
    fn set_offset(&mut self, idx: u16, size: u16) {
        if idx == 0 {
            panic!("set offset idx cant be zero")
        }
        let pos = HEADER_OFFSET + (8 * self.get_nkeys() as usize) + 2 * (idx as usize - 1);
        self.0[pos..pos + 2].copy_from_slice(&size.to_le_bytes())
    }

    /// kv position relative to node
    fn kv_pos(&self, idx: u16) -> Result<usize, Error> {
        if idx > self.get_nkeys() {
            error!("index out of key range");
            return Err(Error::IndexError);
        };
        Ok((HEADER_OFFSET as u16
            + (8 * self.get_nkeys())
            + 2 * self.get_nkeys()
            + self.get_offset(idx)?) as usize)
    }

    /// retrieves key as byte array
    pub fn get_key(&self, idx: u16) -> Result<&[u8], Error> {
        if idx > self.get_nkeys() {
            error!("get_key error");
            return Err(Error::IndexError);
        };
        let kvpos = self.kv_pos(idx)?;
        let key_len = slice_to_u16(self, kvpos)? as usize;

        Ok(&self.0[kvpos + 4..kvpos + 4 + key_len])
    }

    /// retrieves value as byte array
    pub fn get_val(&self, idx: u16) -> Result<&[u8], Error> {
        if let NodeType::Node = self.get_type()? {
            return Ok(b"");
        }
        if idx > self.get_nkeys() {
            error!("index out of key range");
            return Err(Error::IndexError);
        };
        let kvpos = self.kv_pos(idx)?;
        let key_len = slice_to_u16(self, kvpos)? as usize;
        let val_len = slice_to_u16(self, kvpos + 2)? as usize;

        Ok(&self.0[kvpos + 4 + key_len..kvpos + 4 + key_len + val_len])
    }

    /// appends key value and pointer at index
    ///
    /// does not update nkeys!
    pub fn append_kvptr(&mut self, idx: u16, ptr: u64, key: &str, val: &str) -> Result<(), Error> {
        self.set_ptr(idx, ptr)?;
        let kvpos = self.kv_pos(idx).expect("index error append kv");
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

    /// appends range to self starting at dst_idx from source Node starting at src_idx for n elements
    ///
    /// does not update nkeys!
    #[instrument]
    fn append_from_range(
        &mut self,
        src: &Node,
        dst_idx: u16,
        src_idx: u16,
        n: u16,
    ) -> Result<(), Error> {
        info!("appending from range...");

        for i in 0..n {
            let dst_idx = dst_idx + i;
            let src_idx = src_idx + i;
            self.append_kvptr(
                dst_idx,
                src.get_ptr(src_idx)?,
                str::from_utf8(src.get_key(src_idx)?)?,
                str::from_utf8(src.get_val(src_idx)?)?,
            )?;
        }
        Ok(())
    }

    /// find the last index that is less than or equal to the key
    /// if the key is not found, returns the index of the last KV
    /// TODO: binary search
    pub fn lookupidx(&self, key: &str) -> u16 {
        let nkeys = self.get_nkeys();
        if nkeys == 0 {
            return 0;
        }
        let mut idx: u16 = 0;
        while idx < nkeys {
            let cur = str::from_utf8(self.get_key(idx).unwrap()).unwrap();
            if cur == key {
                return idx;
            }
            if cur > key {
                return idx - 1;
            }
            idx += 1;
        }
        idx - 1
    }
    // pub fn lookupidx(&self, key: &str) -> u16 {
    //     let nkeys = self.get_nkeys();
    //     match nkeys {
    //         0 => 0,
    //         n => {
    //             let mut idx: u16 = 0;
    //             while idx < n {
    //                 let cur = str::from_utf8(self.get_key(idx).unwrap()).unwrap();
    //                 if cur == key {
    //                     return idx;
    //                 }
    //                 if cur > key {
    //                     return if idx == 0 { 0 } else { idx - 1 };
    //                 }
    //                 idx += 1;
    //             }
    //             n - 1
    //         }
    //     }
    // }

    /// inserts KV into self, copies content from src, upates keys if necessary
    pub fn insert_from(&mut self, src: Node, key: &str, val: &str) -> Result<(), Error> {
        let idx = src.lookupidx(key);
        if str::from_utf8(src.get_key(idx).unwrap()).unwrap() == key {
            // found! updating
            self.leaf_kvupdate(src, idx, key, val)?;
            return Ok(());
        }
        self.leaf_kvinsert(src, idx + 1, key, val)?; // not found. insert
        Ok(())
    }

    /// helper function: inserts new KV into leaf node copies content from old node
    ///
    /// does not update nkeys!
    pub fn leaf_kvinsert(
        &mut self,
        src: Node,
        idx: u16,
        key: &str,
        val: &str,
    ) -> Result<(), Error> {
        let src_nkeys = src.get_nkeys();
        self.set_header(NodeType::Leaf, src_nkeys + 1);
        // copy kv before idx
        self.append_from_range(&src, 0, 0, idx)?;
        // insert new kv
        self.append_kvptr(idx, 0, key, val)?;
        // copy kv after idx
        self.append_from_range(&src, idx + 1, idx, src_nkeys - idx)?;
        Ok(())
    }

    /// helper function: updates existing KV in leaf node copies content from old node
    ///
    /// does not update nkeys!
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
        self.append_from_range(&src, 0, 0, idx)?;
        // insert new kv
        self.append_kvptr(idx, 0, key, val)?;
        // copy kv after idx
        if src_nkeys > idx + 1 {
            // in case the updated key is the last key
            self.append_from_range(&src, idx + 1, idx + 1, src_nkeys - (idx + 1))?;
        };
        Ok(())
    }

    /// helper function: consumes node and splits it in two
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
        left.set_header(self.get_type()?, nkeys_left);
        left.append_from_range(&self, 0, 0, nkeys_left)
            .map_err(|_| Error::NodeSplitError("append error during split".to_string()))?;
        let nkeys_right = nkeys - nkeys_left;
        right.set_header(self.get_type()?, nkeys_right);
        right
            .append_from_range(&self, 0, nkeys_left, nkeys_right)
            .map_err(|_| Error::NodeSplitError("append error during split".to_string()))?;
        assert!(right.fits_page());
        assert!(left.fits_page());
        Ok((left, right))
    }

    /// consumes node and splits it potentially three ways, returns number of splits and array of split off nodes
    #[instrument(skip(self))]
    pub fn split(self) -> Result<(u16, Vec<Node>), Error> {
        // no split
        let mut arr = Vec::with_capacity(3);
        if self.fits_page() {
            debug!("no split necessary");
            arr.push(self);
            return Ok((1, arr)); // no split necessary
        };
        // two way split
        info!("splitting node...");
        let (left, right) = self
            .split_node()
            .map_err(|_| Error::NodeSplitError("Could not split node once".to_string()))?;
        if left.fits_page() {
            debug!("two way split");
            debug!(
                "split: left = {} bytes, right = {} bytes",
                left.nbytes(),
                right.nbytes()
            );
            arr.push(left);
            arr.push(right);
            return Ok((2, arr));
        };
        // three way split
        debug!("three way split");
        let (leftleft, middle) = left
            .split_node()
            .map_err(|_| Error::NodeSplitError("Could not split node twice".to_string()))?;

        debug!(
            "split: leftleft = {} bytes, middle = {} bytes, right = {}",
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

    ///
    #[instrument]
    fn merge() {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn type_nkeys_setheader() {
        let mut page = Node(Box::new([0u8; 50]));
        page.set_header(NodeType::Node, 5);

        assert_eq!(page.get_type().unwrap(), NodeType::Node);
        assert_eq!(page.get_nkeys(), 5);
    }

    #[test]
    fn ptr() {
        let mut page = Node(Box::new([0u8; 100]));
        page.set_header(NodeType::Node, 5);

        page.set_ptr(1, 10).unwrap();
        page.set_ptr(2, 20).unwrap();

        // for byte in page.0.iter() {
        //     print!("{:08b} ", *byte);
        // }
        // for byte in page.0.iter() {
        //     print!("{:02x} ", *byte);
        // }
        assert_eq!(page.get_ptr(1).unwrap(), 10);
        assert_eq!(page.get_ptr(2).unwrap(), 20);
    }

    #[test]
    fn append() -> Result<(), Error> {
        let mut node = Node::new();
        node.set_header(NodeType::Leaf, 2);
        node.append_kvptr(0, 0, "k1", "hi")?;
        node.append_kvptr(1, 0, "k3", "hello")?;

        assert_eq!(str::from_utf8(node.get_key(0)?).unwrap(), "k1");
        assert_eq!(str::from_utf8(node.get_val(0)?).unwrap(), "hi");
        assert_eq!(str::from_utf8(node.get_key(1)?).unwrap(), "k3");
        assert_eq!(str::from_utf8(node.get_val(1)?).unwrap(), "hello");
        Ok(())
    }

    #[test]
    fn append_range() -> Result<(), Error> {
        tracing_subscriber::fmt().init();
        let mut n1 = Node::new();
        let mut n2 = Node::new();
        n2.set_header(NodeType::Leaf, 2);

        n1.set_header(NodeType::Leaf, 2);
        n1.append_kvptr(0, 0, "k1", "hi")?;
        n1.append_kvptr(1, 0, "k3", "hello")?;
        n2.append_from_range(&n1, 0, 0, n1.get_nkeys())?;

        assert_eq!(str::from_utf8(n2.get_key(0)?).unwrap(), "k1");
        assert_eq!(str::from_utf8(n2.get_val(0)?).unwrap(), "hi");
        assert_eq!(str::from_utf8(n2.get_key(1)?).unwrap(), "k3");
        assert_eq!(str::from_utf8(n2.get_val(1)?).unwrap(), "hello");
        Ok(())
    }
}
