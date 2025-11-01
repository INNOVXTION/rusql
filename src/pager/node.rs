use tracing::{info, instrument, error};
use crate::{helper::*};

use crate::errors::Error;

pub const PAGE_SIZE: usize = 4096; // 4096 bytes
pub const NODE_SIZE: usize = PAGE_SIZE * 2; // in memory buffer
const HEADER_OFFSET: usize = 4;
const POINTER_OFFSET: usize = 8;
const OFFSETARR_OFFSET: usize = 2;
const BTREE_MAX_KEY_SIZE: usize = 1000;
const BTREE_MAX_VAL_SIZE: usize = 3000;

/* -------------------------------Node Layout----------------------------------

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
    Leaf
}

#[allow(dead_code)]
impl Node {
    /// new empty node
    pub fn new() -> Self {
        Node(Box::new([0u8; NODE_SIZE]))
    }

    /// receive the total node size
    #[instrument]
    fn nbytes(&self) -> u16 {
        from_usize(self.kv_pos(self.get_nkeys()).unwrap())
    }

    #[instrument]
    pub fn get_type(&self) -> Result<NodeType, Error> {
        let mut val = [0u8;2];
        val.copy_from_slice(&self.0[..2]);
        match u16::from_le_bytes(val) {
            1 => Ok(NodeType::Node),
            2 => Ok(NodeType::Leaf),
            _ => Err(Error::IndexError)
        }
    }

    #[instrument]
    pub fn get_nkeys(&self) -> u16 {
        info!("getting keys..");
        slice_to_u16(self, 2).unwrap()
        // let mut val = [0u8;2];
        // val.copy_from_slice(&self.0[2..4]);
        // u16::from_le_bytes(val)
    }

    #[instrument]
    pub fn set_HEADER_OFFSET(&mut self, nodetype: NodeType, nkeys: u16) {
        let nodetype: u16 = match nodetype {
            NodeType::Node => 1,
            NodeType::Leaf => 2,
        };
        self.0[..2].copy_from_slice(&nodetype.to_le_bytes());
        self.0[2..4].copy_from_slice(&nkeys.to_le_bytes());
    }

    /// retrieves child pointer(page number) from pointer array: 8 bytes
    #[instrument]
    pub fn get_ptr(&self, idx: u16) -> Result<u64, Error> {
        if idx > self.get_nkeys() {
            return Err(Error::IndexError)
        };
        let pos: usize = HEADER_OFFSET + 8 * idx as usize;
        slice_to_u64(self, pos)
    }

    #[instrument]
    pub fn set_ptr(&mut self, idx: u16, ptr: u64) -> Result<(), Error> {
        if idx > self.get_nkeys() {
            return Err(Error::IndexError)
        };
        let pos: usize = HEADER_OFFSET + 8 * idx as usize;
        self.0[pos..pos + 8].copy_from_slice(&ptr.to_le_bytes());
        Ok(())
    }

    /// reads the value from the offset array for a given index, 0 has no offset
    /// 
    /// the offset is the last byte of the nth KV relative to the first KV
    #[instrument]
    fn get_offset(&self, idx: u16) -> Result<u16, Error> {
        if idx == 0 {
            return Ok(0)
        }
        if idx > self.get_nkeys() {
            error!("index out of key range");
            return Err(Error::IndexError)
        }
        let pos = HEADER_OFFSET + (8 * self.get_nkeys() as usize) + 2 * (idx as usize - 1);
        slice_to_u16(self, pos)
    }

    /// writes a new offset into the array 2 Bytes
    #[instrument]
    fn set_offset(&mut self, idx: u16, size: u16) {
        if idx == 0 {
            panic!("set offset idx cant be zero")
        }
        let pos = HEADER_OFFSET + (8 * self.get_nkeys() as usize) + 2 * (idx as usize - 1);
        self.0[pos..pos + 2].copy_from_slice(&size.to_le_bytes())
    }

    /// kv position relative to node
    #[instrument]
    fn kv_pos(&self, idx: u16) -> Result<usize, Error> {
        if idx > self.get_nkeys() {
            error!("index out of key range");
            return Err(Error::IndexError)
        };
        Ok((HEADER_OFFSET as u16 + (8 * self.get_nkeys()) + 2 * self.get_nkeys() + self.get_offset(idx)?) as usize)
    }

    /// retrieves key as byte array
    #[instrument]
    pub fn get_key(&self, idx: u16) -> Result<&[u8], Error> {
        if idx > self.get_nkeys() {
            error!("get_key error");
            return Err(Error::IndexError)
        };
        let kvpos = self.kv_pos(idx)?;
        let key_len = slice_to_u16(self, kvpos)? as usize;
        
        Ok(&self.0[kvpos + 4..kvpos + 4 + key_len])
    }

    /// retrieves value as byte array
    #[instrument]
    pub fn get_val(&self, idx: u16) -> Result<&[u8], Error> {
        if let NodeType::Node = self.get_type()? {
            error!("writing values to non-leaf node");
            return Err(Error::NodeTypeError)
        }
        if idx > self.get_nkeys() {
            error!("index out of key range");
            return Err(Error::IndexError)
        };    
        let kvpos = self.kv_pos(idx)?;
        let key_len = slice_to_u16(self, kvpos)? as usize;
        let val_len = slice_to_u16(self, kvpos + 2)? as usize;

        Ok(&self.0[kvpos + 4 + key_len..kvpos + 4 + key_len + val_len])
    }

    /// appends key value and pointer at index
    #[instrument]
    pub fn append_kvptr(&mut self, idx: u16, ptr: u64, key: &str, val: &str) -> Result<(), Error> {
        self.set_ptr(idx, ptr)?;  
        let kvpos = self.kv_pos(idx).expect("index error append kv");
        let klen: u16 = key.len().try_into()?;
        let vlen: u16 = val.len().try_into()?;
        
        //inserting klen and vlen = 4 bytes
        self.0[kvpos..kvpos + 2]
            .copy_from_slice(&klen.to_le_bytes());
        self.0[kvpos + 2..kvpos + 4]
            .copy_from_slice(&vlen.to_le_bytes());

        //inserting key and value
        self.0[kvpos + 4..kvpos + 4 + key.len()]
            .copy_from_slice(key.as_bytes());
        self.0[kvpos + 4 + key.len()..kvpos + 4 + key.len() + val.len()]
            .copy_from_slice(val.as_bytes());

        // updating offset for next KV
        self.set_offset(idx + 1, self.get_offset(idx)? + klen + vlen + 4);
        Ok(())
    }

    /// appends range to self starting at dst_idx from source Node starting at src_idx for n elements
    #[instrument]
    fn append_from_range(&mut self, src: &Node, dst_idx: u16, src_idx: u16, n: u16)
        -> Result<(), Error> {
        for i in 0..n {
            let dst_idx = dst_idx + i;
            let src_idx = src_idx + i;
            self.append_kvptr(
                dst_idx,
                src.get_ptr(src_idx)?,
                str::from_utf8(src.get_key(src_idx)?)?,
                str::from_utf8(src.get_val(src_idx)?)?)?;
        }
        Ok(())
    }

    /// find the last index that is less than or equal to the key
    /// 
    /// TODO: binary search
    #[instrument]
    fn lookupidx(&self, key: &str) -> u16 {
        let nkeys = self.get_nkeys();
        let mut idx: u16 = 0;
        for i in 0..nkeys {
            if str::from_utf8(self.get_key(idx).unwrap()).unwrap() == key {
                return idx
            }
            if str::from_utf8(self.get_key(idx).unwrap()).unwrap() > key {
                return idx - 1
            }
            idx += 1;
        }
        idx - 1
    }

    /// inserts KV into new node, copies content from old node, capable of updating existing key
    #[instrument]
    pub fn insert(
        &mut self,
        src: Node,
        key: &str,
        val: &str) -> Result<(), Error> {
            let idx = src.lookupidx(key);
            if str::from_utf8(src.get_key(idx).unwrap()).unwrap() == key { // found! updating
                self.leaf_kvupdate(src, idx, key, val)?;
                return Ok(())
            }
            self.leaf_kvinsert(src, idx, key, val)?; // not found. insert
            Ok(())
    }

    /// helper function: inserts new KV into leaf node copies content from old node
    #[instrument]
    fn leaf_kvinsert(
        &mut self,
        src: Node,
        idx: u16,
        key: &str,
        val: &str) -> Result<(), Error> {
            self.set_HEADER_OFFSET(NodeType::Leaf, src.get_nkeys() + 1);
            // copy kv before idx
            self.append_from_range(&src, 0, 0, idx)?;
            // insert new kv
            self.append_kvptr(idx, 0, key, val)?;
            // copy kv after idx
            self.append_from_range(&src, idx + 1, idx, src.get_nkeys() - idx)?;
            Ok(())
    } 

    /// helper function: updates existing KV in leaf node copies content from old node
    #[instrument]
    fn leaf_kvupdate(
        &mut self, 
        src: Node,
        idx: u16,
        key: &str,
        val: &str) -> Result<(), Error> {
            self.set_HEADER_OFFSET(NodeType::Leaf, src.get_nkeys() + 1);
            // copy kv before idx
            self.append_from_range(&src, 0, 0, idx)?;
            // insert new kv
            self.append_kvptr(idx, 0, key, val)?;
            // copy kv after idx
            self.append_from_range(&src, idx + 1, idx + 1, src.get_nkeys() - (idx - 1))?;
            Ok(())      
    }

    /// consumes node and splits it
    #[instrument]
    fn split(self, left: Node, right: Node,) -> Result<(), Error> {
        // splitting node in the middle as first guess
        if self.get_nkeys() < 2 {
            return Err(Error::IndexError)
        }
        // trying to fit the left half, making sure the new node is not oversized
        let mut nkeys_left = (self.get_nkeys() / 2) as usize;
        let left_bytes = |n| -> usize {
            HEADER_OFFSET + POINTER_OFFSET * n + OFFSETARR_OFFSET * n +
            self.get_offset(from_usize(n)).unwrap() as usize
        };
        // incremently decreasing amount of keys for new node until it fits
        while left_bytes(nkeys_left) > PAGE_SIZE {
            nkeys_left -=1;
        };
        assert!(nkeys_left >= 1);
        // fitting right node
        let right_bytes = |n: usize| -> usize {
            self.nbytes() as usize - left_bytes(n) + 4
        };
        while right_bytes(nkeys_left) > PAGE_SIZE {
            nkeys_left += 1;
        };
        Ok(())
    }
}







#[cfg(test)]
mod test {
    use crate::errors;

    use super::*;

    #[test]
    fn type_nkeys_setHEADER_OFFSET() {
        let mut page = Node(Box::new([0u8; 50]));
        page.set_HEADER_OFFSET(NodeType::Node, 5);

        assert_eq!(page.get_type().unwrap(), NodeType::Node);
        assert_eq!(page.get_nkeys(), 5);

    }

    #[test]
    fn ptr() {
        let mut page = Node(Box::new([0u8; 100]));
        page.set_HEADER_OFFSET(NodeType::Node, 5);

        page.set_ptr(1, 10);
        page.set_ptr(2, 20);

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
    fn append() -> Result<(), errors::Error> {
        let mut node = Node::new();
        node.set_HEADER_OFFSET(NodeType::Leaf, 2);
        node.append_kvptr(0, 0, "k1", "hi")?;
        node.append_kvptr(1, 0, "k3", "hello")?;

        assert_eq!(str::from_utf8(node.get_key(0)?).unwrap(), "k1");
        assert_eq!(str::from_utf8(node.get_val(0)?).unwrap(), "hi");
        assert_eq!(str::from_utf8(node.get_key(1)?).unwrap(), "k3");
        assert_eq!(str::from_utf8(node.get_val(1)?).unwrap(), "hello");
        Ok(())
    }
    
    #[test]
    fn string_cmp() {
        let s1 = "k1";
        let s2 = "k2";
        assert!(s2 > s1);
    }
    fn borrow() {
        let mut val = 5;
        let r = &val;
        
        println!("{}", r);
        
        let m = &mut val;
        
        println!("{}", m);
    }
}