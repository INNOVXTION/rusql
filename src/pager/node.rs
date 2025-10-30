use std::{collections::HashMap};
use std::io::{self, Read, Seek};
use std::fs::File;
use tracing::{info, instrument, error};
use crate::{BLOCK_SIZE, helper::*};

use crate::errors::Error;


const PAGE_SIZE: usize = 100; // 4096 bytes
const HEADER: usize = 4;
const BTREE_MAX_KEY_SIZE: usize = 1000;
const BTREE_MAX_VAL_SIZE: usize = 3000;

/* -------------------------------Node Layout----------------------------------
| type | nkeys | pointers | offsets |            key-values           | unused |
|   2  |   2   | nil nil  |  8 19   | 2 2 "k1" "hi"  2 5 "k3" "hello" |        |
|  2B  |  2B   |   2×8B   |  2×2B   | 4B + 2B + 2B + 4B + 2B + 5B     |        |
*/

#[derive(Debug)]
pub struct Node(pub Box<[u8]>);

#[derive(PartialEq, Debug)]
pub enum NodeType {
    Node,
    Leaf
}

impl Node {
    // new empty page
    fn new() -> Self {
        Node(Box::new([0u8; PAGE_SIZE]))
    }
    // retrieve page content from page number
    #[instrument]
    pub fn get_node(page_number: u64) -> Result<Self, Error> {
        let mut file = File::open("database.rdb")?;
        let mut new_page = Node::new();
        file.seek(io::SeekFrom::Start(PAGE_SIZE as u64 * page_number))?;
        file.read(&mut *new_page.0)?;
        Ok(new_page)
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
        slice_to_u16(&self, 2).unwrap()
        // let mut val = [0u8;2];
        // val.copy_from_slice(&self.0[2..4]);
        // u16::from_le_bytes(val)
    }
    #[instrument]
    pub fn set_header(&mut self, nodetype: NodeType, nkeys: u16) -> Result<(), Error> {
        let nodetype: u16 = match nodetype {
            NodeType::Node => 1,
            NodeType::Leaf => 2,
        };
        self.0[..2].copy_from_slice(&nodetype.to_le_bytes());
        self.0[2..4].copy_from_slice(&nkeys.to_le_bytes());
        Ok(())
    }
    #[instrument]
    pub fn get_ptr(&self, idx: u16) -> Result<u64, Error> {
        if idx > self.get_nkeys() {
            return Err(Error::IndexError)
        };
        let pos: usize = HEADER + 8 * idx as usize;
        
        // let mut val = [0u8;8];
        // val.copy_from_slice(&self.0[pos..pos + 8]);
        // Ok(u64::from_le_bytes(val))
        Ok(slice_to_u64(&self, pos)?)
    }
    #[instrument]
    pub fn set_ptr(&mut self, idx: u16, ptr: u64) {
        let pos: usize = HEADER + 8 * idx as usize;
        self.0[pos..pos + 8].copy_from_slice(&ptr.to_le_bytes());
    }
    // reads the value from the offset array for a given index, 0 has no offset
    #[instrument]
    fn get_offset(&self, idx: u16) -> Result<u16, Error> {
        if idx == 0 {
            return Ok(0)
        }
        if idx > self.get_nkeys() {
            error!("get_offset error");
            return Err(Error::IndexError)
        }
        let pos = HEADER + (8 * self.get_nkeys() as usize) + 2 * (idx as usize - 1);
        Ok(slice_to_u16(&self, pos)?)
    }
    #[instrument]
    fn set_offset(&mut self, idx: u16, size: u16) {
        let pos = HEADER + (8 * self.get_nkeys() as usize) + 2 * (idx as usize - 1);

    }
    // locats kv position relative to node
    #[instrument]
    fn kv_pos(&self, idx: u16) -> Result<usize, Error> {
        if idx > self.get_nkeys() {
            error!("kv_pos error");
            return Err(Error::IndexError)
        };
        Ok((HEADER as u16 + (8 * self.get_nkeys()) + 2 * self.get_nkeys() + self.get_offset(idx)?) as usize)
    }
    #[instrument]
    pub fn get_key(&self, idx: u16) -> Result<&[u8], Error> {
        if idx > self.get_nkeys() {
            error!("get_key error");
            return Err(Error::IndexError)
        };
        let kvpos = self.kv_pos(idx)?;
        let key_len = slice_to_u16(&self, kvpos)? as usize;
        
        Ok(&self.0[kvpos + 4..kvpos + 4 + key_len])
    }
    #[instrument]
    pub fn get_val(&self, idx: u16) -> Result<&[u8], Error> {
        if idx > self.get_nkeys() {
            error!("get_val error");
            return Err(Error::IndexError)
        };    
        let kvpos = self.kv_pos(idx)?;
        let key_len = slice_to_u16(&self, kvpos)? as usize;
        let val_len = slice_to_u16(&self, kvpos + 2)? as usize;

        Ok(&self.0[kvpos + 4 + key_len..kvpos + 4 + key_len + val_len])
    }
    #[instrument]
    pub fn append_kv(&mut self, idx: u16, ptr: u64, key: &str, val: &str) -> Result<(),Error> {
        self.set_ptr(idx, ptr);
        
        let kvpos = self.kv_pos(idx).expect("index error append kv");
        let klen: u16 = key.len().try_into()?;
        let vlen: u16 = val.len().try_into()?;
        self.0[kvpos..kvpos + 2].copy_from_slice(&klen.to_le_bytes());
        self.0[kvpos + 2..kvpos + 4].copy_from_slice(&vlen.to_le_bytes());

        self.0[kvpos + 4..kvpos + 4 + key.len()]
            .copy_from_slice(key.as_bytes());
        self.0[kvpos + 4 + key.len()..kvpos + 4 + key.len() + val.len()]
            .copy_from_slice(val.as_bytes());

        self.set_offset(idx + 1, self.get_offset(idx)? + klen + vlen + 5);
        Ok(())

    }
}





#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_type_nkeys_setheader() {
        let mut page = Node(Box::new([0u8; 50]));
        page.set_header(NodeType::Node, 5).unwrap();

        assert_eq!(page.get_type().unwrap(), NodeType::Node);
        assert_eq!(page.get_nkeys(), 5);

    }

    #[test]
    fn test_ptr() {
        let mut page = Node(Box::new([0u8; 100]));
        page.set_header(NodeType::Node, 5).unwrap();

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
    fn test_append() {
        let mut node = Node::new();
        node.set_header(NodeType::Leaf, 2);
        node.append_kv(0, 0, "k1", "hi");
        node.append_kv(1, 0, "k3", "hello");
    }
}