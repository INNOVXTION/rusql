use std::{collections::HashMap};
use std::io::{self, Read, Seek};
use std::fs::File;
use tracing::{info, instrument};

use crate::errors::Error;


const PAGE_SIZE: usize = 100; // 4096 bytes
const HEADER: usize = 4;
const BTREE_MAX_KEY_SIZE: usize = 1000;
const BTREE_MAX_VAL_SIZE: usize = 3000;

#[derive(Debug)]
pub struct Node(Box<[u8]>);

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
    pub fn get_node(page_number: u64) -> Result<Self, Error> {
        let mut file = File::open("database.rdb")?;
        let mut new_page = Node::new();
        file.seek(io::SeekFrom::Start(PAGE_SIZE as u64 * page_number))?;
        file.read(&mut *new_page.0)?;
        Ok(new_page)

    }
    // writes page into database and returns page number
    pub fn insert(self) -> Result<u64, Error> {
        todo!()
    }
    
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
        let mut val = [0u8;2];
        val.copy_from_slice(&self.0[2..4]);
        u16::from_le_bytes(val)
    }
    
    pub fn set_header(&mut self, nodetype: NodeType, nkeys: u16) -> Result<(), Error> {
        let nodetype: u16 = match nodetype {
            NodeType::Node => 1,
            NodeType::Leaf => 2,
        };
        self.0[..2].copy_from_slice(&nodetype.to_le_bytes());
        self.0[2..4].copy_from_slice(&nkeys.to_le_bytes());
        Ok(())
    }
    
    
    pub fn get_childptr(&self, idx: u16) -> Result<u64, Error> {
        assert!(idx < self.get_nkeys());
        let pos: usize = (HEADER + 8 * idx).try_into().expect("type casting error");
        
        // let mut val = [0u8;8];
        // val.copy_from_slice(&self.0[pos..pos + 8]);
        // Ok(u64::from_le_bytes(val))
        self.0.get(pos..pos + 8)
            .and_then(|s| s.try_into().ok())
            .map(|arr: [u8; 8]| u64::from_le_bytes(arr))
            .ok_or(Error::CastingError)
    }
    
    pub fn set_childptr(&mut self, idx: u16, ptr: u64) {
        let pos: usize = (HEADER + 8 * idx as usize).try_into().expect("type casting error");
        
        let buf  = ptr.to_le_bytes();
        self.0[pos..pos + 8].copy_from_slice(&buf);
    }

}





#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_type_nkeys_setheader() {
        let mut page = Node(Box::new([0u8; 50]));
        page.set_header(NodeType::Node, 5);

        assert_eq!(page.get_type().unwrap(), NodeType::Node);
        assert_eq!(page.get_nkeys(), 5);

    }

    #[test]
    fn test_ptr() {
        let mut page = Node(Box::new([0u8; 50]));
        page.set_header(NodeType::Node, 5);

        page.set_childptr(1, 10);
        page.set_childptr(2, 20);

        // for byte in page.0.iter() {
        //     print!("{:08b} ", *byte);
        // }
        // for byte in page.0.iter() {
        //     print!("{:02x} ", *byte);
        // }
        assert_eq!(page.get_childptr(1).unwrap(), 10);
        assert_eq!(page.get_childptr(2).unwrap(), 20);
        
    }
}