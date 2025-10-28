use std::fmt::Error;
use std::{collections::HashMap};
use std::io::{self, Read, Seek};
use std::fs::File;

const PAGE_SIZE: usize = 4096;

pub struct Page(Box<[u8; PAGE_SIZE]>);

impl Page {
    // new empty page
    fn new() -> Self {
        Page(Box::new([0u8; PAGE_SIZE]))
    }
    // retrieve page content from page number
    pub fn get(page_number: u64) -> Result<Self, io::Error> {
        let mut file = File::open("database.rdb")?;
        let mut new_page = Page::new();
        file.seek(io::SeekFrom::Start(PAGE_SIZE as u64 * page_number));
        file.read(&mut *new_page.0)?;
        Ok(new_page)

    }
    // encodes Node into Page
    pub fn encode_from_node<K, V>(&mut self, data: Node<K, V>) -> Result<(), Error> {

    }
    // writes page into database and returns page number
    pub fn insert(self) -> Result<u64, Error> {
        
    }
}

pub struct BTree {
    root: usize,
}

pub struct Node<K, V> {
    btype: BType,
    // cell count
    nkeys: u16,
    pointers: Vec<u64>,
    offsets: Vec<u8>,
    key_values: KV<K, V>
}

enum BType {
    Node,
    Leaf
}
struct KV<K, V> {
    klen: u16,
    vlen: u16,
    data: HashMap<K, V>
}


impl<K, V> Node {
    fn new() -> Self {

    }

    fn decode_from_page(page: Page) -> Node<K, V> {

    }
    fn check_size(&self) -> Result<(), Error> {
        
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test {

    }
}