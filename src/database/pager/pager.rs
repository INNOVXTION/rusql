use crate::database::{
    errors::Error,
    helper::input_valid,
    types::{Node, Pointer},
};
use std::{
    cell::RefCell,
    rc::{Rc, Weak},
};

trait Pager {
    fn get();
    fn set();
    fn del();
}

impl<'a, FL: GC> Codec for DiskCodec<'a, FL> {
    fn encode(ptr: Pointer) -> Node {
        todo!()
    }

    fn decode(node: Node) -> Pointer {
        todo!()
    }

    fn dalloc(ptr: Pointer) {
        todo!()
    }
}

pub trait Codec {
    fn encode(ptr: Pointer) -> Node;
    fn decode(node: Node) -> Pointer;
    fn dalloc(ptr: Pointer);
}

struct BTree<C: Codec> {
    root: Option<u64>,
    codec: Weak<C>,
}

impl<C: Codec> Tree for BTree<C> {
    fn insert(&mut self, key: &str, value: &str) -> Result<(), Error> {
        todo!()
    }

    fn delete(&mut self, key: &str) -> Result<(), Error> {
        todo!()
    }

    fn search(&self, key: &str) -> Option<String> {
        todo!()
    }
}

trait Tree {
    fn insert(&mut self, key: &str, value: &str) -> Result<(), Error>;
    fn delete(&mut self, key: &str) -> Result<(), Error>;
    fn search(&self, key: &str) -> Option<String>;
}

impl<C: Codec> BTree<C> {
    fn insert(&self, key: &str) {
        println!("inserting something")
    }
}

struct Freelist<C: Codec> {
    list: Option<u64>,
    codec: Weak<C>,
}
impl<C: Codec> GC for Freelist<C> {
    fn get(&mut self) -> Pointer {
        todo!()
    }

    fn append(&mut self, ptr: Pointer) {
        todo!()
    }
}
trait GC {
    fn get(&mut self) -> Pointer;
    fn append(&mut self, ptr: Pointer);
}
