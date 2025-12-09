use std::{
    cell::RefCell,
    rc::{Rc, Weak},
};
use crate::database::{errors::Error, types::{Node, Pointer}};

struct Pager<C, T, FL>
where
    C: Codec,
    T: Tree,
    FL: GC,
{
    codec: Rc<C>,
    freelist: FL,
    btree: T,
}

impl<C, T, FL> Paging for Pager<C, T, FL>
where
    C: Codec,
    T: Tree,
    FL: GC,
{
    fn get(key: &str) -> Result<(), Error> {
        todo!()
    }

    fn set(key: &str, value: &str) -> Result<(), Error> {
        todo!()
    }

    fn del(key: &str) -> Result<(), Error> {
        todo!()
    }
}

trait Paging {
    fn get(key: &str) -> Result<(), Error>;
    fn set(key: &str, value: &str) -> Result<(), Error>;
    fn del(key: &str) -> Result<(), Error>;
}

struct DiskCodec<'a, FL: GC> {
    freelist: &'a FL,
    data: RefCell<Vec<u8>>,
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
trait Codec {
    fn encode(ptr: Pointer) -> Node;
    fn decode(node: Node) -> Pointer;
    fn dalloc(ptr: Pointer);
}

struct BTree<C: Codec> {
    root: Option<u64>,
    codec: Weak<C>,
}

impl<C: Codec> Tree for BTree<C> {
    fn insert(key: &str, value: &str) -> Result<(), Error> {
        todo!()
    }

    fn delete(key: &str) -> Result<(), Error> {
        todo!()
    }

    fn search(key: &str) -> Option<String> {
        todo!()
    }
}

trait Tree {
    fn insert(key: &str, value: &str) -> Result<(), Error>;
    fn delete(key: &str) -> Result<(), Error>;
    fn search(key: &str) -> Option<String>;
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
    fn get() -> Pointer {
        todo!()
    }

    fn append(ptr: Pointer) {
        todo!()
    }
}
trait GC {
    fn get() -> Pointer;
    fn append(ptr: Pointer);
}


fn main() {
    let freelist = Freelist {
        list: None,
        codec: ,
    }
    let disk = Rc::new(DiskCodec {
        freelist: todo!(),
        data: todo!(),
    })
}
