use std::cell::RefCell;
use std::collections::HashMap;

use crate::database::node::Node;

thread_local! {
    pub static FREELIST: RefCell<Vec<u64>> = RefCell::new(Vec::from_iter(1..20));
    pub static PAGER: RefCell<HashMap<u64, Node>> = RefCell::new(HashMap::new());
}

/// loads page into memoory as a node
pub(crate) fn node_get(ptr: u64) -> Node {
    PAGER.with_borrow_mut(|x| x.get(&ptr).expect("couldnt get() page").clone())
}

/// finds a free spot to write node to
pub(crate) fn node_encode(node: Node) -> u64 {
    let free_page = FREELIST.with_borrow_mut(|v| v.pop().expect("no free page available"));
    PAGER.with_borrow_mut(|p| p.insert(free_page, node));
    free_page
}

/// deallocate page
pub(crate) fn node_dealloc(ptr: u64) {
    FREELIST.with_borrow_mut(|v| v.push(ptr));
    PAGER
        .with_borrow_mut(|x| x.remove(&ptr))
        .expect("couldnt remove() page number");
}

// // retrieve page content from page number
// pub fn get_node(page_number: u64) -> Result<Self, Error> {
//     let mut file = File::open("database.rdb")?;
//     let mut new_page = Node::new();
//     file.seek(io::SeekFrom::Start(PAGE_SIZE as u64 * page_number))?;
//     file.read(&mut *new_page.0)?;
//     Ok(new_page)
// }
