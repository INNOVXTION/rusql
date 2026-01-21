use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use parking_lot::Mutex;
use tracing::{debug, error};

use crate::database::{
    pager::lru::LRU,
    types::{LRU_BUFFER_SIZE, Node, Pointer},
};

const DISKBUFFER_LIMIT: usize = 50;

/// Globally shared buffer, used by the freelist
#[derive(Debug)]
pub(crate) struct DiskBuffer {
    pub hmap: HashMap<Pointer, BufferEntry>,
    pub nappend: u64, // number of pages to be appended
}

#[derive(Debug)]
pub(crate) struct BufferEntry {
    pub node: Node,
    pub dirty: bool, // does it need to be written? Only relevant for free list
}

impl DiskBuffer {
    pub fn new() -> Self {
        Self {
            hmap: HashMap::with_capacity(DISKBUFFER_LIMIT),
            nappend: 0,
        }
    }

    pub fn get(&self, ptr: Pointer) -> Option<&Node> {
        self.hmap.get(&ptr).map(|n| &n.node)
    }

    pub fn get_mut(&mut self, ptr: Pointer) -> Option<&mut Node> {
        self.hmap.get_mut(&ptr).map(|n| &mut n.node)
    }

    pub fn set_dirty(&mut self, ptr: &Pointer) {
        self.hmap.entry(*ptr).and_modify(|e| e.dirty = true);
    }

    pub fn get_clean(&self, ptr: Pointer) -> Option<Node> {
        self.hmap
            .get(&ptr)
            .and_then(|n| if !n.dirty { None } else { Some(n.node.clone()) })
    }

    /// retrieves all dirty pages in the buffer
    pub fn to_dirty_iter(&self) -> impl Iterator<Item = (Pointer, &Node)> {
        self.hmap
            .iter()
            .filter_map(|e| {
                if e.1.dirty {
                    Some((*e.0, &e.1.node))
                } else {
                    None
                }
            })
            .into_iter()
    }

    /// marks dirty pages as clean
    pub fn clear(&mut self) {
        for (p, entry) in self.hmap.iter_mut() {
            if entry.dirty {
                entry.dirty = false;
            }
        }
    }

    fn evict_clean(&mut self) -> u16 {
        let clean_node: Vec<Pointer> = self
            .hmap
            .iter()
            .filter_map(|e| if !e.1.dirty { Some(*e.0) } else { None })
            .collect();

        let mut c = 0;
        for ptr in clean_node {
            self.delete(ptr);
            c += 1;
        }
        c
    }

    pub fn insert_clean(&mut self, ptr: Pointer, node: Node) {
        if self.hmap.len() >= DISKBUFFER_LIMIT {
            if self.evict_clean() == 0 {
                error!("fatal error: couldnt evict a page from buffer");
                panic!("this shouldnt be possible")
            };
        };

        self.hmap.insert(
            ptr,
            BufferEntry {
                node,
                dirty: false,
                // retired: false,
            },
        );
    }

    pub fn insert_dirty(&mut self, ptr: Pointer, node: Node) -> Option<()> {
        if self.hmap.len() >= DISKBUFFER_LIMIT {
            self.evict_clean();
        };

        self.hmap
            .insert(
                ptr,
                BufferEntry {
                    node,
                    dirty: true,
                    // retired: false,
                },
            )
            .map(|_| ())
    }

    pub fn delete(&mut self, ptr: Pointer) {
        self.hmap.remove(&ptr);
    }

    pub fn debug_print(&self) {
        #[cfg(test)]
        {
            if let Ok("debug") = std::env::var("RUSQL_LOG_PAGER").as_deref() {
                debug!(buf_len = self.hmap.len(), "current fl buffer:");
                debug!("{:-<10}", "-");
                for e in self.hmap.iter() {
                    let n = &e.1.node;
                    debug!(
                        "{:<10}, {:<10}, dirty = {:<10}",
                        e.0,
                        n.get_type(),
                        e.1.dirty
                    )
                }
                debug!("{:-<10}", "-");
            }
        }
    }

    pub fn erase(&mut self) {
        self.hmap.clear();
        self.nappend = 0;
    }
}

pub(crate) struct SharedBuffer(LRU<Pointer, SharedBufferEntry>);

impl SharedBuffer {
    pub fn new() -> Self {
        SharedBuffer(LRU::new(LRU_BUFFER_SIZE))
    }

    pub fn insert(&mut self, ptr: Pointer, node: Node, version: u64) {
        self.0.insert(
            ptr,
            SharedBufferEntry {
                node: Arc::new(node),
                version,
            },
        );
    }

    pub fn get(&mut self, ptr: Pointer, version: u64) -> Option<Arc<Node>> {
        if self.0.peek(&ptr)?.version == version {
            Some(self.0.get(ptr).expect("we just checked").node.clone())
        } else {
            None
        }
    }

    pub fn clear(&mut self) {
        self.0.clear()
    }
}

struct SharedBufferEntry {
    pub node: Arc<Node>,
    pub version: u64,
}

#[derive(Debug)]
pub(crate) struct OngoingTX {
    pub map: BTreeMap<u64, u16>,
}

impl OngoingTX {
    fn debug_ongoing(&self) {
        #[cfg(test)]
        {
            if let Ok("debug") = std::env::var("RUSQL_LOG_ONGOING").as_deref() {
                debug!("{:-<10}", "-");
                debug!("ongoing len: {}", self.map.len());
                for e in self.map.iter() {
                    debug!("version: {}, amount: {}", e.0, e.1);
                }
                debug!("{:-<10}", "-");
            }
        }
    }

    pub fn push(&mut self, version: u64) {
        self.map.entry(version).and_modify(|e| *e += 1).or_insert(1);
        debug!("adding new ongoing TX: {version}");
        self.debug_ongoing();
        ()
    }

    pub fn pop(&mut self, version: u64) {
        use std::collections::btree_map::Entry as E;

        match self.map.entry(version) {
            E::Occupied(mut occupied_entry) => {
                // decrement
                debug!("decrementing {version} from ongoing");
                *occupied_entry.get_mut() -= 1;
                // remove
                let n = *occupied_entry.get();
                if n == 0 {
                    debug!("removing {version} from ongoing");
                    occupied_entry.remove();
                }
            }
            E::Vacant(vacant_entry) => (),
        };
        self.debug_ongoing();
    }

    pub fn get_oldest_version(&mut self) -> Option<u64> {
        let r = self.map.first_entry().map(|e| *e.key());
        debug!(r, "oldest version");
        self.debug_ongoing();
        r
    }
}

#[cfg(test)]
mod buffer_tests {
    use super::*;
    use crate::database::btree::TreeNode;
    use crate::database::types::Node;

    fn create_test_node() -> Node {
        Node::Tree(TreeNode::new())
    }

    #[test]
    fn buffer_insert_and_get_clean() {
        let mut buf = DiskBuffer::new();

        let node = create_test_node();
        let ptr = Pointer::from(1u64);

        buf.insert_clean(ptr, node);

        assert!(buf.get(ptr).is_some());
        assert_eq!(buf.hmap.len(), 1);
    }

    #[test]
    fn buffer_insert_and_get_dirty() {
        let mut buf = DiskBuffer::new();

        let node = create_test_node();
        let ptr = Pointer::from(1u64);

        let result = buf.insert_dirty(ptr, node);

        assert!(result.is_none()); // dirty pointer didnt exists
        assert!(buf.get(ptr).is_some());
        assert_eq!(buf.hmap.len(), 1);
        assert!(buf.hmap[&ptr].dirty);
    }

    #[test]
    fn buffer_get_returns_none_for_missing_key() {
        let buf = DiskBuffer::new();

        let ptr = Pointer::from(999u64);
        assert!(buf.get(ptr).is_none());
    }

    #[test]
    fn buffer_get_clean_only_returns_dirty_pages() {
        let mut buf = DiskBuffer::new();

        let clean_node = create_test_node();
        let dirty_node = create_test_node();
        let clean_ptr = Pointer::from(1u64);
        let dirty_ptr = Pointer::from(2u64);

        buf.insert_clean(clean_ptr, clean_node);
        buf.insert_dirty(dirty_ptr, dirty_node);

        // get_clean should only return dirty pages
        assert!(buf.get_clean(clean_ptr).is_none());
        assert!(buf.get_clean(dirty_ptr).is_some());
    }

    #[test]
    fn buffer_multiple_inserts() {
        let mut buf = DiskBuffer::new();

        for i in 1..=10 {
            let node = create_test_node();
            let ptr = Pointer::from(i as u64);
            buf.insert_dirty(ptr, node);
        }

        assert_eq!(buf.hmap.len(), 10);

        for i in 1..=10 {
            let ptr = Pointer::from(i as u64);
            assert!(buf.get(ptr).is_some());
        }
    }

    #[test]
    fn buffer_dirty_flag_set_correctly() {
        let mut buf = DiskBuffer::new();

        let clean_node = create_test_node();
        let dirty_node = create_test_node();
        let clean_ptr = Pointer::from(1u64);
        let dirty_ptr = Pointer::from(2u64);

        buf.insert_clean(clean_ptr, clean_node);
        buf.insert_dirty(dirty_ptr, dirty_node);

        assert!(!buf.hmap[&clean_ptr].dirty);
        assert!(buf.hmap[&dirty_ptr].dirty);
    }

    // #[test]
    // fn buffer_retired_flag_set_correctly() {
    //     let mut buf = NodeBuffer {
    //         hmap: HashMap::new(),
    //         nappend: 0,
    //         npages: 0,
    //     };

    //     let node = create_test_node();
    //     let ptr = Pointer::from(1u64);

    //     buf.insert_clean(ptr, node, 0);
    //     assert!(!buf.hmap[&ptr].retired);

    //     // Manually retire for testing
    //     buf.hmap.get_mut(&ptr).unwrap().retired = true;
    //     assert!(buf.hmap[&ptr].retired);
    // }

    // #[test]
    // fn buffer_clear_removes_retired_pages() {
    //     let mut buf = NodeBuffer {
    //         hmap: HashMap::new(),
    //         nappend: 0,
    //         npages: 0,
    //     };

    //     let node1 = create_test_node();
    //     let node2 = create_test_node();
    //     let node3 = create_test_node();

    //     buf.insert_dirty(Pointer::from(1u64), node1, 0);
    //     buf.insert_dirty(Pointer::from(2u64), node2, 0);
    //     buf.insert_dirty(Pointer::from(3u64), node3, 0);

    //     // Mark page 2 as retired
    //     buf.hmap.get_mut(&Pointer::from(2u64)).unwrap().retired = true;

    //     buf.clear();

    //     assert_eq!(buf.hmap.len(), 2);
    //     assert!(buf.get(Pointer::from(2u64)).is_none());
    //     assert!(buf.get(Pointer::from(1u64)).is_some());
    //     assert!(buf.get(Pointer::from(3u64)).is_some());
    // }

    #[test]
    fn buffer_clear_marks_dirty_as_clean() {
        let mut buf = DiskBuffer::new();

        let node = create_test_node();
        let ptr = Pointer::from(1u64);

        buf.insert_dirty(ptr, node);
        assert!(buf.hmap[&ptr].dirty);

        buf.clear();

        assert!(!buf.hmap[&ptr].dirty);
    }

    #[test]
    fn buffer_to_dirty_iter_only_returns_dirty() {
        let mut buf = DiskBuffer::new();

        buf.insert_clean(Pointer::from(1u64), create_test_node());
        buf.insert_dirty(Pointer::from(2u64), create_test_node());
        buf.insert_dirty(Pointer::from(3u64), create_test_node());

        let dirty_count = buf.to_dirty_iter().count();
        assert_eq!(dirty_count, 2);
    }

    #[test]
    fn buffer_delete_removes_page() {
        let mut buf = DiskBuffer::new();

        let node = create_test_node();
        let ptr = Pointer::from(1u64);

        buf.insert_clean(ptr, node);
        assert!(buf.get(ptr).is_some());

        buf.delete(ptr);
        assert!(buf.get(ptr).is_none());
    }

    // #[test]
    // fn buffer_multiple_retirements_and_clear() {
    //     let mut buf = NodeBuffer {
    //         hmap: HashMap::new(),
    //         nappend: 0,
    //         npages: 0,
    //     };

    //     for i in 1..=5 {
    //         buf.insert_dirty(Pointer::from(i as u64), create_test_node(), 0);
    //     }

    //     // Retire pages 2, 3, and 5
    //     for ptr in &[
    //         Pointer::from(2u64),
    //         Pointer::from(3u64),
    //         Pointer::from(5u64),
    //     ] {
    //         buf.hmap.get_mut(ptr).unwrap().retired = true;
    //     }

    //     assert_eq!(buf.hmap.len(), 5);
    //     buf.clear();
    //     assert_eq!(buf.hmap.len(), 2);

    //     // Only pages 1 and 4 should remain
    //     assert!(buf.get(Pointer::from(1u64)).is_some());
    //     assert!(buf.get(Pointer::from(4u64)).is_some());
    //     assert!(buf.get(Pointer::from(2u64)).is_none());
    // }

    #[test]
    fn buffer_insert_dirty_overwrites_existing() {
        let mut buf = DiskBuffer::new();

        let ptr = Pointer::from(1u64);
        let node1 = create_test_node();
        let node2 = create_test_node();

        buf.insert_dirty(ptr, node1);
        let result = buf.insert_dirty(ptr, node2);

        // insert_dirty should return Some(_) on overwrite
        assert!(result.is_some());
        assert_eq!(buf.hmap.len(), 1);
    }

    #[test]
    fn buffer_large_page_count() {
        let mut buf = DiskBuffer::new();

        // Insert many pages
        for i in 0..1000 {
            let ptr = Pointer::from(i as u64);
            buf.insert_dirty(ptr, create_test_node());
        }

        assert_eq!(buf.hmap.len(), 1000);

        // Verify random access
        assert!(buf.get(Pointer::from(500u64)).is_some());
        assert!(buf.get(Pointer::from(999u64)).is_some());
        assert!(buf.get(Pointer::from(1000u64)).is_none());
    }

    #[test]
    fn buffer_mixed_clean_and_dirty_operations() {
        let mut buf = DiskBuffer::new();

        for i in 1..=10 {
            let node = create_test_node();
            let ptr = Pointer::from(i as u64);
            if i % 2 == 0 {
                buf.insert_dirty(ptr, node);
            } else {
                buf.insert_clean(ptr, node);
            }
        }

        let dirty_count = buf.to_dirty_iter().count();
        assert_eq!(dirty_count, 5); // Even numbers

        buf.clear();

        for i in 1..=10 {
            let ptr = Pointer::from(i as u64);
            if i % 2 == 0 {
                assert!(!buf.hmap[&ptr].dirty);
            }
        }
    }
}
