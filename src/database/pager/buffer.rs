use std::{cell::RefCell, collections::HashMap, rc::Rc};

use tracing::debug;

use crate::database::types::{Node, Pointer};

#[derive(Debug)]
pub(crate) struct Buffer {
    pub hmap: HashMap<Pointer, BufferEntry>, // in memory buffer
    pub nappend: u64,                        // number of pages to be appended
    pub npages: u64,                         // database size in number of pages
}

#[derive(Debug)]
pub(crate) struct BufferEntry {
    pub node: Rc<RefCell<Node>>,
    pub dirty: bool,   // does it need to be written?
    pub retired: bool, // has the page been deallocated?
}

impl Buffer {
    pub fn get(&self, ptr: Pointer) -> Option<Rc<RefCell<Node>>> {
        Some(self.hmap.get(&ptr)?.node.clone())
    }

    pub fn get_clean(&self, ptr: Pointer) -> Option<Rc<RefCell<Node>>> {
        let n = self.hmap.get(&ptr)?;
        if !n.dirty { None } else { Some(n.node.clone()) }
    }

    /// retrieves all dirty pages in the buffer
    pub fn to_dirty_iter(&self) -> impl Iterator<Item = (Pointer, std::cell::Ref<'_, Node>)> {
        let iter = self
            .hmap
            .iter()
            .filter_map(|e| {
                if e.1.dirty {
                    Some((*e.0, e.1.node.borrow()))
                } else {
                    None
                }
            })
            .into_iter();
        iter
    }

    /// removes retired pages, marks dirty pages as clean
    pub fn clear(&mut self) {
        let mut v = vec![];

        for (p, entry) in self.hmap.iter_mut() {
            if entry.dirty {
                entry.dirty = false;
            }
            if entry.retired {
                v.push(*p);
            }
        }

        // removing retired pages from buffer
        for p in v.iter() {
            let k = self.hmap.remove(p);
            debug_assert!(k.is_some());
        }
    }

    pub fn insert_clean(&mut self, ptr: Pointer, node: Node) {
        self.hmap.insert(
            ptr,
            BufferEntry {
                node: Rc::new(RefCell::new(node)),
                dirty: false,
                retired: false,
            },
        );
    }

    pub fn insert_dirty(&mut self, ptr: Pointer, node: Node) -> Option<()> {
        self.hmap
            .insert(
                ptr,
                BufferEntry {
                    node: Rc::new(RefCell::new(node)),
                    dirty: true,
                    retired: false,
                },
            )
            .map(|_| ())
    }

    pub fn delete(&mut self, ptr: Pointer) {
        self.hmap.remove(&ptr);
    }

    /// helper function for debugging purposes
    pub fn debug_print(&self) {
        #[cfg(test)]
        {
            if let Ok("debug") = std::env::var("RUSQL_LOG_PAGER").as_deref() {
                debug!("current buffer:");
                debug!("---------");
                for e in self.hmap.iter() {
                    let n = e.1.node.borrow();
                    debug!(
                        "{:<10}, {:<10}, dirty = {:<10}",
                        e.0,
                        n.get_type(),
                        e.1.dirty
                    )
                }
                debug!("---------")
            }
        }
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
        let mut buf = Buffer {
            hmap: HashMap::new(),
            nappend: 0,
            npages: 0,
        };

        let node = create_test_node();
        let ptr = Pointer::from(1u64);

        buf.insert_clean(ptr, node);

        assert!(buf.get(ptr).is_some());
        assert_eq!(buf.hmap.len(), 1);
    }

    #[test]
    fn buffer_insert_and_get_dirty() {
        let mut buf = Buffer {
            hmap: HashMap::new(),
            nappend: 0,
            npages: 0,
        };

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
        let buf = Buffer {
            hmap: HashMap::new(),
            nappend: 0,
            npages: 0,
        };

        let ptr = Pointer::from(999u64);
        assert!(buf.get(ptr).is_none());
    }

    #[test]
    fn buffer_get_clean_only_returns_dirty_pages() {
        let mut buf = Buffer {
            hmap: HashMap::new(),
            nappend: 0,
            npages: 0,
        };

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
        let mut buf = Buffer {
            hmap: HashMap::new(),
            nappend: 0,
            npages: 0,
        };

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
        let mut buf = Buffer {
            hmap: HashMap::new(),
            nappend: 0,
            npages: 0,
        };

        let clean_node = create_test_node();
        let dirty_node = create_test_node();
        let clean_ptr = Pointer::from(1u64);
        let dirty_ptr = Pointer::from(2u64);

        buf.insert_clean(clean_ptr, clean_node);
        buf.insert_dirty(dirty_ptr, dirty_node);

        assert!(!buf.hmap[&clean_ptr].dirty);
        assert!(buf.hmap[&dirty_ptr].dirty);
    }

    #[test]
    fn buffer_retired_flag_set_correctly() {
        let mut buf = Buffer {
            hmap: HashMap::new(),
            nappend: 0,
            npages: 0,
        };

        let node = create_test_node();
        let ptr = Pointer::from(1u64);

        buf.insert_clean(ptr, node);
        assert!(!buf.hmap[&ptr].retired);

        // Manually retire for testing
        buf.hmap.get_mut(&ptr).unwrap().retired = true;
        assert!(buf.hmap[&ptr].retired);
    }

    #[test]
    fn buffer_clear_removes_retired_pages() {
        let mut buf = Buffer {
            hmap: HashMap::new(),
            nappend: 0,
            npages: 0,
        };

        let node1 = create_test_node();
        let node2 = create_test_node();
        let node3 = create_test_node();

        buf.insert_dirty(Pointer::from(1u64), node1);
        buf.insert_dirty(Pointer::from(2u64), node2);
        buf.insert_dirty(Pointer::from(3u64), node3);

        // Mark page 2 as retired
        buf.hmap.get_mut(&Pointer::from(2u64)).unwrap().retired = true;

        buf.clear();

        assert_eq!(buf.hmap.len(), 2);
        assert!(buf.get(Pointer::from(2u64)).is_none());
        assert!(buf.get(Pointer::from(1u64)).is_some());
        assert!(buf.get(Pointer::from(3u64)).is_some());
    }

    #[test]
    fn buffer_clear_marks_dirty_as_clean() {
        let mut buf = Buffer {
            hmap: HashMap::new(),
            nappend: 0,
            npages: 0,
        };

        let node = create_test_node();
        let ptr = Pointer::from(1u64);

        buf.insert_dirty(ptr, node);
        assert!(buf.hmap[&ptr].dirty);

        buf.clear();

        assert!(!buf.hmap[&ptr].dirty);
    }

    #[test]
    fn buffer_to_dirty_iter_only_returns_dirty() {
        let mut buf = Buffer {
            hmap: HashMap::new(),
            nappend: 0,
            npages: 0,
        };

        buf.insert_clean(Pointer::from(1u64), create_test_node());
        buf.insert_dirty(Pointer::from(2u64), create_test_node());
        buf.insert_dirty(Pointer::from(3u64), create_test_node());

        let dirty_count = buf.to_dirty_iter().count();
        assert_eq!(dirty_count, 2);
    }

    #[test]
    fn buffer_delete_removes_page() {
        let mut buf = Buffer {
            hmap: HashMap::new(),
            nappend: 0,
            npages: 0,
        };

        let node = create_test_node();
        let ptr = Pointer::from(1u64);

        buf.insert_clean(ptr, node);
        assert!(buf.get(ptr).is_some());

        buf.delete(ptr);
        assert!(buf.get(ptr).is_none());
    }

    #[test]
    fn buffer_multiple_retirements_and_clear() {
        let mut buf = Buffer {
            hmap: HashMap::new(),
            nappend: 0,
            npages: 0,
        };

        for i in 1..=5 {
            buf.insert_dirty(Pointer::from(i as u64), create_test_node());
        }

        // Retire pages 2, 3, and 5
        for ptr in &[
            Pointer::from(2u64),
            Pointer::from(3u64),
            Pointer::from(5u64),
        ] {
            buf.hmap.get_mut(ptr).unwrap().retired = true;
        }

        assert_eq!(buf.hmap.len(), 5);
        buf.clear();
        assert_eq!(buf.hmap.len(), 2);

        // Only pages 1 and 4 should remain
        assert!(buf.get(Pointer::from(1u64)).is_some());
        assert!(buf.get(Pointer::from(4u64)).is_some());
        assert!(buf.get(Pointer::from(2u64)).is_none());
    }

    #[test]
    fn buffer_insert_dirty_overwrites_existing() {
        let mut buf = Buffer {
            hmap: HashMap::new(),
            nappend: 0,
            npages: 0,
        };

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
    fn buffer_nappend_and_npages_tracking() {
        let mut buf = Buffer {
            hmap: HashMap::new(),
            nappend: 5,
            npages: 10,
        };

        assert_eq!(buf.nappend, 5);
        assert_eq!(buf.npages, 10);

        buf.nappend = 0;
        buf.npages = 15;

        assert_eq!(buf.nappend, 0);
        assert_eq!(buf.npages, 15);
    }

    #[test]
    fn buffer_large_page_count() {
        let mut buf = Buffer {
            hmap: HashMap::new(),
            nappend: 0,
            npages: 0,
        };

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
        let mut buf = Buffer {
            hmap: HashMap::new(),
            nappend: 0,
            npages: 0,
        };

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
