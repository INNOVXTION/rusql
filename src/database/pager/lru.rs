use std::{collections::HashMap, ptr::null_mut};

pub(crate) struct LRU<K, V>
where
    K: Eq + std::hash::Hash + Copy,
    V: Sized,
{
    map: HashMap<K, Box<Node<K, V>>>,
    ll: LinkedList<K, V>,
    len: usize,
    cap: usize,
}

impl<K, V> LRU<K, V>
where
    K: Eq + std::hash::Hash + Copy,
    V: Sized,
{
    pub fn new(cap: usize) -> Self {
        LRU {
            map: HashMap::with_capacity(cap),
            ll: LinkedList::new(),
            len: 0,
            cap,
        }
    }

    pub fn put(&mut self, key: K, val: V) {
        // Check if key already exists
        if let Some(existing) = self.map.get_mut(&key) {
            existing.val = val;

            self.ll.disconnect_node(&existing);

            let ptr = self.get_ptr(key).expect("key exists");
            self.ll.push_tail(ptr);
            return;
        }

        let new = Box::new(Node {
            key,
            val,
            next: null_mut(),
            prev: null_mut(),
        });

        if self.len < self.cap {
            self.map.insert(key, new);

            let ptr = self.get_ptr(key).expect("we just inserted it");
            self.ll.push_tail(ptr);
        } else {
            self.evict_lru();
            self.map.insert(key, new);

            let new = self.get_ptr(key).expect("we just inserted it");
            self.ll.push_tail(new);
        }
        self.len += 1;
    }

    pub fn get(&mut self, key: K) -> Option<&V> {
        let ptr = self.get_ptr(key)?;
        let n = self.map.get(&key)?;

        self.ll.disconnect_node(&n);
        self.ll.push_tail(ptr);

        Some(&n.val)
    }

    pub fn remove(&mut self, key: K) -> Option<V> {
        let ptr = self.get_ptr(key)?;
        let n = self.map.remove(&key)?;

        self.ll.disconnect_node(&n);
        self.len -= 1;

        Some(n.val)
    }

    fn evict_lru(&mut self) {
        let old = self
            .ll
            .pop_front()
            .expect("we only evict when the cache is capped");
        self.map.remove(&old);
        self.len -= 1;
    }

    fn get_ptr(&mut self, key: K) -> Option<*mut Node<K, V>> {
        self.map
            .get_mut(&key)
            .map(|node| node.as_mut() as *mut Node<K, V>)
    }
}

struct LinkedList<K, V>
where
    K: Eq + std::hash::Hash + Copy,
    V: Sized,
{
    head: *mut Node<K, V>,
    tail: *mut Node<K, V>,
    len: usize,
}

struct Node<K, V> {
    key: K,
    val: V,
    next: *mut Node<K, V>,
    prev: *mut Node<K, V>,
}

impl<K, V> LinkedList<K, V>
where
    K: Eq + std::hash::Hash + Copy,
    V: Sized,
{
    fn new() -> Self {
        LinkedList {
            head: std::ptr::null_mut(),
            tail: std::ptr::null_mut(),
            len: 0,
        }
    }

    fn push_tail(&mut self, new: *mut Node<K, V>) {
        unsafe {
            (*new).next = null_mut();
            (*new).prev = null_mut();

            if !self.tail.is_null() {
                (*self.tail).prev = new;
                (*new).next = self.tail;
            } else {
                self.head = new;
            }

            self.tail = new;
            self.len += 1;
        }
    }

    fn disconnect_node(&mut self, node: &Node<K, V>) {
        unsafe {
            let next = node.next;
            let prev = node.prev;

            // node is already disconnected
            if next.is_null() && prev.is_null() {
                // its the only node
                if self.head as *const _ == node as *const _ {
                    self.head = null_mut();
                    self.tail = null_mut();
                    self.len -= 1;
                }
                return;
            }

            // its the head node
            if next.is_null() {
                (*prev).next = null_mut();
                self.head = prev;
                self.len -= 1;
                return;
            }

            // its the tail node
            if prev.is_null() {
                (*next).prev = null_mut();
                self.tail = next;
                self.len -= 1;
                return;
            }

            // the node is inbetween
            (*next).prev = prev;
            (*prev).next = next;
            self.len -= 1;
        }
    }

    fn pop_front(&mut self) -> Option<K> {
        unsafe {
            if self.head.is_null() {
                return None;
            }
            let n = self.head;

            // its the only node
            if (*n).next.is_null() && (*n).prev.is_null() {
                self.head = null_mut();
                self.tail = null_mut();
                self.len -= 1;

                return Some((*n).key);
            }

            self.head = (*n).prev;
            (*self.head).next = null_mut();
            self.len -= 1;

            Some((*n).key)
        }
    }
}

#[cfg(test)]
mod lru {
    use super::*;

    #[test]
    fn test_new_cache() {
        let cache: LRU<i32, i32> = LRU::new(3);
        assert_eq!(cache.len, 0);
        assert_eq!(cache.cap, 3);
    }

    #[test]
    fn test_put_single_item() {
        let mut cache = LRU::new(3);
        cache.put(1, 100);
        assert_eq!(cache.len, 1);

        assert_eq!(*cache.get(1).unwrap(), 100);
    }

    #[test]
    fn test_put_multiple_items() {
        let mut cache = LRU::new(3);
        cache.put(1, 100);
        cache.put(2, 200);
        cache.put(3, 300);

        assert_eq!(cache.len, 3);
        assert_eq!(*cache.get(1).unwrap(), 100);
        assert_eq!(*cache.get(2).unwrap(), 200);
        assert_eq!(*cache.get(3).unwrap(), 300);
    }

    #[test]
    fn test_put_exceeds_capacity() {
        let mut cache = LRU::new(3);
        cache.put(1, 100);
        cache.put(2, 200);
        cache.put(3, 300);
        cache.put(4, 400);

        assert_eq!(cache.len, 3);
        assert!(cache.get(1).is_none()); // 1 should be evicted
        assert_eq!(*cache.get(2).unwrap(), 200);
        assert_eq!(*cache.get(3).unwrap(), 300);
        assert_eq!(*cache.get(4).unwrap(), 400);
    }

    #[test]
    fn test_get_nonexistent_key() {
        let mut cache = LRU::new(3);
        cache.put(1, 100);
        assert!(cache.get(999).is_none());
    }

    #[test]
    fn test_get_updates_recency() {
        let mut cache = LRU::new(3);
        cache.put(1, 100);
        cache.put(2, 200);
        cache.put(3, 300);

        // Access key 1, making it most recently used
        cache.get(1);

        // Add key 4, which should evict key 2 (least recently used)
        cache.put(4, 400);

        assert_eq!(*cache.get(1).unwrap(), 100);
        assert!(cache.get(2).is_none()); // 2 should be evicted
        assert_eq!(*cache.get(3).unwrap(), 300);
        assert_eq!(*cache.get(4).unwrap(), 400);
    }

    #[test]
    fn test_eviction_order() {
        let mut cache = LRU::new(2);
        cache.put(1, 100);
        cache.put(2, 200);
        cache.put(3, 300); // Should evict 1

        assert!(cache.get(1).is_none());
        assert_eq!(*cache.get(2).unwrap(), 200); // This moves 2 to MRU

        cache.put(4, 400); // Should evict 3 (not 2, since we just accessed 2)
        assert!(cache.get(3).is_none()); // 3 is evicted
        assert_eq!(*cache.get(2).unwrap(), 200); // 2 still exists
        assert_eq!(*cache.get(4).unwrap(), 400);
    }

    #[test]
    fn test_remove_existing_key() {
        let mut cache = LRU::new(3);
        cache.put(1, 100);
        cache.put(2, 200);

        let removed = cache.remove(1);
        assert_eq!(removed, Some(100));
        assert_eq!(cache.len, 1);
        assert!(cache.get(1).is_none());
    }

    #[test]
    fn test_remove_nonexistent_key() {
        let mut cache = LRU::new(3);
        cache.put(1, 100);

        let removed = cache.remove(999);
        assert!(removed.is_none());
        assert_eq!(cache.len, 1);
    }

    #[test]
    fn test_remove_from_middle() {
        let mut cache = LRU::new(3);
        cache.put(1, 100);
        cache.put(2, 200);
        cache.put(3, 300);

        assert_eq!(cache.remove(2), Some(200)); // Remove middle element
        assert_eq!(cache.len, 2);
        assert_eq!(*cache.get(1).unwrap(), 100);
        assert!(cache.get(2).is_none());
        assert_eq!(*cache.get(3).unwrap(), 300);
    }

    #[test]
    fn test_remove_head() {
        let mut cache = LRU::new(3);
        cache.put(1, 100);
        cache.put(2, 200);
        cache.put(3, 300);

        assert_eq!(cache.remove(3), Some(300)); // Remove head (most recent)
        assert_eq!(cache.len, 2);
        assert_eq!(*cache.get(1).unwrap(), 100);
        assert_eq!(*cache.get(2).unwrap(), 200);
        assert!(cache.get(3).is_none());
    }

    #[test]
    fn test_remove_tail() {
        let mut cache = LRU::new(3);
        cache.put(1, 100);
        cache.put(2, 200);
        cache.put(3, 300);

        assert_eq!(cache.remove(1), Some(100)); // Remove tail (least recent)
        assert_eq!(cache.len, 2);
        assert!(cache.get(1).is_none());
        assert_eq!(*cache.get(2).unwrap(), 200);
        assert_eq!(*cache.get(3).unwrap(), 300);
    }

    #[test]
    fn test_remove_only_element() {
        let mut cache = LRU::new(3);
        cache.put(1, 100);

        assert_eq!(cache.remove(1), Some(100));
        assert_eq!(cache.len, 0);
        assert!(cache.get(1).is_none());
    }

    #[test]
    fn test_capacity_one() {
        let mut cache = LRU::new(1);
        cache.put(1, 100);
        assert_eq!(*cache.get(1).unwrap(), 100);

        cache.put(2, 200);
        assert!(cache.get(1).is_none());
        assert_eq!(*cache.get(2).unwrap(), 200);
    }

    #[test]
    fn test_repeated_access_pattern() {
        let mut cache = LRU::new(3);
        cache.put(1, 100);
        cache.put(2, 200);
        cache.put(3, 300);

        // Access pattern: 1, 1, 2, 1
        cache.get(1);
        cache.get(1);
        cache.get(2);
        cache.get(1);

        // Key 3 is least recently used, should be evicted
        cache.put(4, 400);

        assert_eq!(*cache.get(1).unwrap(), 100);
        assert_eq!(*cache.get(2).unwrap(), 200);
        assert!(cache.get(3).is_none());
        assert_eq!(*cache.get(4).unwrap(), 400);
    }

    #[test]
    fn test_alternating_access() {
        let mut cache = LRU::new(2);
        cache.put(1, 100);
        cache.put(2, 200);

        cache.get(1);
        cache.put(3, 300); // Evicts 2
        assert!(cache.get(2).is_none());

        cache.get(1);
        cache.put(4, 400); // Evicts 3
        assert!(cache.get(3).is_none());

        assert_eq!(*cache.get(1).unwrap(), 100);
        assert_eq!(*cache.get(4).unwrap(), 400);
    }

    #[test]
    fn test_fill_and_drain() {
        let mut cache = LRU::new(3);
        cache.put(1, 100);
        cache.put(2, 200);
        cache.put(3, 300);

        cache.remove(1);
        cache.remove(2);
        cache.remove(3);

        assert_eq!(cache.len, 0);

        // Refill after draining
        cache.put(4, 400);
        cache.put(5, 500);

        assert_eq!(cache.len, 2);
        assert_eq!(*cache.get(4).unwrap(), 400);
        assert_eq!(*cache.get(5).unwrap(), 500);
    }

    #[test]
    fn test_stress_eviction() {
        let mut cache = LRU::new(5);

        // Add 10 items to a cache of size 5
        for i in 0..10 {
            cache.put(i, i * 100);
        }

        assert_eq!(cache.len, 5);

        // Only the last 5 items should remain
        for i in 0..5 {
            assert!(cache.get(i).is_none());
        }

        for i in 5..10 {
            assert_eq!(*cache.get(i).unwrap(), i * 100);
        }
    }

    #[test]
    fn test_update_value_via_get() {
        let mut cache = LRU::new(3);
        cache.put(1, 100);
        cache.put(2, 200);
        cache.put(3, 300);

        // Get and verify value
        assert_eq!(*cache.get(1).unwrap(), 100);

        // After accessing 1, add new item
        cache.put(4, 400);

        // Key 2 should be evicted (not 1, which was just accessed)
        assert!(cache.get(2).is_none());
        assert_eq!(*cache.get(1).unwrap(), 100);
    }

    #[test]
    fn test_string_keys() {
        let mut cache: LRU<&str, i32> = LRU::new(3);
        cache.put("one", 1);
        cache.put("two", 2);
        cache.put("three", 3);

        assert_eq!(*cache.get("one").unwrap(), 1);
        assert_eq!(*cache.get("two").unwrap(), 2);
        assert_eq!(*cache.get("three").unwrap(), 3);

        cache.put("four", 4);
        assert!(cache.get("one").is_none());
    }

    #[test]
    fn test_linked_list_integrity_after_operations() {
        let mut cache = LRU::new(3);
        cache.put(1, 100);
        cache.put(2, 200);
        cache.put(3, 300);

        // Verify linked list length matches cache length
        assert_eq!(cache.ll.len, 3);

        cache.remove(2);
        assert_eq!(cache.ll.len, 2);

        cache.put(4, 400);
        assert_eq!(cache.ll.len, 3);

        cache.put(5, 500); // Eviction
        assert_eq!(cache.ll.len, 3);
    }

    #[test]
    fn test_overwrite_existing_key() {
        let mut cache = LRU::new(3);
        cache.put(1, 100);
        cache.put(2, 200);
        cache.put(3, 300);

        // Overwrite key 1 with new value
        cache.put(1, 999);

        assert_eq!(cache.len, 3);
        assert_eq!(*cache.get(1).unwrap(), 999);
    }
}
