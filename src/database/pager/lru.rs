use std::{collections::HashMap, fmt::Display, ptr::null_mut, sync::Arc};

use crate::database::types::Pointer;

pub(crate) struct LRU<K, V>
where
    K: Eq + std::hash::Hash + Copy,
{
    map: HashMap<K, Box<Node<K, V>>>,
    ll: LinkedList<K, V>,
    len: usize,
    cap: usize,
}

impl<K, V> LRU<K, V>
where
    K: Eq + std::hash::Hash + Copy + Display,
{
    pub fn new(cap: usize) -> Self {
        LRU {
            map: HashMap::new(),
            ll: LinkedList::new(),
            len: 0,
            cap,
        }
    }

    pub fn insert(&mut self, key: K, val: V) {
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

    pub fn iter(&self) -> LRUIter<'_, K, V> {
        LRUIter {
            lru: self,
            ptr: self.ll.head,
        }
    }

    pub fn clear(&mut self) {
        self.map.clear();
        self.ll = LinkedList::new();
        self.len = 0;
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

pub fn debug_print(lru: &LRU<Pointer, Arc<crate::database::types::Node>>) {
    #[cfg(test)]
    {
        if let Ok("debug") = std::env::var("RUSQL_LOG_PAGER").as_deref() {
            use tracing::debug;

            debug!(buf_len = lru.len, "current LRU buffer:");
            debug!("{:-<10}", "-");
            for e in lru.iter() {
                debug!("{:<10}, {:<10},", e.0, e.1.get_type())
            }
            debug!("{:-<10}", "-");
        }
    }
}

struct LinkedList<K, V>
where
    K: Eq + std::hash::Hash + Copy,
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
}

pub(crate) struct LRUIter<'a, K, V>
where
    K: Eq + std::hash::Hash + Copy,
{
    lru: &'a LRU<K, V>,
    ptr: *mut Node<K, V>,
}

impl<'a, K, V> Iterator for LRUIter<'a, K, V>
where
    K: Eq + std::hash::Hash + Copy,
    V: Sized,
{
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            if self.ptr.is_null() {
                return None;
            }
            let res = Some((&(*self.ptr).key, &(*self.ptr).val));
            self.ptr = (*self.ptr).prev;
            res
        }
    }
}

unsafe impl<K, V> Send for LRU<K, V>
where
    K: Eq + std::hash::Hash + Copy + Send + Sync,
    V: Send + Sync,
{
}
unsafe impl<K, V> Sync for LRU<K, V>
where
    K: Eq + std::hash::Hash + Copy + Send + Sync,
    V: Send + Sync,
{
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
        cache.insert(1, 100);
        assert_eq!(cache.len, 1);

        assert_eq!(*cache.get(1).unwrap(), 100);
    }

    #[test]
    fn test_put_multiple_items() {
        let mut cache = LRU::new(3);
        cache.insert(1, 100);
        cache.insert(2, 200);
        cache.insert(3, 300);

        assert_eq!(cache.len, 3);
        assert_eq!(*cache.get(1).unwrap(), 100);
        assert_eq!(*cache.get(2).unwrap(), 200);
        assert_eq!(*cache.get(3).unwrap(), 300);
    }

    #[test]
    fn test_put_exceeds_capacity() {
        let mut cache = LRU::new(3);
        cache.insert(1, 100);
        cache.insert(2, 200);
        cache.insert(3, 300);
        cache.insert(4, 400);

        assert_eq!(cache.len, 3);
        assert!(cache.get(1).is_none()); // 1 should be evicted
        assert_eq!(*cache.get(2).unwrap(), 200);
        assert_eq!(*cache.get(3).unwrap(), 300);
        assert_eq!(*cache.get(4).unwrap(), 400);
    }

    #[test]
    fn test_get_nonexistent_key() {
        let mut cache = LRU::new(3);
        cache.insert(1, 100);
        assert!(cache.get(999).is_none());
    }

    #[test]
    fn test_get_updates_recency() {
        let mut cache = LRU::new(3);
        cache.insert(1, 100);
        cache.insert(2, 200);
        cache.insert(3, 300);

        // Access key 1, making it most recently used
        cache.get(1);

        // Add key 4, which should evict key 2 (least recently used)
        cache.insert(4, 400);

        assert_eq!(*cache.get(1).unwrap(), 100);
        assert!(cache.get(2).is_none()); // 2 should be evicted
        assert_eq!(*cache.get(3).unwrap(), 300);
        assert_eq!(*cache.get(4).unwrap(), 400);
    }

    #[test]
    fn test_eviction_order() {
        let mut cache = LRU::new(2);
        cache.insert(1, 100);
        cache.insert(2, 200);
        cache.insert(3, 300); // Should evict 1

        assert!(cache.get(1).is_none());
        assert_eq!(*cache.get(2).unwrap(), 200); // This moves 2 to MRU

        cache.insert(4, 400); // Should evict 3 (not 2, since we just accessed 2)
        assert!(cache.get(3).is_none()); // 3 is evicted
        assert_eq!(*cache.get(2).unwrap(), 200); // 2 still exists
        assert_eq!(*cache.get(4).unwrap(), 400);
    }

    #[test]
    fn test_remove_existing_key() {
        let mut cache = LRU::new(3);
        cache.insert(1, 100);
        cache.insert(2, 200);

        let removed = cache.remove(1);
        assert_eq!(removed, Some(100));
        assert_eq!(cache.len, 1);
        assert!(cache.get(1).is_none());
    }

    #[test]
    fn test_remove_nonexistent_key() {
        let mut cache = LRU::new(3);
        cache.insert(1, 100);

        let removed = cache.remove(999);
        assert!(removed.is_none());
        assert_eq!(cache.len, 1);
    }

    #[test]
    fn test_remove_from_middle() {
        let mut cache = LRU::new(3);
        cache.insert(1, 100);
        cache.insert(2, 200);
        cache.insert(3, 300);

        assert_eq!(cache.remove(2), Some(200)); // Remove middle element
        assert_eq!(cache.len, 2);
        assert_eq!(*cache.get(1).unwrap(), 100);
        assert!(cache.get(2).is_none());
        assert_eq!(*cache.get(3).unwrap(), 300);
    }

    #[test]
    fn test_remove_head() {
        let mut cache = LRU::new(3);
        cache.insert(1, 100);
        cache.insert(2, 200);
        cache.insert(3, 300);

        assert_eq!(cache.remove(3), Some(300)); // Remove head (most recent)
        assert_eq!(cache.len, 2);
        assert_eq!(*cache.get(1).unwrap(), 100);
        assert_eq!(*cache.get(2).unwrap(), 200);
        assert!(cache.get(3).is_none());
    }

    #[test]
    fn test_remove_tail() {
        let mut cache = LRU::new(3);
        cache.insert(1, 100);
        cache.insert(2, 200);
        cache.insert(3, 300);

        assert_eq!(cache.remove(1), Some(100)); // Remove tail (least recent)
        assert_eq!(cache.len, 2);
        assert!(cache.get(1).is_none());
        assert_eq!(*cache.get(2).unwrap(), 200);
        assert_eq!(*cache.get(3).unwrap(), 300);
    }

    #[test]
    fn test_remove_only_element() {
        let mut cache = LRU::new(3);
        cache.insert(1, 100);

        assert_eq!(cache.remove(1), Some(100));
        assert_eq!(cache.len, 0);
        assert!(cache.get(1).is_none());
    }

    #[test]
    fn test_capacity_one() {
        let mut cache = LRU::new(1);
        cache.insert(1, 100);
        assert_eq!(*cache.get(1).unwrap(), 100);

        cache.insert(2, 200);
        assert!(cache.get(1).is_none());
        assert_eq!(*cache.get(2).unwrap(), 200);
    }

    #[test]
    fn test_repeated_access_pattern() {
        let mut cache = LRU::new(3);
        cache.insert(1, 100);
        cache.insert(2, 200);
        cache.insert(3, 300);

        // Access pattern: 1, 1, 2, 1
        cache.get(1);
        cache.get(1);
        cache.get(2);
        cache.get(1);

        // Key 3 is least recently used, should be evicted
        cache.insert(4, 400);

        assert_eq!(*cache.get(1).unwrap(), 100);
        assert_eq!(*cache.get(2).unwrap(), 200);
        assert!(cache.get(3).is_none());
        assert_eq!(*cache.get(4).unwrap(), 400);
    }

    #[test]
    fn test_alternating_access() {
        let mut cache = LRU::new(2);
        cache.insert(1, 100);
        cache.insert(2, 200);

        cache.get(1);
        cache.insert(3, 300); // Evicts 2
        assert!(cache.get(2).is_none());

        cache.get(1);
        cache.insert(4, 400); // Evicts 3
        assert!(cache.get(3).is_none());

        assert_eq!(*cache.get(1).unwrap(), 100);
        assert_eq!(*cache.get(4).unwrap(), 400);
    }

    #[test]
    fn test_fill_and_drain() {
        let mut cache = LRU::new(3);
        cache.insert(1, 100);
        cache.insert(2, 200);
        cache.insert(3, 300);

        cache.remove(1);
        cache.remove(2);
        cache.remove(3);

        assert_eq!(cache.len, 0);

        // Refill after draining
        cache.insert(4, 400);
        cache.insert(5, 500);

        assert_eq!(cache.len, 2);
        assert_eq!(*cache.get(4).unwrap(), 400);
        assert_eq!(*cache.get(5).unwrap(), 500);
    }

    #[test]
    fn test_stress_eviction() {
        let mut cache = LRU::new(5);

        // Add 10 items to a cache of size 5
        for i in 0..10 {
            cache.insert(i, i * 100);
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
        cache.insert(1, 100);
        cache.insert(2, 200);
        cache.insert(3, 300);

        // Get and verify value
        assert_eq!(*cache.get(1).unwrap(), 100);

        // After accessing 1, add new item
        cache.insert(4, 400);

        // Key 2 should be evicted (not 1, which was just accessed)
        assert!(cache.get(2).is_none());
        assert_eq!(*cache.get(1).unwrap(), 100);
    }

    #[test]
    fn test_string_keys() {
        let mut cache: LRU<&str, i32> = LRU::new(3);
        cache.insert("one", 1);
        cache.insert("two", 2);
        cache.insert("three", 3);

        assert_eq!(*cache.get("one").unwrap(), 1);
        assert_eq!(*cache.get("two").unwrap(), 2);
        assert_eq!(*cache.get("three").unwrap(), 3);

        cache.insert("four", 4);
        assert!(cache.get("one").is_none());
    }

    #[test]
    fn test_linked_list_integrity_after_operations() {
        let mut cache = LRU::new(3);
        cache.insert(1, 100);
        cache.insert(2, 200);
        cache.insert(3, 300);

        // Verify linked list length matches cache length
        assert_eq!(cache.ll.len, 3);

        cache.remove(2);
        assert_eq!(cache.ll.len, 2);

        cache.insert(4, 400);
        assert_eq!(cache.ll.len, 3);

        cache.insert(5, 500); // Eviction
        assert_eq!(cache.ll.len, 3);
    }

    #[test]
    fn test_overwrite_existing_key() {
        let mut cache = LRU::new(3);
        cache.insert(1, 100);
        cache.insert(2, 200);
        cache.insert(3, 300);

        // Overwrite key 1 with new value
        cache.insert(1, 999);

        assert_eq!(cache.len, 3);
        assert_eq!(*cache.get(1).unwrap(), 999);
    }

    #[test]
    fn test_iter_empty_cache() {
        let cache: LRU<i32, i32> = LRU::new(3);
        let items: Vec<_> = cache.iter().collect();
        assert_eq!(items.len(), 0);
    }

    #[test]
    fn test_iter_single_item() {
        let mut cache = LRU::new(3);
        cache.insert(1, 100);

        let items: Vec<_> = cache.iter().collect();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0], (&1, &100));
    }

    #[test]
    fn test_iter_multiple_items() {
        let mut cache = LRU::new(3);
        cache.insert(1, 100);
        cache.insert(2, 200);
        cache.insert(3, 300);

        let items: Vec<_> = cache.iter().collect();
        assert_eq!(items.len(), 3);
        // Iterator goes from head (LRU) to tail (MRU)
        assert_eq!(items[0], (&1, &100)); // Least recent
        assert_eq!(items[1], (&2, &200));
        assert_eq!(items[2], (&3, &300)); // Most recent
    }

    #[test]
    fn test_iter_order_matches_recency() {
        let mut cache = LRU::new(3);
        cache.insert(1, 100);
        cache.insert(2, 200);
        cache.insert(3, 300);

        // Access 1, making it most recent
        cache.get(1);

        let items: Vec<_> = cache.iter().collect();
        // New order: 2 (LRU), 3, 1 (MRU)
        assert_eq!(items[0], (&2, &200));
        assert_eq!(items[1], (&3, &300));
        assert_eq!(items[2], (&1, &100));
    }

    #[test]
    fn test_iter_after_eviction() {
        let mut cache = LRU::new(2);
        cache.insert(1, 100);
        cache.insert(2, 200);
        cache.insert(3, 300); // Evicts 1

        let items: Vec<_> = cache.iter().collect();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0], (&2, &200)); // LRU
        assert_eq!(items[1], (&3, &300)); // MRU
    }

    #[test]
    fn test_iter_after_removal() {
        let mut cache = LRU::new(3);
        cache.insert(1, 100);
        cache.insert(2, 200);
        cache.insert(3, 300);

        cache.remove(2);

        let items: Vec<_> = cache.iter().collect();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0], (&1, &100));
        assert_eq!(items[1], (&3, &300));
    }

    #[test]
    fn test_iter_after_update() {
        let mut cache = LRU::new(3);
        cache.insert(1, 100);
        cache.insert(2, 200);
        cache.insert(3, 300);

        // Update value of key 1
        cache.insert(1, 999);

        let items: Vec<_> = cache.iter().collect();
        // Key 1 should now be most recent with updated value
        // Order: 2 (LRU), 3, 1 (MRU)
        assert_eq!(items[0], (&2, &200));
        assert_eq!(items[1], (&3, &300));
        assert_eq!(items[2], (&1, &999));
    }

    #[test]
    fn test_iter_does_not_modify_cache() {
        let mut cache = LRU::new(3);
        cache.insert(1, 100);
        cache.insert(2, 200);
        cache.insert(3, 300);

        // Iterate twice and verify order is the same
        let items1: Vec<_> = cache.iter().collect();
        let items2: Vec<_> = cache.iter().collect();

        assert_eq!(items1, items2);
        assert_eq!(cache.len, 3);
    }

    #[test]
    fn test_iter_with_complex_access_pattern() {
        let mut cache = LRU::new(4);
        cache.insert(1, 100);
        cache.insert(2, 200);
        cache.insert(3, 300);
        cache.insert(4, 400);

        // Access pattern: 2, 1, 3
        cache.get(2);
        cache.get(1);
        cache.get(3);

        let items: Vec<_> = cache.iter().collect();
        // Order should be: 4 (LRU), 2, 1, 3 (MRU)
        assert_eq!(items[0], (&4, &400));
        assert_eq!(items[1], (&2, &200));
        assert_eq!(items[2], (&1, &100));
        assert_eq!(items[3], (&3, &300));
    }

    #[test]
    fn test_iter_multiple_times() {
        let mut cache = LRU::new(3);
        cache.insert(1, 100);
        cache.insert(2, 200);

        let _items1: Vec<_> = cache.iter().collect();
        let _items2: Vec<_> = cache.iter().collect();
        let items3: Vec<_> = cache.iter().collect();

        // Should still work after multiple iterations
        assert_eq!(items3.len(), 2);
        assert_eq!(items3[0], (&1, &100));
        assert_eq!(items3[1], (&2, &200));
    }

    #[test]
    fn test_iter_partial_consumption() {
        let mut cache = LRU::new(3);
        cache.insert(1, 100);
        cache.insert(2, 200);
        cache.insert(3, 300);

        let mut iter = cache.iter();
        assert_eq!(iter.next(), Some((&1, &100)));
        assert_eq!(iter.next(), Some((&2, &200)));
        // Don't consume the last item
        drop(iter);

        // Cache should still be intact
        assert_eq!(cache.len, 3);
    }

    #[test]
    fn test_iter_with_string_keys() {
        let mut cache: LRU<&str, i32> = LRU::new(3);
        cache.insert("apple", 1);
        cache.insert("banana", 2);
        cache.insert("cherry", 3);

        let items: Vec<_> = cache.iter().collect();
        assert_eq!(items.len(), 3);
        assert_eq!(items[0], (&"apple", &1));
        assert_eq!(items[1], (&"banana", &2));
        assert_eq!(items[2], (&"cherry", &3));
    }

    #[test]
    fn test_iter_find_key() {
        let mut cache = LRU::new(5);
        for i in 0..5 {
            cache.insert(i, i * 100);
        }

        let found = cache.iter().find(|(k, _)| **k == 3);
        assert_eq!(found, Some((&3, &300)));
    }

    #[test]
    fn test_iter_filter_values() {
        let mut cache = LRU::new(5);
        cache.insert(1, 100);
        cache.insert(2, 250);
        cache.insert(3, 300);
        cache.insert(4, 150);

        let high_values: Vec<_> = cache.iter().filter(|(_, v)| **v >= 200).collect();

        assert_eq!(high_values.len(), 2);
    }

    #[test]
    fn test_iter_map_values() {
        let mut cache = LRU::new(3);
        cache.insert(1, 100);
        cache.insert(2, 200);
        cache.insert(3, 300);

        let doubled: Vec<_> = cache.iter().map(|(k, v)| (*k, *v * 2)).collect();

        assert_eq!(doubled[0], (1, 200));
        assert_eq!(doubled[1], (2, 400));
        assert_eq!(doubled[2], (3, 600));
    }

    #[test]
    fn test_iter_count() {
        let mut cache = LRU::new(10);
        for i in 0..7 {
            cache.insert(i, i);
        }

        assert_eq!(cache.iter().count(), 7);
    }

    #[test]
    fn test_iter_after_capacity_one() {
        let mut cache = LRU::new(1);
        cache.insert(1, 100);
        cache.insert(2, 200); // Evicts 1

        let items: Vec<_> = cache.iter().collect();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0], (&2, &200));
    }
}
