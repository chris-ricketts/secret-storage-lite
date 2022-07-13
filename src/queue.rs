use cosmwasm_std::{StdResult, Storage};

use crate::Map;

pub struct Queue<'a, T> {
    capacity: u32,
    map: Map<'a, u32, T>,
}

impl<'a, T> Queue<'a, T> {
    const HEAD: &'static [u8] = b"_head";
    const TAIL: &'static [u8] = b"_tail";

    // Creates a Queue with capacity `u32::MAX - 1'
    pub const fn new(namespace: &'a str) -> Self {
        Self::with_capacity(namespace, u32::MAX - 1)
    }

    // Creates a Queue with a number of slots equal to `capacity`
    // panics if requested capacity is zero or `u32::MAX`
    pub const fn with_capacity(namespace: &'a str, capacity: u32) -> Self {
        if capacity == 0 {
            panic!("zero sized queues are illegal");
        }

        if capacity == u32::MAX {
            panic!("the maximum legal capacity is u32::MAX - 1");
        }

        let map = Map::new(namespace);
        Self {
            capacity: capacity + 1,
            map,
        }
    }

    pub fn namespace(&self) -> &'a [u8] {
        self.map.namespace()
    }

    pub fn max_capacity(&self) -> u32 {
        self.capacity - 1
    }

    pub fn len(&self, store: &dyn Storage) -> u32 {
        let tail = self.tail(store);
        let head = self.head(store);
        self.determine_len(head, tail)
    }

    pub fn free_capacity(&self, store: &dyn Storage) -> u32 {
        self.max_capacity() - self.len(store)
    }

    pub fn is_full(&self, store: &dyn Storage) -> bool {
        let tail = self.tail(store);
        let head = self.head(store);
        self.determine_is_full(head, tail)
    }

    fn determine_is_full(&self, head: u32, tail: u32) -> bool {
        self.determine_len(head, tail) == self.max_capacity()
    }

    fn determine_len(&self, head: u32, tail: u32) -> u32 {
        if tail >= head {
            tail.abs_diff(head)
        } else {
            self.capacity - head + tail
        }
    }

    fn head(&self, store: &dyn Storage) -> u32 {
        self.with_namespace_suffix(Self::HEAD, |ns| load_u32(store, ns))
    }

    fn inc_head(&self, store: &mut dyn Storage, head: u32) {
        let head = (head + 1) % self.capacity;
        self.with_namespace_suffix(Self::HEAD, |ns| save_u32(store, ns, head))
    }

    fn tail(&self, store: &dyn Storage) -> u32 {
        self.with_namespace_suffix(Self::TAIL, |ns| load_u32(store, ns))
    }

    fn inc_tail(&self, store: &mut dyn Storage, tail: u32) {
        let tail = (tail + 1) % self.capacity;
        self.with_namespace_suffix(Self::TAIL, |ns| save_u32(store, ns, tail))
    }

    fn with_namespace_suffix<R, F: FnOnce(&[u8]) -> R>(&self, namespace: &[u8], f: F) -> R {
        let namespace = &[self.namespace(), namespace].concat();
        f(namespace)
    }
}

impl<'a, T> Queue<'a, T>
where
    T: serde::Serialize + serde::de::DeserializeOwned,
{
    /// Add an item to the back of the queue, returns true if the item is added or false if the queue is full
    pub fn push_back(&self, store: &mut dyn Storage, t: &T) -> StdResult<bool> {
        let tail = self.tail(store);
        let head = self.head(store);

        if self.determine_is_full(head, tail) {
            return Ok(false);
        }

        self.map.save(store, tail.into(), t)?;

        self.inc_tail(store, tail);

        Ok(true)
    }

    /// Pop an item from the front of the queue, returns None if the queue is empty
    pub fn pop_front(&self, store: &mut dyn Storage) -> StdResult<Option<T>> {
        let tail = self.tail(store);
        let head = self.head(store);

        if tail == head {
            return Ok(None);
        }

        let popped = self.map.may_load(store, head.into())?;

        self.inc_head(store, head);

        Ok(popped)
    }
}

fn load_u32(store: &dyn Storage, namespace: &[u8]) -> u32 {
    store
        .get(namespace)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_be_bytes)
        .unwrap_or_default()
}

fn save_u32(store: &mut dyn Storage, namespace: &[u8], u: u32) {
    store.set(namespace, &u.to_be_bytes())
}

#[cfg(test)]
mod test {
    use std::collections::VecDeque;

    use cosmwasm_std::testing::MockStorage;
    use proptest::prelude::*;

    use super::*;

    #[derive(Debug, Clone, Copy, proptest_derive::Arbitrary)]
    enum Op {
        Push(u8),
        Pop,
    }

    struct Model {
        max: usize,
        q: VecDeque<u8>,
    }

    impl Model {
        fn new(max: usize) -> Self {
            Self {
                max,
                q: VecDeque::default(),
            }
        }

        fn push(&mut self, i: u8) -> bool {
            if self.q.len() == self.max {
                return false;
            }
            self.q.push_back(i);
            true
        }

        fn pop(&mut self) -> Option<u8> {
            self.q.pop_front()
        }

        fn len(&self) -> usize {
            self.q.len()
        }

        fn free_capacity(&self) -> usize {
            self.max - self.q.len()
        }

        fn is_full(&self) -> bool {
            self.q.len() == self.max
        }
    }

    fn setup_queue<T>(size: u32) -> (Queue<'static, T>, MockStorage) {
        let q = Queue::with_capacity("test", size);
        let store = cosmwasm_std::testing::MockStorage::new();
        (q, store)
    }

    proptest! {
        #[test]
        fn impl_matches_model(size in 1u32..1000u32, ops: Vec<Op>) {
            let mut model = Model::new(size as _);
            let (queue, mut store) = setup_queue(size);
            for op in ops {
                match op {
                    Op::Push(u) => {
                        let model_res = model.push(u);
                        let impl_res = queue.push_back(&mut store, &u).unwrap();
                        prop_assert_eq!(model_res, impl_res, "push results differ");
                    }
                    Op::Pop => {
                        let model_res = model.pop();
                        let impl_res = queue.pop_front(&mut store).unwrap();
                        prop_assert_eq!(model_res, impl_res, "pop results differ");
                    }
                }

                prop_assert_eq!(queue.len(&store), model.len() as u32, "len results differ");
                prop_assert_eq!(queue.free_capacity(&store), model.free_capacity() as u32, "free_capacity results differ");
                prop_assert_eq!(queue.is_full(&store), model.is_full(), "is_full results differ");
            }
        }
    }

    #[test]
    fn invariant_max_capacity_queue_wraps_around() {
        let queue = Queue::new("test");
        let mut store = MockStorage::new();
        save_u32(&mut store, b"test_tail", u32::MAX - 1);
        save_u32(&mut store, b"test_head", u32::MAX - 1);
        assert_eq!(queue.len(&store), 0);
        assert!(queue.push_back(&mut store, &0u8).unwrap());
        assert!(queue.push_back(&mut store, &0u8).unwrap());
        assert_eq!(queue.len(&store), 2);
        assert_eq!(queue.tail(&store), 1);
        assert!(queue.pop_front(&mut store).unwrap().is_some());
        assert!(queue.pop_front(&mut store).unwrap().is_some());
        assert_eq!(queue.head(&store), 1);
        assert_eq!(queue.len(&store), 0);
    }

    #[test]
    fn invariant_push_non_full_changes_tail() {
        let (queue, mut store) = setup_queue(1);
        let pre_tail = queue.tail(&store);
        assert!(queue.push_back(&mut store, &0u8).unwrap());
        let post_tail = queue.tail(&store);
        assert_ne!(pre_tail, post_tail);
    }

    #[test]
    fn invariant_pop_non_empty_changes_head() {
        let (queue, mut store) = setup_queue(1);
        assert!(queue.push_back(&mut store, &0u8).unwrap());
        let pre_head = queue.head(&store);
        assert!(queue.pop_front(&mut store).unwrap().is_some());
        let post_head = queue.head(&store);
        assert_ne!(pre_head, post_head);
    }

    #[test]
    fn invariant_tail_and_head_wrap_around() {
        let (queue, mut store) = setup_queue(1);
        assert!(queue.push_back(&mut store, &0u8).unwrap());
        assert!(queue.pop_front(&mut store).unwrap().is_some());
        assert!(queue.push_back(&mut store, &0u8).unwrap());
        assert!(queue.pop_front(&mut store).unwrap().is_some());
        assert_eq!(queue.tail(&store), 0);
        assert_eq!(queue.head(&store), 0);
    }

    #[test]
    fn invariant_cannot_push_onto_full_queue() {
        let (queue, mut store) = setup_queue(2);
        assert!(queue.push_back(&mut store, &0u8).unwrap());
        assert!(queue.push_back(&mut store, &0u8).unwrap());
        assert_eq!(queue.push_back(&mut store, &0u8), Ok(false));
    }

    #[test]
    fn invariant_cannot_pop_off_empty_queue() {
        let (queue, mut store) = setup_queue(2);
        assert!(queue.push_back(&mut store, &0u8).unwrap());
        assert!(queue.push_back(&mut store, &0u8).unwrap());
        assert!(queue.pop_front(&mut store).unwrap().is_some());
        assert!(queue.pop_front(&mut store).unwrap().is_some());
        assert!(queue.pop_front(&mut store).unwrap().is_none());
    }
}
