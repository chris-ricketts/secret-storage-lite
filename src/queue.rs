use cosmwasm_std::{StdResult, Storage};

use crate::Map;

pub struct Queue<'a, T> {
    capacity: u32,
    map: Map<'a, u32, T>,
}

impl<'a, T> Queue<'a, T> {
    // Creates a Queue with capacity `u32::MAX`
    pub const fn new(namespace: &'a str) -> Self {
        Self::with_capacity(namespace, u32::MAX)
    }

    // Creates a Queue with a number of slots equal to `capacity`
    pub const fn with_capacity(namespace: &'a str, capacity: u32) -> Self {
        let map = Map::new(namespace);
        Self { capacity, map }
    }

    pub fn namespace(&self) -> &'a [u8] {
        self.map.namespace()
    }

    pub fn max_capacity(&self) -> u32 {
        self.capacity
    }
}

impl<'a, T> Queue<'a, T>
where
    T: serde::Serialize + serde::de::DeserializeOwned,
{
    const FULL: &'static [u8] = b"_full";
    const HEAD: &'static [u8] = b"_head";
    const TAIL: &'static [u8] = b"_tail";

    pub fn len(&self, store: &dyn Storage) -> u32 {
        let tail = self.tail(store);
        let head = self.head(store);
        tail.distance_from(head)
    }

    pub fn free_capacity(&self, store: &dyn Storage) -> u32 {
        self.max_capacity() - self.len(store)
    }

    pub fn is_full(&self, store: &dyn Storage) -> bool {
        self.with_namespace_suffix(Self::FULL, |ns| load_bool(store, ns))
    }

    /// Add an item to the back of the queue, returns true if the item is added or false if the queue is full
    pub fn push_back(&self, store: &mut dyn Storage, t: &T) -> StdResult<bool> {
        let tail = self.tail(store);
        let head = self.head(store);

        if self.is_full(store) {
            return Ok(false);
        }

        self.map.save(store, tail.into(), t)?;

        let tail = tail + 1;

        if tail.distance_from(head) == self.max_capacity() {
            self.set_is_full(store, true);
        }

        self.set_tail(store, tail);

        Ok(true)
    }

    /// Pop an item from the front of the queue, returns None if the queue is empty
    pub fn pop_front(&self, store: &mut dyn Storage) -> StdResult<Option<T>> {
        let tail = self.tail(store);
        let head = self.head(store);

        if tail == head && !self.is_full(store) {
            return Ok(None);
        }

        let popped = self.map.may_load(store, head.into())?;

        if tail.distance_from(head) == self.max_capacity() {
            self.set_is_full(store, false);
        }

        let head = head + 1;

        self.set_head(store, head);

        Ok(popped)
    }

    fn set_is_full(&self, store: &mut dyn Storage, is_full: bool) {
        self.with_namespace_suffix(Self::FULL, |ns| save_bool(store, ns, is_full))
    }

    fn head(&self, store: &dyn Storage) -> Pointer {
        self.with_namespace_suffix(Self::HEAD, |ns| {
            let i = load_u32(store, ns);
            let max = self.max_capacity();
            Pointer { i, max }
        })
    }

    fn set_head(&self, store: &mut dyn Storage, head: Pointer) {
        self.with_namespace_suffix(Self::HEAD, |ns| save_u32(store, ns, head.into()))
    }

    fn tail(&self, store: &dyn Storage) -> Pointer {
        self.with_namespace_suffix(Self::TAIL, |ns| {
            let i = load_u32(store, ns);
            let max = self.max_capacity();
            Pointer { i, max }
        })
    }

    fn set_tail(&self, store: &mut dyn Storage, tail: Pointer) {
        self.with_namespace_suffix(Self::TAIL, |ns| save_u32(store, ns, tail.into()))
    }

    fn with_namespace_suffix<R, F: FnOnce(&[u8]) -> R>(&self, namespace: &[u8], f: F) -> R {
        let namespace = &[self.namespace(), namespace].concat();
        f(namespace)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct Pointer {
    i: u32,
    max: u32,
}

impl Pointer {
    fn distance_from(self, other: Pointer) -> u32 {
        if self.i >= other.i {
            self.i - other.i
        } else {
            self.i + other.i
        }
    }
}

impl From<Pointer> for u32 {
    fn from(p: Pointer) -> Self {
        p.i
    }
}

impl std::ops::Add<u32> for Pointer {
    type Output = Pointer;

    fn add(self, rhs: u32) -> Self::Output {
        let (mut i, is_of) = self.i.overflowing_add(rhs);

        if !is_of && i > self.max {
            i -= self.max;
        }

        Pointer { i, ..self }
    }
}

impl std::ops::Add for Pointer {
    type Output = Pointer;

    fn add(self, rhs: Self) -> Self::Output {
        self + rhs.i
    }
}

impl std::ops::Sub<u32> for Pointer {
    type Output = Pointer;

    fn sub(mut self, mut rhs: u32) -> Self::Output {
        if rhs <= self.i {
            self.i -= rhs;
            return self;
        }

        if rhs > self.max + 1 {
            rhs = rhs % (self.max + 1);
        }

        let (i, _) = self.i.overflowing_sub(rhs);

        // take into account user specified max value
        let i = i - (u32::MAX - self.max);

        Pointer { i, ..self }
    }
}

impl std::ops::Sub for Pointer {
    type Output = Pointer;

    fn sub(self, rhs: Self) -> Self::Output {
        self - rhs.i
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

fn load_bool(store: &dyn Storage, namespace: &[u8]) -> bool {
    store
        .get(namespace)
        .map(|bytes| bytes[0] == 1)
        .unwrap_or_default()
}

fn save_bool(store: &mut dyn Storage, namespace: &[u8], b: bool) {
    store.set(namespace, if b { &[1] } else { &[0] })
}

#[cfg(test)]
mod test {
    use super::*;

    fn test_is_empty(queue: &Queue<u32>, store: &mut dyn Storage) {
        let tail = queue.tail(store);
        let head = queue.head(store);
        assert_eq!(tail, head);
        assert_eq!(queue.len(store), 0);
        assert_eq!(queue.free_capacity(store), queue.max_capacity());
        assert_eq!(queue.is_full(store), false);
        assert_eq!(queue.pop_front(store), Ok(None));
    }

    fn test_is_full(queue: &Queue<u32>, store: &mut dyn Storage) {
        assert_eq!(queue.len(store), queue.max_capacity());
        assert_eq!(queue.free_capacity(store), 0);
        assert_eq!(queue.is_full(store), true);
        assert_eq!(queue.push_back(store, &0u32), Ok(false));
    }

    fn test_push(queue: &Queue<u32>, store: &mut dyn Storage, i: u32) {
        assert!(queue.push_back(store, &i).unwrap());
        let tail = queue.tail(store);
        let head = queue.head(store);
        let len = i + 1;
        let capacity = queue.max_capacity() - len;
        let is_full = queue.max_capacity() == len;

        assert_eq!(
            queue.len(store),
            len,
            "expected len == {len} after pushing {i}. tail = {tail:?}, head = {head:?}"
        );
        assert_eq!(
            queue.free_capacity(store),
            capacity,
            "expected == {capacity} after pushing {i}. tail = {tail:?}, head = {head:?}"
        );
        assert_eq!(
            queue.is_full(store),
            is_full,
            "expected is_full == {is_full} after pushing {i}. tail = {tail:?}, head = {head:?}, len = {len}, capacity = {capacity}",
        );
    }

    fn test_pop(queue: &Queue<u32>, store: &mut dyn Storage, i: u32) {
        assert_eq!(queue.pop_front(store), Ok(Some(i)));
        let tail = queue.tail(store);
        let head = queue.head(store);
        let len = queue.max_capacity() - i - 1;
        let capacity = i + 1;
        assert_eq!(
            queue.len(store),
            len,
            "expected len == {len} after pop {i}. tail = {tail:?}, head = {head:?}"
        );
        assert_eq!(
            queue.free_capacity(store),
            capacity,
            "expected == {capacity} after pop {i}. tail = {tail:?}, head = {head:?}"
        );
        assert_eq!(queue.is_full(store), false);
    }

    #[test]
    fn smoke_test() {
        const SIZE: u32 = 5;

        let queue = Queue::with_capacity("test", SIZE);

        let mut store = cosmwasm_std::testing::MockStorage::new();

        test_is_empty(&queue, &mut store);

        for i in 0..SIZE {
            test_push(&queue, &mut store, i);
        }

        test_is_full(&queue, &mut store);

        for i in 0..SIZE {
            test_pop(&queue, &mut store, i);
        }

        test_is_empty(&queue, &mut store);
    }
}
