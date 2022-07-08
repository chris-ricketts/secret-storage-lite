use cosmwasm_std::{StdResult, Storage};

use crate::{keys::PrimaryKey, Map};

pub struct Set<'a, T> {
    map: Map<'a, T, ()>,
}

impl<'a, T> Set<'a, T> {
    pub const fn new(namespace: &'a str) -> Self {
        let map = Map::new(namespace);
        Self { map }
    }

    pub fn namespace(&self) -> &'a [u8] {
        self.map.namespace()
    }
}

impl<'a, T> Set<'a, T>
where
    T: PrimaryKey<'a>,
{
    pub fn save(&self, store: &mut dyn Storage, t: T) -> StdResult<()> {
        self.map.save(store, t, &())
    }

    pub fn contains(&self, store: &dyn Storage, t: T) -> bool {
        self.map.has(store, t)
    }

    pub fn remove(&self, store: &mut dyn Storage, t: T) {
        self.map.remove(store, t)
    }
}
