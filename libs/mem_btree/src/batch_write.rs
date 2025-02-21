use std::{collections::BTreeMap, ops::Add, time::Duration};

use crate::now;

#[derive(Debug)]
pub enum Action<V> {
    Put(V, Option<Duration>),
    Delete,
}
impl<V> Action<V> {
    pub(crate) fn value(self) -> (V, Option<Duration>) {
        match self {
            Self::Put(v, t) => (v, t),
            Self::Delete => unreachable!(),
        }
    }

    pub fn mut_value(&mut self) -> &mut V {
        match self {
            Self::Put(v, _) => v,
            Self::Delete => unreachable!(),
        }
    }
}

#[derive(Debug, Default)]
pub struct BatchWrite<K, V> {
    inner: BTreeMap<K, Action<V>>,
}

impl<K, V> From<BTreeMap<K, Action<V>>> for BatchWrite<K, V> {
    fn from(inner: BTreeMap<K, Action<V>>) -> Self {
        Self { inner }
    }
}

impl<K, V> BatchWrite<K, V>
where
    K: Ord,
{
    pub fn put(&mut self, key: K, value: V) {
        self.inner.insert(key, Action::Put(value, None));
    }

    pub fn put_ttl(&mut self, key: K, value: V, ttl: Duration) {
        self.inner
            .insert(key, Action::Put(value, Some(now().add(ttl))));
    }

    pub fn delete(&mut self, key: K) {
        self.inner.insert(key, Action::Delete);
    }

    pub fn into_map(self) -> BTreeMap<K, Action<V>> {
        self.inner
    }
}
