use std::sync::Arc;

use crate::*;

pub struct Node<K, V> {
    pub key: Option<Item<K, V>>,
    ttl: Option<Duration>,
    length: usize,
    pub children: Vec<N<K, V>>,
}

impl<K, V> Node<K, V>
where
    K: Ord,
{
    pub fn instance(children: Vec<N<K, V>>) -> N<K, V> {
        let key = if children.is_empty() {
            None
        } else {
            children[0].key().cloned()
        };

        let mut ttl = None;

        let mut length = 0;

        for c in children.iter() {
            if let Some(t) = c.ttl() {
                if let Some(t1) = &ttl {
                    if t < t1 {
                        ttl = Some(*t);
                    }
                } else {
                    ttl = Some(*t);
                }
            }

            length += c.len();
        }

        Arc::new(BTreeType::Node(Self {
            key,
            length,
            ttl,
            children,
        }))
    }

    pub fn put(&self, m: usize, k: K, v: V, ttl: Option<Duration>) -> PutResult<K, V> {
        let index = self.search_index(&k);

        let (values, old) = self.children[index].put(m, k, v, ttl);

        let mut children = Vec::with_capacity(self.children.len() + values.len());

        children.extend(self.children[..index].iter().cloned());
        children.extend(values);
        children.extend(self.children[index + 1..].iter().cloned());

        if children.len() < m {
            return (vec![Self::instance(children)], old);
        }

        let mid = m / 2;

        let left = children[..mid].to_vec();
        let right = children[mid..].to_vec();

        (vec![Self::instance(left), Self::instance(right)], old)
    }

    pub fn get<Q>(&self, k: &Q) -> Option<&V>
    where
        K: Borrow<Q> + Ord,
        Q: ?Sized + Ord,
    {
        self.children[self.search_index(k)].get(k)
    }

    pub fn remove<Q>(&self, k: &Q) -> Option<(N<K, V>, Item<K, V>)>
    where
        K: Borrow<Q> + Ord,
        Q: ?Sized + Ord,
    {
        let index = self.search_index(k);

        let (child, item) = self.children[index].remove(k)?;

        let mut children = Vec::with_capacity(self.children.len() - 1);
        children.extend(self.children[..index].iter().cloned());
        if child.len() > 0 {
            children.push(child);
        }
        children.extend(self.children[index + 1..].iter().cloned());

        Some((Self::instance(children), item))
    }

    pub fn expir(&self) -> Option<N<K, V>> {
        let now = now();
        match self.ttl {
            Some(t) if t < now => {
                let children = self
                    .children
                    .iter()
                    .filter_map(|c| {
                        let c = c.expir();
                        if c.len() > 0 {
                            Some(c)
                        } else {
                            None
                        }
                    })
                    .collect();
                Some(Self::instance(children))
            }
            _ => None,
        }
    }

    pub fn write(&self, m: usize, mut actions: BTreeMap<K, Action<V>>) -> Vec<N<K, V>> {
        let mut children = Vec::with_capacity(self.children.len() + actions.len());

        let mut start_index = 0;

        loop {
            if let Some((k, _)) = actions.first_key_value() {
                let index = self.search_index(k);

                if start_index < index {
                    children.extend_from_slice(&self.children[start_index..index]);
                }

                if index + 1 < self.children.len() {
                    //get next key for current childs
                    if let Some(k) = self.children[index + 1].key() {
                        let temp = actions.split_off(&k.0);
                        children.extend(self.children[index].write(m, actions));
                        start_index = index + 1;
                        actions = temp;
                    }
                } else {
                    children.extend(self.children[index].write(m, actions));
                    break;
                }
            } else {
                children.extend_from_slice(&self.children[start_index..]);
                break;
            }
        }

        children
            .chunks(m)
            .filter_map(|c| {
                if c.is_empty() {
                    None
                } else {
                    Some(Self::instance(c.to_vec()))
                }
            })
            .collect()
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub fn split_off<Q>(&self, k: &Q) -> (N<K, V>, N<K, V>)
    where
        K: Borrow<Q> + Ord,
        Q: ?Sized + Ord,
    {
        let index = self.search_index(k);

        let (l, r) = self.children[index].split_off(k);

        let mut left = Vec::with_capacity(index);
        left.extend_from_slice(&self.children[..index]);
        if l.len() > 0 {
            left.push(l);
        }

        let mut right = Vec::with_capacity(self.children.len() - index);
        if r.len() > 0 {
            right.push(r);
        }
        right.extend_from_slice(&self.children[index + 1..]);

        (Self::instance(left), Self::instance(right))
    }

    pub fn search_index<Q>(&self, k: &Q) -> usize
    where
        K: Borrow<Q> + Ord,
        Q: ?Sized + Ord,
    {
        match self.children.binary_search_by(|c| cmp(c.key(), Some(k))) {
            Ok(i) => i,
            Err(i) => {
                if i == 0 {
                    i
                } else {
                    i - 1
                }
            }
        }
    }

    pub(crate) fn ttl(&self) -> Option<&Duration>
    where
        K: Ord,
    {
        self.ttl.as_ref()
    }
}
