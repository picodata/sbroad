use crate::errors::QueryPlannerError;
use crate::executor::QueryCache;
use std::collections::{hash_map::Entry, HashMap};

pub const DEFAULT_CAPACITY: usize = 50;

#[derive(Clone, Debug)]
struct LRUNode<Key, Value>
where
    Value: Default,
{
    /// The value of the node.
    value: Value,
    /// Next node key in a hash map.
    next: Option<Key>,
    /// Previous node key in a hash map.
    prev: Option<Key>,
}

impl<Key, Value> LRUNode<Key, Value>
where
    Value: Default,
{
    fn new(value: Value) -> Self {
        LRUNode {
            value,
            next: None,
            prev: None,
        }
    }

    fn sentinel() -> Self {
        LRUNode::new(Value::default())
    }

    fn replace_next(&mut self, next: Option<Key>) {
        self.next = next;
    }

    fn replace_prev(&mut self, prev: Option<Key>) {
        self.prev = prev;
    }
}

#[derive(Clone, Debug)]
pub struct LRUCache<Key, Value>
where
    Value: Default,
{
    /// The capacity of the cache.
    capacity: usize,
    /// Actual amount of nodes in the cache.
    size: usize,
    /// `None` key is reserved for the LRU sentinel head.
    map: HashMap<Option<Key>, LRUNode<Key, Value>>,
}

impl<Key, Value> LRUCache<Key, Value>
where
    Value: Default,
    Key: Clone + Eq + std::hash::Hash + std::fmt::Debug,
{
    fn get_node_or_none(&self, key: &Option<Key>) -> Option<&LRUNode<Key, Value>> {
        self.map.get(key)
    }

    fn get_node(&self, key: &Option<Key>) -> Result<&LRUNode<Key, Value>, QueryPlannerError> {
        self.map.get(key).ok_or_else(|| {
            QueryPlannerError::CustomError(format!("LRU node with key {:?} not found", key))
        })
    }

    fn get_node_mut(
        &mut self,
        key: &Option<Key>,
    ) -> Result<&mut LRUNode<Key, Value>, QueryPlannerError> {
        self.map.get_mut(key).ok_or_else(|| {
            QueryPlannerError::CustomError(format!("Mutable LRU node with key {:?} not found", key))
        })
    }

    fn add_first(&mut self, key: Key, value: Value) -> Result<(), QueryPlannerError> {
        let new_node = LRUNode::new(value);
        self.map.insert(Some(key.clone()), new_node);
        self.size += 1;
        let head_node = self.get_node(&None)?;
        let head_next_id = head_node.next.clone();
        self.link_node(key, &None, &head_next_id)?;
        Ok(())
    }

    fn make_first(&mut self, key: &Key) -> Result<(), QueryPlannerError> {
        self.unlink_node(&Some(key.clone()))?;
        let head_node = self.get_node(&None)?;
        let head_next_id = head_node.next.clone();
        self.link_node(key.clone(), &None, &head_next_id)?;
        Ok(())
    }

    fn is_first(&self, key: &Key) -> Result<bool, QueryPlannerError> {
        let head_node = self.get_node(&None)?;
        Ok(head_node.next == Some(key.clone()))
    }

    fn link_node(
        &mut self,
        key: Key,
        prev: &Option<Key>,
        next: &Option<Key>,
    ) -> Result<(), QueryPlannerError> {
        let node = self.get_node_mut(&Some(key.clone()))?;
        node.replace_prev(prev.clone());
        node.replace_next(next.clone());
        let prev_node = self.get_node_mut(prev)?;
        prev_node.replace_next(Some(key.clone()));
        let next_node = self.get_node_mut(next)?;
        next_node.replace_prev(Some(key));
        Ok(())
    }

    fn unlink_node(&mut self, key: &Option<Key>) -> Result<(), QueryPlannerError> {
        // We don't want to remove sentinel.
        if key.is_none() {
            return Ok(());
        }

        let node = self.get_node_mut(key)?;
        let prev_id = node.prev.take();
        let next_id = node.next.take();
        let prev_node = self.get_node_mut(&prev_id)?;
        prev_node.replace_next(next_id.clone());
        let next_node = self.get_node_mut(&next_id)?;
        next_node.replace_prev(prev_id);
        Ok(())
    }

    fn remove_last(&mut self) -> Result<(), QueryPlannerError> {
        let head_node = self.get_node(&None)?;
        let head_prev_id = head_node.prev.clone();
        if head_prev_id.is_none() {
            return Ok(());
        }
        self.unlink_node(&head_prev_id.clone())?;
        if self.map.remove(&head_prev_id).is_some() {
            self.size -= 1;
        }
        Ok(())
    }
}

impl<Key, Value> QueryCache<Key, Value> for LRUCache<Key, Value>
where
    Value: Default + Clone,
    Key: Clone + Eq + std::hash::Hash + std::fmt::Debug,
{
    fn new(capacity: usize) -> Result<Self, QueryPlannerError> {
        if capacity == 0 {
            return Err(QueryPlannerError::CustomError(
                "LRU cache capacity must be greater than zero".to_string(),
            ));
        }
        let head = LRUNode::sentinel();
        let mut map: HashMap<Option<Key>, LRUNode<Key, Value>> =
            HashMap::with_capacity((capacity + 2) as usize);
        map.insert(None, head);

        Ok(LRUCache {
            capacity: capacity as usize,
            size: 0,
            map,
        })
    }

    fn get(&mut self, key: &Key) -> Result<Option<Value>, QueryPlannerError> {
        let value = if let Some(node) = self.get_node_or_none(&Some(key.clone())) {
            node.value.clone()
        } else {
            return Ok(None);
        };

        if self.is_first(key)? {
            return Ok(Some(value));
        }

        self.make_first(key)?;
        Ok(Some(value))
    }

    fn put(&mut self, key: Key, value: Value) -> Result<(), QueryPlannerError> {
        if let Entry::Occupied(mut entry) = self.map.entry(Some(key.clone())) {
            let node = entry.get_mut();
            node.value = value;
            self.make_first(&key)?;
            return Ok(());
        }

        self.add_first(key, value)?;
        if self.size > self.capacity {
            self.remove_last()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
