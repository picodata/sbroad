use super::LRUCache;
use crate::errors::QueryPlannerError;
use crate::executor::QueryCache;
use crate::ir::Plan;
use pretty_assertions::assert_eq;

#[test]
fn lru1() {
    let mut cache: LRUCache<usize, Plan> = LRUCache::new(2).unwrap();
    cache.put(1, Plan::default()).unwrap();
    cache.put(2, Plan::default()).unwrap();
    assert_eq!(cache.get(&1).unwrap().is_some(), true);
    cache.put(3, Plan::default()).unwrap();
    assert_eq!(cache.get(&2).unwrap().is_none(), true);
    assert_eq!(cache.get(&3).unwrap().is_some(), true);
    assert_eq!(cache.get(&1).unwrap().is_some(), true);
}

#[test]
fn lru2() {
    let cache_err: Result<LRUCache<usize, Plan>, QueryPlannerError> = LRUCache::new(0);
    assert_eq!(cache_err.is_err(), true);
}
