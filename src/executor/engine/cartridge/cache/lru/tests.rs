use super::LRUCache;
use crate::errors::QueryPlannerError;
use crate::executor::QueryCache;
use crate::frontend::sql::ast::AbstractSyntaxTree;
use crate::frontend::Ast;
use pretty_assertions::assert_eq;

#[test]
fn lru1() {
    let mut cache: LRUCache<usize, AbstractSyntaxTree> = LRUCache::new(2).unwrap();
    cache.put(1, AbstractSyntaxTree::empty()).unwrap();
    cache.put(2, AbstractSyntaxTree::empty()).unwrap();
    assert_eq!(cache.get(&1).unwrap().is_some(), true);
    cache.put(3, AbstractSyntaxTree::empty()).unwrap();
    assert_eq!(cache.get(&2).unwrap().is_none(), true);
    assert_eq!(cache.get(&3).unwrap().is_some(), true);
    assert_eq!(cache.get(&1).unwrap().is_some(), true);
}

#[test]
fn lru2() {
    let cache_err: Result<LRUCache<usize, AbstractSyntaxTree>, QueryPlannerError> =
        LRUCache::new(0);
    assert_eq!(cache_err.is_err(), true);
}
