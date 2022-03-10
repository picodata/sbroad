use crate::frontend::sql::ast::ParseNodes;
use std::cell::RefCell;

#[derive(Debug)]
pub struct AstIterator<'n> {
    current: &'n usize,
    child: RefCell<usize>,
    nodes: &'n ParseNodes,
}

impl<'n> Iterator for AstIterator<'n> {
    type Item = &'n usize;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(node) = self.nodes.arena.get(*self.current) {
            let step = *self.child.borrow();
            if step < node.children.len() {
                *self.child.borrow_mut() += 1;
                return node.children.get(step);
            }
            None
        } else {
            None
        }
    }
}

impl<'n> ParseNodes {
    #[allow(dead_code)]
    pub fn ast_iter(&'n self, current: &'n usize) -> AstIterator<'n> {
        AstIterator {
            current,
            child: RefCell::new(0),
            nodes: self,
        }
    }
}
