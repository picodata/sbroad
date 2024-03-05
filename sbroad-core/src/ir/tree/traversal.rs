use std::collections::VecDeque;

pub const EXPR_CAPACITY: usize = 64;
pub const REL_CAPACITY: usize = 32;

/// Pair of (`node_id`, it's tree level).
pub type LevelNode = (usize, usize);

pub struct PostOrder<F, I>
where
    F: FnMut(usize) -> I,
    I: Iterator<Item = usize>,
{
    inner: PostOrderWithFilter<'static, F, I>,
}

impl<F, I> PostOrder<F, I>
where
    F: FnMut(usize) -> I,
    I: Iterator<Item = usize>,
{
    pub fn iter(&mut self, root: usize) -> impl Iterator<Item = LevelNode> {
        self.inner.iter(root)
    }

    pub fn new(iter_children: F, nodes: Vec<LevelNode>) -> Self {
        Self {
            inner: PostOrderWithFilter::new(iter_children, nodes, Box::new(|_| true)),
        }
    }

    pub fn populate_nodes(&mut self, root: usize) {
        self.inner.populate_nodes(root);
    }

    pub fn take_nodes(&mut self) -> Vec<LevelNode> {
        self.inner.take_nodes()
    }

    pub fn with_capacity(iter_children: F, capacity: usize) -> Self {
        Self {
            inner: PostOrderWithFilter::with_capacity(iter_children, capacity, Box::new(|_| true)),
        }
    }
}

pub type FilterFn<'filter> = Box<dyn Fn(usize) -> bool + 'filter>;

pub struct PostOrderWithFilter<'filter, F, I>
where
    F: FnMut(usize) -> I,
    I: Iterator<Item = usize>,
{
    iter_children: F,
    nodes: Vec<LevelNode>,
    filter_fn: FilterFn<'filter>,
}

impl<'filter, F, I> PostOrderWithFilter<'filter, F, I>
where
    F: FnMut(usize) -> I,
    I: Iterator<Item = usize>,
{
    pub fn iter(&mut self, root: usize) -> impl Iterator<Item = LevelNode> {
        self.populate_nodes(root);
        self.take_nodes().into_iter()
    }

    pub fn new(iter_children: F, nodes: Vec<LevelNode>, filter_fn: FilterFn<'filter>) -> Self {
        Self {
            iter_children,
            nodes,
            filter_fn,
        }
    }

    pub fn populate_nodes(&mut self, root: usize) {
        self.nodes.clear();
        self.traverse(root, 0);
    }

    pub fn take_nodes(&mut self) -> Vec<LevelNode> {
        std::mem::take(&mut self.nodes)
    }

    fn traverse(&mut self, root: usize, level: usize) {
        for child in (self.iter_children)(root) {
            self.traverse(child, level + 1);
        }
        if (self.filter_fn)(root) {
            self.nodes.push((level, root));
        }
    }

    pub fn with_capacity(iter_children: F, capacity: usize, filter: FilterFn<'filter>) -> Self {
        Self {
            iter_children,
            nodes: Vec::with_capacity(capacity),
            filter_fn: filter,
        }
    }
}

pub struct BreadthFirst<F, I>
where
    F: FnMut(usize) -> I,
    I: Iterator<Item = usize>,
{
    iter_children: F,
    queue: VecDeque<LevelNode>,
    nodes: Vec<LevelNode>,
}

impl<F, I> BreadthFirst<F, I>
where
    F: FnMut(usize) -> I,
    I: Iterator<Item = usize>,
{
    pub fn iter(&mut self, root: usize) -> impl Iterator<Item = LevelNode> {
        self.populate_nodes(root);
        self.take_nodes().into_iter()
    }

    pub fn new(iter_children: F, queue: VecDeque<LevelNode>, nodes: Vec<LevelNode>) -> Self {
        Self {
            iter_children,
            queue,
            nodes,
        }
    }

    pub fn populate_nodes(&mut self, root: usize) {
        self.queue.push_back((0, root));
        while let Some((level, node)) = self.queue.pop_front() {
            self.nodes.push((level, node));
            for child in (self.iter_children)(node) {
                self.queue.push_back((level + 1, child));
            }
        }
    }

    pub fn take_nodes(&mut self) -> Vec<LevelNode> {
        std::mem::take(&mut self.nodes)
    }

    pub fn with_capacity(iter_children: F, node_capacity: usize, queue_capacity: usize) -> Self {
        Self {
            iter_children,
            queue: VecDeque::with_capacity(queue_capacity),
            nodes: Vec::with_capacity(node_capacity),
        }
    }
}
