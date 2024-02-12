use std::sync::Arc;

#[derive(PartialEq, Eq, Hash)]
pub enum Children<T> {
    Zero,
    One(T),
    Two(T, T),
}

#[derive(PartialEq, Eq, Hash)]
pub struct BadDag {
    pub value: usize,
    pub children: Children<Arc<BadDag>>,
}

impl BadDag {
    pub fn leaf() -> Arc<Self> {
        Arc::new(Self {
            value: 0,
            children: Children::Zero,
        })
    }

    pub fn new_unary(value: usize, child: Arc<Self>) -> Arc<Self> {
        Arc::new(Self {
            value,
            children: Children::One(child),
        })
    }

    pub fn new_binary(value: usize, left: Arc<Self>, right: Arc<Self>) -> Arc<Self> {
        Arc::new(Self {
            value,
            children: Children::Two(left, right),
        })
    }

    /// Tree looks like this:
    ///
    /// ```text
    ///       X
    ///      / \
    ///     X   X
    /// ```
    pub fn basic_tree() -> Arc<Self> {
        Self::new_binary(1, Self::leaf(), Self::leaf())
    }

    /// Tree looks like this:
    ///
    /// ```text
    ///         X
    ///        / \
    ///       X   X
    ///      / \
    ///     X   X
    ///        / \
    ///       X   X
    /// ```
    pub fn binary_tree() -> Arc<Self> {
        let low_right = Self::basic_tree();
        let left = Self::new_binary(2, Self::leaf(), low_right);
        Self::new_binary(8, left, Self::leaf())
    }

    /// DAG looks like this:
    ///
    /// ```text
    ///       X
    ///       |
    ///       X
    ///      / \
    ///     X   X
    ///      \ /
    ///       X
    ///      / \
    ///     X   X
    /// ```
    pub fn medium_dag() -> Arc<Self> {
        let bottom_tree = Self::basic_tree();

        let split_left = Self::new_unary(2, bottom_tree.clone());
        let split_right = Self::new_unary(4, bottom_tree.clone());

        let mid = Self::new_binary(8, split_left, split_right);

        Self::new_unary(16, mid)
    }
}
