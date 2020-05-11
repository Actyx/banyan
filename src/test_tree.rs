use std::sync::Arc;

enum Index<T> {
    Branch {
        level: u32,
        child: Box<Branch<T>>,
    },
    Leaf {
        child: Vec<T>
    }
}

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;
type Branch<T> = Vec<Index<T>>;
