
use serde::{Serialize, Deserialize, de::DeserializeOwned};

pub trait Semigroup {
    fn combine(self, b: Self) -> Self;
}

/// a compact representation of a sequence of 1 or more items
/// can be serialized/deserialized
pub trait CompactSeq {
    /// item type
    type Item: Semigroup;
    /// Creates a sequence with a single element
    fn single(item: Self::Item) -> Self;
    /// pushes an additional element to the end
    fn push(&mut self, value: Self::Item);
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleCompactSeq<T>(Vec<T>);

impl<T: Serialize + DeserializeOwned + Semigroup + Clone> CompactSeq for SimpleCompactSeq<T> {
    type Item = T;
    fn single(item: T) -> Self {
        Self(vec![item])
    }
    fn push(&mut self, item: T) {
        self.0.push(item)
    }
}
