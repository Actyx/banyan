
use serde::{Serialize, Deserialize, de::DeserializeOwned};

pub trait Semigroup {
    fn combine(&mut self, b: &Self);
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
    /// extends the last element with the item
    fn extend(&mut self, value: &Self::Item);
    /// combines all elements with the semigroup op
    fn summarize(&self) -> Self::Item;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimpleCompactSeq<T>(Vec<T>);

impl<T: Serialize + DeserializeOwned + Semigroup + Clone> CompactSeq for SimpleCompactSeq<T> {
    type Item = T;
    fn single(item: T) -> Self {
        Self(vec![item])
    }
    fn push(&mut self, item: T) {
        self.0.push(item)
    }
    fn extend(&mut self, item: &T) {
        self.0.last_mut().unwrap().combine(item);
    }
    fn summarize(&self) -> T {
        let mut res = self.0[0].clone();
        for i in 1..self.0.len() {
            res.combine(&self.0[i]);
        }
        res
    }
}
