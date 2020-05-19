use serde::{de::DeserializeOwned, Deserialize, Serialize};

pub trait Semigroup {
    fn combine(&mut self, b: &Self);
}

/// a compact representation of a sequence of 1 or more items
///
/// in general, this will have a different internal representation than just a bunch of values.
///
pub trait CompactSeq: Serialize + DeserializeOwned {
    /// item type
    type Item: Semigroup;
    /// Creates a sequence with a single element
    fn single(item: &Self::Item) -> Self;
    /// pushes an additional element to the end
    fn push(&mut self, value: &Self::Item);
    /// extends the last element with the item
    fn extend(&mut self, value: &Self::Item);

    fn items(&self) -> Vec<Self::Item>;

    fn get(&self, index: usize) -> Option<Self::Item>;
    /// combines all elements with the semigroup op
    fn summarize(&self) -> Self::Item;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimpleCompactSeq<T>(Vec<T>);

impl<T: Serialize + DeserializeOwned + Semigroup + Clone> CompactSeq for SimpleCompactSeq<T> {
    type Item = T;
    fn single(item: &T) -> Self {
        Self(vec![item.clone()])
    }
    fn push(&mut self, item: &T) {
        self.0.push(item.clone())
    }
    fn extend(&mut self, item: &T) {
        self.0.last_mut().unwrap().combine(item);
    }
    fn get(&self, index: usize) -> Option<T> {
        self.0.get(index).cloned()
    }
    fn items(&self) -> Vec<T> {
        self.0.clone()
    }
    fn summarize(&self) -> T {
        let mut res = self.0[0].clone();
        for i in 1..self.0.len() {
            res.combine(&self.0[i]);
        }
        res
    }
}
