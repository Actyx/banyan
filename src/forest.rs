use serde::{de::DeserializeOwned, Deserialize, Serialize};

/// trait for items that can be combined in an associative and commutative way
///
/// https://mathworld.wolfram.com/AbelianSemigroup.html
pub trait Semigroup {
    fn combine(&mut self, b: &Self);
}

/// a compact representation of a sequence of 1 or more items
///
/// in general, this will have a different internal representation than just a bunch of values that is more compact and
/// makes it easier to query an entire sequence for matching indices.
pub trait CompactSeq: Serialize + DeserializeOwned {
    /// item type
    type Item: Semigroup;
    /// Creates a sequence with a single element
    fn single(item: &Self::Item) -> Self;
    /// pushes an additional element to the end
    fn push(&mut self, value: &Self::Item);
    /// extends the last element with the item
    fn extend(&mut self, value: &Self::Item);
    /// number of elements
    fn count(&self) -> u64;
    /// get nth element. Guaranteed to succeed with Some for index < count.
    fn get(&self, index: u64) -> Option<Self::Item>;
    /// combines all elements with the semigroup op
    fn summarize(&self) -> Self::Item;
}

/// utility function to get all items for a compactseq.
///
/// This can not be a trait method because it returns an unnameable type, and therefore requires impl Trait,
/// which is not available within traits.
pub fn compactseq_items<'a, T: CompactSeq>(value: &'a T) -> impl Iterator<Item = T::Item> + 'a {
    (0..value.count()).map(move |i| value.get(i).unwrap())
}

/// utility function to select some items for a compactseq.
///
/// This can not be a trait method because it returns an unnameable type, and therefore requires impl Trait,
/// which is not available within traits.
pub fn compactseq_select_items<'a, T: CompactSeq>(
    value: &'a T,
    it: impl Iterator<Item = bool> + 'a,
) -> impl Iterator<Item = (u64, T::Item)> + 'a {
    (0..value.count()).zip(it).filter_map(move |(i, take)| {
        if take {
            Some((i, value.get(i).unwrap()))
        } else {
            None
        }
    })
}

/// A trivial implementation of a CompactSeq as just a Seq.
///
/// This is useful mostly as a reference impl and for testing.
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
    fn get(&self, index: u64) -> Option<T> {
        self.0.get(index as usize).cloned()
    }
    fn count(&self) -> u64 {
        self.0.len() as u64
    }
    fn summarize(&self) -> T {
        let mut res = self.0[0].clone();
        for i in 1..self.0.len() {
            res.combine(&self.0[i]);
        }
        res
    }
}
