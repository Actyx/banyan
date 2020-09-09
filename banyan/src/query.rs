//! types for querying banyan trees
//!
//! To implement querying for a new key type, implement the [Query] trait. Queries can be combined
//! using boolean combinators.
//!
//! [Query]: trait.Query.html
use crate::{
    forest::TreeTypes,
    index::{BranchIndex, CompactSeq, LeafIndex},
    util::RangeBoundsExt,
};
pub use bitvec::vec::BitVec;
use std::{fmt::Debug, ops::RangeBounds, sync::Arc};

/// A query
///
/// Queries work on compact value sequences instead of individual values for efficiency.
pub trait Query<T: TreeTypes>: Debug {
    /// a bitvec with `x.data.count()` elements, where each value is a bool indicating if the query *does* match
    fn containing(&self, offset: u64, _index: &LeafIndex<T>, res: &mut BitVec);
    /// a bitvec with `x.data.count()` elements, where each value is a bool indicating if the query *can* match
    fn intersecting(&self, offset: u64, _index: &BranchIndex<T>, res: &mut BitVec);
}

impl<T: TreeTypes> Query<T> for Box<dyn Query<T>> {
    fn containing(&self, offset: u64, x: &LeafIndex<T>, res: &mut BitVec) {
        self.as_ref().containing(offset, x, res);
    }

    fn intersecting(&self, offset: u64, x: &BranchIndex<T>, res: &mut BitVec) {
        self.as_ref().intersecting(offset, x, res);
    }
}

impl<T: TreeTypes> Query<T> for Arc<dyn Query<T>> {
    fn containing(&self, offset: u64, x: &LeafIndex<T>, res: &mut BitVec) {
        self.as_ref().containing(offset, x, res);
    }

    fn intersecting(&self, offset: u64, x: &BranchIndex<T>, res: &mut BitVec) {
        self.as_ref().intersecting(offset, x, res);
    }
}

/// The only information that does not require
#[derive(Debug, Clone)]
pub struct OffsetRangeQuery<R>(R);

impl<R: RangeBounds<u64>> From<R> for OffsetRangeQuery<R> {
    fn from(value: R) -> Self {
        Self(value)
    }
}

impl<T: TreeTypes, R: RangeBounds<u64> + Debug + Sync + Send> Query<T> for OffsetRangeQuery<R> {
    fn containing(&self, mut offset: u64, index: &LeafIndex<T>, res: &mut BitVec) {
        let range = offset..offset + index.keys.count();
        // shortcut test
        if !&self.0.intersects(&range) {
            res.set_all(false);
        } else {
            for i in 0..(index.keys.len()).min(res.len()) {
                if res[i] {
                    res.set(i, self.0.contains(&offset))
                }
                offset += 1;
            }
        }
    }

    fn intersecting(&self, offset: u64, index: &BranchIndex<T>, res: &mut BitVec) {
        // we just look at whether the entire index overlaps with the query range.
        // if not, we just clear all bits.
        let range = offset..offset + index.count;
        if !&self.0.intersects(&range) {
            res.set_all(false);
        }
    }
}

/// A query that matches nothing
#[derive(Debug, Clone)]
pub struct EmptyQuery;

impl<T: TreeTypes> Query<T> for EmptyQuery {
    fn containing(&self, _offset: u64, _index: &LeafIndex<T>, res: &mut BitVec) {
        res.set_all(false);
    }

    fn intersecting(&self, _offset: u64, _index: &BranchIndex<T>, res: &mut BitVec) {
        res.set_all(false);
    }
}

/// A query that matches everything
#[derive(Debug, Clone)]
pub struct AllQuery;

impl<T: TreeTypes> Query<T> for AllQuery {
    fn containing(&self, _offset: u64, _index: &LeafIndex<T>, _res: &mut BitVec) {
        // this query does not add any additional constraints, so we don't have to do anything
    }

    fn intersecting(&self, _offset: u64, _index: &BranchIndex<T>, _res: &mut BitVec) {
        // this query does not add any additional constraints, so we don't have to do anything
    }
}

/// An intersection of two queries.
///
/// This is equivalent to performing the two sub-queries and performing a boolean and on the results.
#[derive(Debug, Clone)]
pub struct AndQuery<A, B>(pub A, pub B);

impl<T: TreeTypes, A: Query<T>, B: Query<T>> Query<T> for AndQuery<A, B> {
    fn containing(&self, offset: u64, index: &LeafIndex<T>, res: &mut BitVec) {
        self.0.containing(offset, index, res);
        self.1.containing(offset, index, res);
    }

    fn intersecting(&self, offset: u64, index: &BranchIndex<T>, res: &mut BitVec) {
        self.0.intersecting(offset, index, res);
        self.1.intersecting(offset, index, res);
    }
}

/// Union of two subqueries
///
/// This is equivalent to performing the two sub-queries and performing a boolean or on the results.
#[derive(Debug, Clone)]
pub struct OrQuery<A, B>(pub A, pub B);

impl<T: TreeTypes, A: Query<T>, B: Query<T>> Query<T> for OrQuery<A, B> {
    fn containing(&self, offset: u64, index: &LeafIndex<T>, res: &mut BitVec) {
        let mut tmp = res.clone();
        self.0.containing(offset, index, res);
        self.1.containing(offset, index, &mut tmp);
        *res |= tmp;
    }

    fn intersecting(&self, offset: u64, index: &BranchIndex<T>, res: &mut BitVec) {
        let mut tmp = res.clone();
        self.0.intersecting(offset, index, res);
        self.1.intersecting(offset, index, &mut tmp);
        *res |= tmp;
    }
}

#[cfg(test)]
mod tests {}
