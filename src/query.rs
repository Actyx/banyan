use crate::{
    index::{BranchIndex, CompactSeq, LeafIndex},
    tree::TreeTypes,
    util::RangeBoundsExt,
};
use bitvec::prelude::BitVec;
use std::ops::RangeBounds;

/// A query
///
/// Queries work on compact value sequences instead of individual values for efficiency.
pub trait Query<T: TreeTypes> {
    /// a bitvec with `x.data.count()` elements, where each value is a bool indicating if the query *does* match
    fn containing(&self, offset: u64, _index: &LeafIndex<T::Link, T::Seq>, res: &mut BitVec);
    /// a bitvec with `x.data.count()` elements, where each value is a bool indicating if the query *can* match
    fn intersecting(&self, offset: u64, _index: &BranchIndex<T::Link, T::Seq>, res: &mut BitVec);
}

impl<TT: TreeTypes> Query<TT> for Box<dyn Query<TT>> {
    fn containing(&self, offset: u64, x: &LeafIndex<TT::Link, TT::Seq>, res: &mut BitVec) {
        self.as_ref().containing(offset, x, res);
    }

    fn intersecting(&self, offset: u64, x: &BranchIndex<TT::Link, TT::Seq>, res: &mut BitVec) {
        self.as_ref().intersecting(offset, x, res);
    }
}

#[derive(Debug, Clone)]
pub struct OffsetRangeQuery<R>(R);

impl<R: RangeBounds<u64>> From<R> for OffsetRangeQuery<R> {
    fn from(value: R) -> Self {
        Self(value)
    }
}

impl<T: TreeTypes, R: RangeBounds<u64>> Query<T> for OffsetRangeQuery<R> {
    fn containing(&self, mut offset: u64, index: &LeafIndex<T::Link, T::Seq>, res: &mut BitVec) {
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

    fn intersecting(&self, offset: u64, index: &BranchIndex<T::Link, T::Seq>, res: &mut BitVec) {
        // we just look at whether the entire index overlaps with the query range.
        // if not, we just clear all bits.
        let range = offset..offset + index.count;
        if !&self.0.intersects(&range) {
            res.set_all(false);
        }
    }
}

pub struct EmptyQuery;

impl<TT: TreeTypes> Query<TT> for EmptyQuery {
    fn containing(&self, _offset: u64, _index: &LeafIndex<TT::Link, TT::Seq>, res: &mut BitVec) {
        res.set_all(false);
    }

    fn intersecting(&self, _offset: u64, _index: &BranchIndex<TT::Link, TT::Seq>, res: &mut BitVec) {
        res.set_all(false);
    }
}

pub struct AllQuery;

impl<TT: TreeTypes> Query<TT> for AllQuery {
    fn containing(&self, _offset: u64, _index: &LeafIndex<TT::Link, TT::Seq>, _res: &mut BitVec) {
        // this query does not add any additional constraints, so we don't have to do anything
    }

    fn intersecting(&self, _offset: u64, _index: &BranchIndex<TT::Link, TT::Seq>, _res: &mut BitVec) {
        // this query does not add any additional constraints, so we don't have to do anything
    }
}

pub struct AndQuery<A, B>(A, B);

impl<TT: TreeTypes, A: Query<TT>, B: Query<TT>> Query<TT> for AndQuery<A, B> {
    fn containing(&self, offset: u64, index: &LeafIndex<TT::Link, TT::Seq>, res: &mut BitVec) {
        self.0.containing(offset, index, res);
        self.1.containing(offset, index, res);
    }

    fn intersecting(&self, offset: u64, index: &BranchIndex<TT::Link, TT::Seq>, res: &mut BitVec) {
        self.0.intersecting(offset, index, res);
        self.1.intersecting(offset, index, res);
    }
}

#[cfg(test)]
mod tests {}
