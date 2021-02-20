use futures::{channel::mpsc, executor::ThreadPool, SinkExt};
use libipld::Ipld;
use std::{
    collections::BTreeMap,
    ops::{Bound, RangeBounds},
};

fn lt<T: Ord>(end: Bound<T>, start: Bound<T>) -> bool {
    match (end, start) {
        (Bound::Unbounded, _) => false,
        (_, Bound::Unbounded) => false,
        (Bound::Included(end), Bound::Included(start)) => end < start,
        (Bound::Excluded(end), Bound::Included(start)) => end <= start,
        (Bound::Included(end), Bound::Excluded(start)) => end <= start,
        (Bound::Excluded(end), Bound::Excluded(start)) => end <= start,
    }
}

pub(crate) trait RangeBoundsExt<T: Ord>: RangeBounds<T> {
    fn intersects(&self, b: &impl RangeBounds<T>) -> bool {
        !lt(self.end_bound(), b.start_bound()) && !lt(b.end_bound(), self.start_bound())
    }
}

impl<T: Ord, R: RangeBounds<T>> RangeBoundsExt<T> for R {}

pub(crate) fn is_sorted<T: Ord>(iter: impl Iterator<Item = T>) -> bool {
    iter.collect::<Vec<_>>().windows(2).all(|x| x[0] <= x[1])
}

/// Some extensions to make bool slices nicer to work with
pub trait BoolSliceExt {
    fn clear(self);

    fn any(self) -> bool;

    fn or_with(self, rhs: &[bool]);
}

impl BoolSliceExt for &mut [bool] {
    fn clear(self) {
        // todo: use fill https://github.com/rust-lang/rust/issues/70758 is merged
        self.iter_mut().for_each(|x| *x = false);
    }

    fn any(self) -> bool {
        self.iter().any(|x| *x)
    }

    fn or_with(self, rhs: &[bool]) {
        for i in 0..self.len().min(rhs.len()) {
            self[i] |= rhs[i]
        }
    }
}

#[derive(libipld::DagCbor)]
pub(crate) struct IpldNode(BTreeMap<u64, Ipld>, Ipld);

impl IpldNode {
    pub fn new(links: BTreeMap<u64, Ipld>, data: impl Into<Vec<u8>>) -> Self {
        Self(links, Ipld::Bytes(data.into()))
    }

    pub fn into_data(self) -> anyhow::Result<Vec<u8>> {
        if let Ipld::Bytes(data) = self.1 {
            Ok(data)
        } else {
            Err(anyhow::anyhow!("expected ipld bytes"))
        }
    }
}

/// Some convenience fns so we don't have to depend on IterTools
pub(crate) trait IterExt<'a>
where
    Self: Iterator + Sized + Send + 'a,
{
    fn boxed(self) -> BoxedIter<'a, Self::Item> {
        Box::new(self)
    }

    fn left_iter<R>(self) -> EitherIter<Self, R> {
        EitherIter::Left(self)
    }

    fn right_iter<L>(self) -> EitherIter<L, Self> {
        EitherIter::Right(self)
    }
}

impl<'a, T: Iterator + Sized + Send + 'a> IterExt<'a> for T {}

/// same approach as https://crates.io/crates/iterstream
pub(crate) trait ToStreamExt: Iterator
where
    Self: 'static + Sized + Send,
    Self::Item: Send,
{
    fn to_stream(self, buffer_size: usize, thread_pool: ThreadPool) -> mpsc::Receiver<Self::Item> {
        let (mut sender, receiver) = mpsc::channel(buffer_size);

        thread_pool.spawn_ok(async move {
            for value in self {
                if sender.send(value).await.is_err() {
                    break;
                }
            }
        });
        receiver
    }
}

impl<I> ToStreamExt for I
where
    I: Iterator + Send + 'static,
    I::Item: Send,
{
}

pub(crate) type BoxedIter<'a, T> = Box<dyn Iterator<Item = T> + Send + 'a>;

/// Like the one from itertools, but more convenient
pub(crate) enum EitherIter<L, R> {
    Left(L),
    Right(R),
}

impl<L, R, T> Iterator for EitherIter<L, R>
where
    L: Iterator<Item = T>,
    R: Iterator<Item = T>,
{
    type Item = T;
    fn next(&mut self) -> std::option::Option<<Self as Iterator>::Item> {
        match self {
            Self::Left(l) => l.next(),
            Self::Right(r) => r.next(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intersects() {
        assert!(!(0..10).intersects(&(10..20)));
        assert!((0..11).intersects(&(10..20)));

        assert!(!(0..10).intersects(&(10..)));
        assert!((0..11).intersects(&(10..)));

        assert!(!(..10).intersects(&(10..)));
        assert!((..11).intersects(&(10..)));

        assert!(!(..=10).intersects(&(11..)));
        assert!((..=11).intersects(&(11..)));

        assert!((..).intersects(&(10..20)));
        assert!((10..20).intersects(&(..)));
    }
}
