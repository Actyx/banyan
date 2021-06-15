use crate::TreeTypes;
use futures::{channel::mpsc, executor::ThreadPool, prelude::*, SinkExt};
use smallvec::{smallvec, SmallVec};
use std::{
    convert::TryFrom,
    ops::{Bound, RangeBounds},
};

pub(crate) fn nonce<T: TreeTypes>() -> &'static chacha20::XNonce {
    <&chacha20::XNonce>::try_from(T::NONCE).unwrap()
}

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

pub trait BoolSliceExt {
    /// true if any of the fields is true
    fn any(self) -> bool;
}

impl BoolSliceExt for &[bool] {
    fn any(self) -> bool {
        self.iter().any(|x| *x)
    }
}

/// Some extensions to make bool slices nicer to work with
pub trait MutBoolSliceExt {
    /// Set all bools to false
    fn clear(self);

    /// or combination with another bool slice, with false as default for where
    /// the sizes don't match.
    fn or_with(self, rhs: &[bool]);
}

impl MutBoolSliceExt for &mut [bool] {
    fn clear(self) {
        // todo: use fill https://github.com/rust-lang/rust/issues/70758 is merged
        self.iter_mut().for_each(|x| *x = false);
    }

    fn or_with(self, rhs: &[bool]) {
        for i in 0..self.len().min(rhs.len()) {
            self[i] |= rhs[i]
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
    fn into_stream(
        self,
        buffer_size: usize,
        thread_pool: ThreadPool,
    ) -> mpsc::Receiver<Self::Item> {
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

#[allow(dead_code)]
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

pub(crate) fn take_until_condition<T: Sized + Stream>(
    stream: T,
    condition: impl Fn(&T::Item) -> bool + 'static,
) -> impl Stream<Item = T::Item> {
    stream
        .flat_map(move |item| {
            let items: SmallVec<[_; 2]> = if condition(&item) {
                smallvec![Some(item), None]
            } else {
                smallvec![Some(item)]
            };
            stream::iter(items)
        })
        .take_while(|x| future::ready(x.is_some()))
        .filter_map(future::ready)
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
