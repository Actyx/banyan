use std::ops::{Bound, RangeBounds};

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
