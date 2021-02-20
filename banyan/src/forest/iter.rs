//! helper methods to stream trees
use crate::{index::IndexRef, store::ReadOnlyStore, tree::Tree};

use super::{FilteredChunk, Forest, TreeTypes};
use crate::query::*;
use serde::{de::DeserializeOwned, Serialize};
use std::sync::atomic::Ordering;
use std::{fmt::Debug, ops::RangeInclusive, sync::atomic::AtomicU64, sync::Arc};

impl<
        T: TreeTypes + 'static,
        V: Clone + Send + Sync + Debug + Serialize + DeserializeOwned + 'static,
        R: ReadOnlyStore<T::Link> + Clone + Send + Sync + 'static,
    > Forest<T, V, R>
{
    /// Given a sequence of roots, will iterate matching events in ascending order indefinitely.
    ///
    /// This is implemented by calling [iter_trees_chunked] and just flattening the chunks.
    pub fn iter_trees<Q, S>(
        &self,
        query: Q,
        trees: S,
    ) -> impl Iterator<Item = anyhow::Result<(u64, T::Key, V)>>
    where
        Q: Query<T> + Clone + 'static,
        S: Iterator<Item = Tree<T>> + Send + 'static,
    {
        self.iter_trees_chunked(query, trees, 0..=u64::max_value(), &|_| ())
            .map(|res| match res {
                Ok(chunk) => itertools::Either::Left(chunk.data.into_iter().map(Ok)),
                Err(cause) => itertools::Either::Right(std::iter::once(Err(cause))),
            })
            .flatten()
    }

    /// Given a sequence of roots, will stream chunks in ascending order indefinitely.
    ///
    /// Note that this method has no way to know when the query is done. So ending this stream,
    /// if desired, will have to be done by the caller using e.g. `take_while(...)`.
    /// - query: the query
    /// - roots: the stream of roots. It is assumed that trees later in this stream will be bigger
    /// - range: the range which to stream. It is up to the caller to ensure that we have events for this range.
    /// - mk_extra: a fn that allows to compute extra info from indices.
    ///     this can be useful to get progress info even if the query does not match any events
    pub fn iter_trees_chunked<I, Q, E, F>(
        &self,
        query: Q,
        trees: I,
        range: RangeInclusive<u64>,
        mk_extra: &'static F,
    ) -> impl Iterator<Item = anyhow::Result<FilteredChunk<T, V, E>>> + Send
    where
        Q: Query<T> + Clone + Send + 'static,
        E: Send + 'static,
        F: Send + Sync + Fn(IndexRef<T>) -> E + 'static,
        I: Iterator<Item = Tree<T>> + Send,
    {
        let offset = Arc::new(AtomicU64::new(*range.start()));
        let forest = self.clone();
        trees
            .filter_map(move |link| link.into_inner())
            .flat_map(move |index| {
                // create an intersection of a range query and the main query
                // and wrap it in an arc so it is cheap to clone
                let range = offset.load(Ordering::SeqCst)..=*range.end();
                let query = AndQuery(OffsetRangeQuery::from(range), query.clone()).boxed();
                let offset = offset.clone();
                forest
                    .iter_filtered_chunked0(0, query, index, mk_extra)
                    .take_while(move |result| {
                        if let Ok(chunk) = result {
                            // update the offset
                            offset.store(chunk.range.end, Ordering::SeqCst)
                        }
                        // abort at the first non-ok offset
                        result.is_ok()
                    })
            })
    }

    // /// Given a sequence of roots, will stream chunks in reverse order until it arrives at offset 0.
    // ///
    // /// Values within chunks are in ascending offset order, so if you flatten them you have to reverse them first.
    // /// - query: the query
    // /// - trees: the stream of roots. It is assumed that trees later in this stream will be bigger
    // /// - range: the range which to stream. It is up to the caller to ensure that we have events for this range.
    // /// - mk_extra: a fn that allows to compute extra info from indices.
    // ///     this can be useful to get progress info even if the query does not match any events
    // pub fn iter_trees_chunked_reverse<I, Q, E, F>(
    //     &self,
    //     query: Q,
    //     trees: I,
    //     range: RangeInclusive<u64>,
    //     mk_extra: &'static F,
    // ) -> impl Iterator<Item = anyhow::Result<FilteredChunk<T, V, E>>> + Send + 'static
    // where
    //     Q: Query<T> + Clone + Send + 'static,
    //     E: Send + 'static,
    //     F: Send + Sync + 'static + Fn(IndexRef<T>) -> E,
    //     I: Iterator<Item = Tree<T>> + Send + 'static,
    // {
    //     let end_offset_ref = Arc::new(AtomicU64::new(*range.end()));
    //     let forest = self.clone();
    //     trees
    //         .filter_map(move |tree| tree.into_inner())
    //         .flat_map(move |index| {
    //             let end_offset = end_offset_ref.load(Ordering::SeqCst);
    //             // create an intersection of a range query and the main query
    //             // and wrap it in an arc so it is cheap to clone
    //             let query = AndQuery(
    //                 OffsetRangeQuery::from(*range.start()..=end_offset),
    //                 query.clone(),
    //             )
    //             .boxed();
    //             let end_offset_ref = end_offset_ref.clone();
    //             forest
    //                 .stream_filtered_chunked_reverse0(0, query, index, mk_extra)
    //                 .take_while(move |result| {
    //                     if let Ok(chunk) = result {
    //                         // update the end offset from the start of what we got
    //                         end_offset_ref.store(chunk.range.start, Ordering::SeqCst);
    //                     }
    //                     // abort at the first non-ok offset
    //                     future::ready(result.is_ok())
    //                 })
    //         })
    // }
}
