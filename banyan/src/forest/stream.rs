//! helper methods to stream trees
use crate::{
    index::IndexRef,
    store::{BanyanValue, ReadOnlyStore},
    tree::Tree,
    util::{take_until_condition, ToStreamExt},
};

use super::{FilteredChunk, Forest, TreeTypes};
use crate::query::*;
use futures::executor::ThreadPool;
use futures::prelude::*;
use std::sync::atomic::Ordering;
use std::{ops::RangeInclusive, sync::atomic::AtomicU64, sync::Arc};

impl<T: TreeTypes, R: ReadOnlyStore<T::Link>> Forest<T, R> {
    /// Given a sequence of roots, will stream matching events in ascending order indefinitely.
    ///
    /// This is implemented by calling stream_trees_chunked and just flattening the chunks.
    pub fn stream_trees<Q, S, V>(
        &self,
        query: Q,
        trees: S,
    ) -> impl Stream<Item = anyhow::Result<(u64, T::Key, V)>> + Send
    where
        Q: Query<T> + Clone,
        S: Stream<Item = Tree<T, V>> + Send + 'static,
        V: BanyanValue,
    {
        self.stream_trees_chunked(query, trees, 0..=u64::max_value(), &|_| ())
            .map_ok(|chunk| stream::iter(chunk.data.into_iter().map(Ok)))
            .try_flatten()
    }

    /// Given a sequence of roots, will stream chunks in ascending order until it arrives at `range.end()`.
    /// - query: the query
    /// - roots: the stream of roots. It is assumed that trees later in this stream will be bigger
    /// - range: the range which to stream. It is up to the caller to ensure that we have events for this range.
    /// - mk_extra: a fn that allows to compute extra info from indices.
    ///     this can be useful to get progress info even if the query does not match any events
    pub fn stream_trees_chunked<S, Q, V, E, F>(
        &self,
        query: Q,
        trees: S,
        range: RangeInclusive<u64>,
        mk_extra: &'static F,
    ) -> impl Stream<Item = anyhow::Result<FilteredChunk<(u64, T::Key, V), E>>> + Send + 'static
    where
        S: Stream<Item = Tree<T, V>> + Send + 'static,
        Q: Query<T> + Clone,
        V: BanyanValue,
        E: Send + 'static,
        F: Send + Sync + 'static + Fn(IndexRef<T>) -> E,
    {
        let end = *range.end();
        let start_offset_ref = Arc::new(AtomicU64::new(*range.start()));
        let forest = self.clone();
        let result = trees
            .filter_map(move |tree| future::ready(tree.into_inner()))
            .flat_map(move |(index, secrets, _)| {
                // create an intersection of a range query and the main query
                // and wrap it in an arc so it is cheap to clone
                let range = start_offset_ref.load(Ordering::SeqCst)..=*range.end();
                let query = AndQuery(OffsetRangeQuery::from(range), query.clone()).boxed();
                let start_offset_ref = start_offset_ref.clone();
                forest
                    .stream_filtered_chunked0(secrets, query, index, mk_extra)
                    .take_while(move |result| {
                        if let Ok(chunk) = result {
                            // update the offset
                            start_offset_ref.store(chunk.range.end, Ordering::SeqCst)
                        }
                        // abort at the first non-ok offset
                        future::ready(result.is_ok())
                    })
            });
        // make sure we terminate when `end` is reached
        take_until_condition(result, move |chunk| match chunk {
            // end is inclusive, whereas `chunk.range.end` is exclusive
            Ok(chunk) => chunk.range.end > end,
            Err(_) => true,
        })
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
    pub fn stream_trees_chunked_threaded<S, Q, V, E, F>(
        &self,
        query: Q,
        trees: S,
        range: RangeInclusive<u64>,
        mk_extra: &'static F,
        thread_pool: ThreadPool,
    ) -> impl Stream<Item = anyhow::Result<FilteredChunk<(u64, T::Key, V), E>>> + Send + 'static
    where
        S: Stream<Item = Tree<T, V>> + Send + 'static,
        Q: Query<T> + Clone,
        V: BanyanValue,
        E: Send + 'static,
        F: Send + Sync + 'static + Fn(IndexRef<T>) -> E,
    {
        let offset = Arc::new(AtomicU64::new(*range.start()));
        let forest = self.clone();
        trees
            .filter_map(move |tree| future::ready(tree.into_inner()))
            .flat_map(move |(index, secrets, _)| {
                // create an intersection of a range query and the main query
                // and wrap it in an arc so it is cheap to clone
                let range = offset.load(Ordering::SeqCst)..=*range.end();
                let query = AndQuery(OffsetRangeQuery::from(range), query.clone()).boxed();
                let offset = offset.clone();
                let iter = forest
                    .clone()
                    .traverse0(secrets, query, index, mk_extra)
                    .take_while(move |result| {
                        if let Ok(chunk) = result {
                            // update the offset
                            offset.store(chunk.range.end, Ordering::SeqCst)
                        }
                        // abort at the first non-ok offset
                        result.is_ok()
                    });
                iter.into_stream(1, thread_pool.clone())
            })
    }

    /// Given a sequence of roots, will stream chunks in reverse order until it arrives at `range.start()`.
    ///
    /// Values within chunks are in ascending offset order, so if you flatten them you have to reverse them first.
    /// - query: the query
    /// - trees: the stream of roots. It is assumed that trees later in this stream will be bigger
    /// - range: the range which to stream. It is up to the caller to ensure that we have events for this range.
    /// - mk_extra: a fn that allows to compute extra info from indices.
    ///     this can be useful to get progress info even if the query does not match any events
    pub fn stream_trees_chunked_reverse<S, Q, V, E, F>(
        &self,
        query: Q,
        trees: S,
        range: RangeInclusive<u64>,
        mk_extra: &'static F,
    ) -> impl Stream<Item = anyhow::Result<FilteredChunk<(u64, T::Key, V), E>>> + Send + 'static
    where
        S: Stream<Item = Tree<T, V>> + Send + 'static,
        Q: Query<T> + Clone,
        V: BanyanValue,
        E: Send + 'static,
        F: Send + Sync + 'static + Fn(IndexRef<T>) -> E,
    {
        let start = *range.start();
        let end_offset_ref = Arc::new(AtomicU64::new(*range.end()));
        let forest = self.clone();
        let result = trees
            .filter_map(move |tree| future::ready(tree.into_inner()))
            .flat_map(move |(index, secrets, _)| {
                let end_offset = end_offset_ref.load(Ordering::SeqCst);
                // create an intersection of a range query and the main query
                // and wrap it in an arc so it is cheap to clone
                let query = AndQuery(
                    OffsetRangeQuery::from(*range.start()..=end_offset),
                    query.clone(),
                )
                .boxed();
                let end_offset_ref = end_offset_ref.clone();
                forest
                    .stream_filtered_chunked_reverse0(&secrets, query, index, mk_extra)
                    .take_while(move |result| {
                        if let Ok(chunk) = result {
                            // update the end offset from the start of what we got
                            end_offset_ref.store(chunk.range.start, Ordering::SeqCst);
                        }
                        // abort at the first non-ok offset
                        future::ready(result.is_ok())
                    })
            });
        // make sure we terminate when `start` is reached
        take_until_condition(result, move |chunk| match chunk {
            Ok(chunk) => chunk.range.start <= start,
            Err(_) => true,
        })
    }
}
