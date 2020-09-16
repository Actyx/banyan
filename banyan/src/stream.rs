//! helper methods to stream trees
use crate::index::IndexRef;

use super::forest::*;
use super::query::*;
use super::tree::*;
use futures::{prelude::*, stream::BoxStream};
use serde::{de::DeserializeOwned, Serialize};
use std::sync::atomic::Ordering;
use std::{fmt::Debug, sync::atomic::AtomicU64, sync::Arc};

impl<
        T: TreeTypes + 'static,
        V: Clone + Send + Sync + Debug + Serialize + DeserializeOwned + 'static,
    > Forest<T, V>
{
    /// Given a sequence of roots, will stream matching events in ascending order indefinitely.
    ///
    /// This is implemented by calling [stream_roots_chunked] and just flattening the chunks.
    pub fn stream_roots<Q: Query<T> + Clone + 'static>(
        self: Arc<Self>,
        query: Q,
        roots: BoxStream<'static, T::Link>,
    ) -> impl Stream<Item = anyhow::Result<(u64, T::Key, V)>> + Send {
        self.stream_roots_chunked(query, roots, &|_| ())
            .map_ok(|chunk| stream::iter(chunk.data.into_iter().map(|x| Ok(x))))
            .try_flatten()
    }

    /// Given a sequence of roots, will stream chunks in ascending order indefinitely.
    ///
    /// Note that this method has no way to know when the query is done. So ending this stream,
    /// if desired, will have to be done by the caller using e.g. `take_while(...)`.
    /// - query: the query
    /// - roots: the stream of roots. It is assumed that trees later in this stream will be bigger
    /// - mk_extra: a fn that allows to compute extra info from indices.
    ///     this can be useful to get progress info even if the query does not match any events
    pub fn stream_roots_chunked<S, Q, E, F>(
        self: Arc<Self>,
        query: Q,
        roots: S,
        mk_extra: &'static F,
    ) -> impl Stream<Item = anyhow::Result<FilteredChunk<T, V, E>>> + Send + 'static
    where
        Q: Query<T> + Clone + Send + 'static,
        E: Send + 'static,
        F: Send + Sync + 'static + Fn(IndexRef<T>) -> E,
        S: Stream<Item = T::Link> + Send + 'static,
    {
        let offset = Arc::new(AtomicU64::new(0));
        roots
            .filter_map(move |cid| Tree::<T, V>::from_link(cid, self.clone()).map(|r| r.ok()))
            .flat_map(move |tree: Tree<T, V>| {
                // create an intersection of a range query and the main query
                // and wrap it in an arc so it is cheap to clone
                let query: Arc<dyn Query<T>> = Arc::new(AndQuery(
                    OffsetRangeQuery::from(offset.load(Ordering::SeqCst)..),
                    query.clone(),
                ));
                let offset = offset.clone();
                tree.stream_filtered_static_chunked(query, mk_extra)
                    .take_while(move |result| {
                        if let Ok(chunk) = result {
                            // update the offset
                            offset.store(chunk.range.end, Ordering::SeqCst)
                        }
                        // abort at the first non-ok offset
                        future::ready(result.is_ok())
                    })
            })
    }

    /// Given a sequence of roots, will stream chunks in reverse order until it arrives at offset 0.
    ///
    /// Values within chunks are in ascending offset order, so if you flatten them you have to reverse them first.
    /// - query: the query
    /// - roots: the stream of roots. It is assumed that trees later in this stream will be bigger
    /// - end_offset: the *exclusive* end offset from which to stream
    /// - mk_extra: a fn that allows to compute extra info from indices.
    ///     this can be useful to get progress info even if the query does not match any events
    pub fn stream_roots_chunked_reverse<S, Q, E, F>(
        self: Arc<Self>,
        query: Q,
        roots: S,
        end_offset: u64,
        mk_extra: &'static F,
    ) -> impl Stream<Item = anyhow::Result<FilteredChunk<T, V, E>>> + Send + 'static
    where
        Q: Query<T> + Clone + Send + 'static,
        E: Send + 'static,
        F: Send + Sync + 'static + Fn(IndexRef<T>) -> E,
        S: Stream<Item = T::Link> + Send + 'static,
    {
        let end_offset_ref = Arc::new(AtomicU64::new(end_offset));
        roots
            .filter_map(move |cid| Tree::<T, V>::from_link(cid, self.clone()).map(|r| r.ok()))
            .flat_map(move |tree: Tree<T, V>| {
                let end_offset = end_offset_ref.load(Ordering::SeqCst);
                // create an intersection of a range query and the main query
                // and wrap it in an arc so it is cheap to clone
                let query: Arc<dyn Query<T>> = Arc::new(AndQuery(
                    OffsetRangeQuery::from(..end_offset),
                    query.clone(),
                ));
                let end_offset_ref = end_offset_ref.clone();
                tree.stream_filtered_static_chunked_reverse(query, mk_extra)
                    .take_while(move |result| {
                        if let Ok(chunk) = result {
                            // update the end offset from the start of what we got
                            end_offset_ref.store(chunk.range.start, Ordering::SeqCst);
                        }
                        // abort at the first non-ok offset
                        future::ready(result.is_ok())
                    })
            })
    }
}
