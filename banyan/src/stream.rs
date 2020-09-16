//! helper methods to stream trees
use crate::index::IndexRef;

use super::forest::*;
use super::query::*;
use super::tree::*;
use futures::{prelude::*, stream::BoxStream};
use serde::{de::DeserializeOwned, Serialize};
use std::{cell::Cell, fmt::Debug, rc::Rc, sync::Arc, sync::atomic::AtomicU64};
use std::sync::atomic::Ordering;

impl<
        T: TreeTypes + 'static,
        V: Clone + Send + Sync + Debug + Serialize + DeserializeOwned + 'static,
    > Forest<T, V>
{
    pub fn stream_roots<Q: Query<T> + Clone + 'static>(
        self: Arc<Self>,
        query: Q,
        roots: BoxStream<'static, T::Link>,
    ) -> impl Stream<Item = anyhow::Result<(u64, T::Key, V)>> {
        let offset = Rc::new(Cell::new(0u64));
        let forest = self;
        roots
            .filter_map(move |cid| Tree::<T, V>::from_link(cid, forest.clone()).map(|r| r.ok()))
            .flat_map(move |tree: Tree<T, V>| {
                // create an intersection of a range query and the main query
                // and wrap it in an rc so it is cheap to clone
                let query: Arc<dyn Query<T>> = Arc::new(AndQuery(
                    OffsetRangeQuery::from(offset.get()..),
                    query.clone(),
                ));
                // dump the results while updating the offset
                let offset = offset.clone();
                tree.stream_filtered_static(query)
                    .take_while(move |result| {
                        if let Ok((o, _, _)) = result {
                            // update the offset
                            offset.set(*o + 1)
                        }
                        // abort at the first non-ok offset
                        future::ready(result.is_ok())
                    })
            })
    }

    pub fn stream_roots_chunked<Q: Query<T> + Clone + Send + 'static, E: Send + 'static, F: Send + Sync + 'static + Fn(IndexRef<T>) -> E>(
        self: Arc<Self>,
        query: Q,
        roots: BoxStream<'static, T::Link>,
        mk_extra: &'static F,
    ) -> impl Stream<Item = anyhow::Result<FilteredChunk<T, V, E>>> {
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
            .boxed()
    }
}
