//! helper methods to stream trees
use super::query::*;
use super::tree::*;
use futures::prelude::*;
use serde::{de::DeserializeOwned, Serialize};
use std::{cell::Cell, fmt::Debug, rc::Rc, sync::Arc};
use stream::LocalBoxStream;

pub struct SourceStream<TT: TreeTypes + 'static, Q>(pub Arc<Forest<TT>>, pub Q);

impl<TT: TreeTypes + 'static, Q: Query<TT> + Clone + 'static> SourceStream<TT, Q> {
    pub fn stream<V: Serialize + DeserializeOwned + Clone + Send + Sync + Debug + 'static>(
        self,
        roots: LocalBoxStream<'static, TT::Link>,
    ) -> impl Stream<Item = anyhow::Result<(u64, TT::Key, V)>> {
        let offset = Rc::new(Cell::new(0u64));
        let query = self.1;
        let forest = self.0;
        roots
            .filter_map(move |cid| Tree::<TT, V>::from_link(cid, forest.clone()).map(|r| r.ok()))
            .flat_map(move |tree: Tree<TT, V>| {
                // create an intersection of a range query and the main query
                // and wrap it in an rc so it is cheap to clone
                let query: Rc<dyn Query<TT>> = Rc::new(AndQuery(
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

    pub fn stream_chunked<
        V: Serialize + DeserializeOwned + Clone + Send + Sync + Debug + 'static,
    >(
        self,
        roots: LocalBoxStream<'static, TT::Link>,
    ) -> impl Stream<Item = anyhow::Result<FilteredChunk<TT, V>>> {
        let offset = Rc::new(Cell::new(0u64));
        let query = self.1;
        let forest = self.0;
        roots
            .filter_map(move |cid| Tree::<TT, V>::from_link(cid, forest.clone()).map(|r| r.ok()))
            .flat_map(move |tree: Tree<TT, V>| {
                // create an intersection of a range query and the main query
                // and wrap it in an rc so it is cheap to clone
                let query: Rc<dyn Query<TT>> = Rc::new(AndQuery(
                    OffsetRangeQuery::from(offset.get()..),
                    query.clone(),
                ));
                // dump the results while updating the offset
                let offset = offset.clone();
                tree.stream_filtered_static_chunked(query)
                    .take_while(move |result| {
                        if let Ok(chunk) = result {
                            // update the offset
                            offset.set(chunk.max)
                        }
                        // abort at the first non-ok offset
                        future::ready(result.is_ok())
                    })
            })
    }
}
