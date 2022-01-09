use banyan::{
    index::{BranchIndex, LeafIndex},
    query::Query,
    store::{BranchCache, MemStore},
    Config, Forest, Secrets, StreamBuilder, Tree,
};
use common::{Key, KeyQuery, Sha256Digest, TT};
use fnv::FnvHashMap;
use futures::prelude::*;
use rand::Rng;
use range_collections::RangeSet;
use std::{sync::Arc, time::Duration};
use std::{
    task::{Context, Poll, Waker},
    usize,
};
use tokio::sync::Mutex;

mod common;

#[derive(Clone)]
struct MiniStore {
    forest: Forest<TT, MemStore<Sha256Digest>>,
    builder: Arc<Mutex<StreamBuilder<TT, u64>>>,
    current: Variable<Tree<TT, u64>>,
}

impl MiniStore {
    pub fn new() -> Self {
        Self {
            forest: Forest::new(
                MemStore::new(usize::max_value(), Sha256Digest::digest),
                BranchCache::new(1 << 20),
            ),
            builder: Arc::new(Mutex::new(StreamBuilder::new(
                Config::debug(),
                Secrets::default(),
            ))),
            current: Variable::new(Tree::default()),
        }
    }

    pub async fn push(&self, xs: Vec<(Key, u64)>) -> anyhow::Result<()> {
        let mut guard = self.builder.lock().await;
        let mut txn = self.forest.transaction(|x| (x.clone(), x));
        txn.extend_unpacked(&mut guard, xs)?;
        self.current.set(guard.snapshot());
        Ok(())
    }

    pub fn forest(&self) -> &Forest<TT, MemStore<Sha256Digest>> {
        &self.forest
    }

    pub fn trees(&self) -> impl Stream<Item = Tree<TT, u64>> {
        self.current.new_observer()
    }
}
#[derive(Debug, Clone)]
pub struct EqQuery(Key);

impl Query<TT> for EqQuery {
    fn containing(&self, _: u64, index: &LeafIndex<TT>, res: &mut [bool]) {
        for (key, res) in index.keys().zip(res.iter_mut()) {
            *res = key == self.0
        }
    }

    fn intersecting(&self, _: u64, _index: &BranchIndex<TT>, _: &mut [bool]) {}
}

#[tokio::test(flavor = "multi_thread")]
async fn hammer_mini_store_tokio() -> anyhow::Result<()> {
    let n_writers = 20;
    let n_events = 20;
    let store = MiniStore::new();
    let handles = (0..n_writers)
        .flat_map(|i| {
            let w = store.clone();
            let r = store.clone();
            let wh = tokio::task::spawn(async move {
                for j in 0..n_events {
                    println!("Thread {} pushing {}", i, j);
                    w.push(vec![(Key(i), j)]).await.unwrap();
                    let delay = rand::thread_rng().gen_range(1..100);
                    println!("sleeping for {}", delay);
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
            });
            let rh = tokio::task::spawn(async move {
                let trees = r.trees();
                let events = r
                    .forest()
                    .stream_trees(KeyQuery(RangeSet::from(i..i + 1)), trees)
                    .take(n_events as usize)
                    .inspect_ok(|ev| println!("reader {} got event {:?}", i, ev))
                    .map_ok(|(_, _, value)| value)
                    .collect::<Vec<_>>()
                    .await;
                let events = events.into_iter().flat_map(|x| x.ok()).collect::<Vec<_>>();
                let expected = (0..n_events).collect::<Vec<_>>();
                assert_eq!(events, expected);
                println!("events {:?}", events);
            });
            vec![wh, rh]
        })
        .collect::<Vec<_>>();
    futures::future::join_all(handles).await;
    let tree = store.trees().next().await.unwrap();
    let events = store
        .forest()
        .collect(&tree)?
        .into_iter()
        .flatten()
        .map(|x| x.1)
        .collect::<Vec<_>>();
    println!("events {:?}", events);
    assert_eq!(events.len() as u64, n_events * n_writers);
    Ok(())
}

#[derive(Debug)]
pub struct Observer<T> {
    id: usize,
    inner: Arc<parking_lot::Mutex<VariableInner<T>>>,
}

impl<T> Observer<T> {
    fn new(inner: Arc<parking_lot::Mutex<VariableInner<T>>>) -> Self {
        let id = inner.lock().new_observer_id();
        Self { id, inner }
    }
}

fn poll_next_impl<'a, T, U>(
    mut inner: parking_lot::MutexGuard<'a, VariableInner<T>>,
    id: usize,
    cx: &mut Context<'_>,
    f: &impl Fn(&T) -> U,
) -> std::task::Poll<Option<U>> {
    if inner.writers == 0 {
        // if the sender is gone, make sure that the final value is delivered
        // (the .remove() ensures that next time will return None)
        if let Some(receiver) = inner.observers.remove(&id) {
            if !receiver.received {
                return Poll::Ready(Some(f(&inner.latest)));
            }
        }
        Poll::Ready(None)
    } else if let Some(receiver) = inner.observers.get_mut(&id) {
        if receiver.received {
            receiver.waker = Some(cx.waker().clone());
            // we have already received this value
            Poll::Pending
        } else {
            // got a value, make sure we don't get it again and return it
            receiver.received = true;
            Poll::Ready(Some(f(&inner.latest)))
        }
    } else {
        // this means that the sender was dropped, so end the stream
        Poll::Ready(None)
    }
}

impl<T: Clone> Stream for Observer<T> {
    type Item = T;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        poll_next_impl(self.inner.lock(), self.id, cx, &|x: &T| x.clone())
    }
}

/// A variable that can be observed by an arbitrary number of observer streams
///
/// Observer streams will only get the most recent variable value.
///
/// Having zero observers is often useful, so setting the value will not fail
/// even if there are no observers.
#[derive(Debug)]
pub struct Variable<T> {
    inner: Arc<parking_lot::Mutex<VariableInner<T>>>,
}

impl<T> Variable<T> {
    pub fn new(value: T) -> Self {
        let inner = Arc::new(parking_lot::Mutex::new(VariableInner::new(value)));
        Self { inner }
    }

    /// Number of current observers.
    pub fn observer_count(&self) -> usize {
        self.inner.lock().observers.len()
    }

    /// Send a value and notify all current receivers.
    /// This will not fail even if all receivers are dropped. It will just go into nirvana.
    pub fn set(&self, value: T) {
        self.inner.lock().set(value)
    }

    /// One way of creating a new observer. The other is to clone an existing observer.
    pub fn new_observer(&self) -> Observer<T> {
        Observer::new(self.inner.clone())
    }
}

impl<T> Clone for Variable<T> {
    fn clone(&self) -> Self {
        self.inner.lock().writers += 1;
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Drop for Variable<T> {
    fn drop(&mut self) {
        self.inner.lock().writers -= 1;
    }
}

impl<T> Unpin for Variable<T> {}

impl<T: Default> Default for Variable<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

#[derive(Debug)]
struct VariableInner<T> {
    next_id: usize,
    observers: FnvHashMap<usize, ReceiverInner>,
    latest: T,
    writers: usize,
}

impl<T> VariableInner<T> {
    pub fn new(value: T) -> Self {
        Self {
            next_id: 0,
            observers: Default::default(),
            latest: value,
            writers: 1,
        }
    }

    fn set(&mut self, value: T) {
        // we don't check for dupliates. You can send the same value twice.
        self.latest = value;
        self.notify();
    }

    fn notify(&mut self) {
        for observer in self.observers.values_mut() {
            // reset received
            observer.received = false;
            if let Some(waker) = observer.waker.take() {
                waker.wake();
            }
        }
    }

    /// Allocate a new receiver and return its id
    fn new_observer_id(&mut self) -> usize {
        let id = self.next_id;
        self.next_id += 1;
        // If the sender is dropped, there is no point in storing a new receiver.
        if self.writers > 0 {
            self.observers.insert(id, ReceiverInner::new());
        }
        id
    }
}

#[derive(Debug, Default)]
struct ReceiverInner {
    received: bool,
    waker: Option<Waker>,
}

impl ReceiverInner {
    fn new() -> Self {
        Self {
            received: false,
            waker: None,
        }
    }
}
