use core::fmt;
use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

use crate::{
    forest::{Config, Secrets, TreeTypes},
    index::Index,
    tree::Tree,
};

/// A thing that hands out unique offsets. Parts of StreamBuilderState
///
/// these are the byte offsets for the stream cipher and not related to the event offsets
/// inside the tree.
#[derive(Debug, Clone)]
pub(crate) struct CipherOffset {
    value: u64,
}

impl CipherOffset {
    pub fn new(value: u64) -> Self {
        Self { value }
    }

    pub fn as_u64(&self) -> u64 {
        self.value
    }

    pub fn reserve(&mut self, n: usize) -> u64 {
        let result = self.value;
        self.value = self
            .value
            .checked_add(n as u64)
            .expect("ran out of offsets");
        result
    }

    pub fn current(&self) -> u64 {
        self.value
    }
}

#[derive(Debug)]
pub(crate) struct StreamBuilderState {
    /// the secrets used for building the trees of the stream
    secrets: Secrets,
    /// tree config that determines the branch size etc.
    config: Config,
    /// current stream offset
    ///
    /// this is the first free offset, or the total number of bytes ever written
    /// on this stream.
    pub(crate) offset: CipherOffset,
}

impl StreamBuilderState {
    pub fn new(offset: u64, secrets: Secrets, config: Config) -> Self {
        Self {
            offset: CipherOffset::new(offset),
            secrets,
            config,
        }
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn secrets(&self) -> &Secrets {
        &self.secrets
    }

    pub fn value_key(&self) -> &chacha20::Key {
        self.secrets.value_key()
    }

    pub fn index_key(&self) -> &chacha20::Key {
        self.secrets.index_key()
    }
}

/// A builder for a stream of trees
///
/// Most of the logic except for handling the empty case is implemented in the forest
pub struct StreamBuilder<T: TreeTypes, V> {
    root: Option<Index<T>>,
    state: StreamBuilderState,
    _p: PhantomData<V>,
}

impl<T: TreeTypes, V> fmt::Debug for StreamBuilder<T, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.root {
            Some(root) => f
                .debug_struct("Tree")
                .field("count", &self.count())
                .field("key_bytes", &root.key_bytes())
                .field("value_bytes", &root.value_bytes())
                .field("link", &root.link())
                .finish(),
            None => f
                .debug_struct("Tree")
                .field("count", &self.count())
                .finish(),
        }
    }
}

impl<T: TreeTypes, V> fmt::Display for StreamBuilder<T, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.root {
            Some(root) => write!(f, "{:?}", root.link(),),
            None => write!(f, "empty tree"),
        }
    }
}

impl<T: TreeTypes, V> StreamBuilder<T, V> {
    pub fn new(config: Config, secrets: Secrets) -> Self {
        let state = StreamBuilderState::new(0, secrets, config);
        Self::new_from_index(None, state)
    }

    /// Creates a stream builder with debug settings.
    ///
    /// The tree config will be the debug config that produces "interesting" trees even with a small
    /// number of elements.
    ///
    /// The secrets will be the default secrets
    ///
    /// Do not use this in production!
    pub fn debug() -> Self {
        let state = StreamBuilderState::new(0, Secrets::default(), Config::debug());
        Self::new_from_index(None, state)
    }

    pub fn snapshot(&self) -> Tree<T, V> {
        self.root
            .as_ref()
            .map(|root| {
                Tree::new(
                    root.clone(),
                    self.state.secrets().clone(),
                    self.state.offset.current(),
                )
            })
            .unwrap_or_default()
    }

    pub fn link(&self) -> Option<(u64, u64)> {
        self.root.as_ref().and_then(|r| *r.link())
    }

    pub fn as_index_ref(&self) -> Option<&Index<T>> {
        self.root.as_ref()
    }

    pub fn level(&self) -> i32 {
        self.root.as_ref().map(|x| x.level() as i32).unwrap_or(-1)
    }

    /// true for an empty tree
    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }

    /// number of elements in the tree
    pub fn count(&self) -> u64 {
        self.root.as_ref().map(|x| x.count()).unwrap_or_default()
    }

    /// root of a non-empty tree
    pub fn root(&self) -> Option<&(u64, u64)> {
        self.root.as_ref().and_then(|index| index.link().as_ref())
    }

    /// root of a non-empty tree
    pub fn index(&self) -> Option<&Index<T>> {
        self.root.as_ref()
    }

    /// Modify a StreamBuilder and roll back the changes if the operation was not successful
    ///
    /// Note that consumed offets are *not* rolled back to make sure we don't reuse offsets.
    pub fn transaction(&mut self) -> StreamTransaction<'_, T, V> {
        StreamTransaction::new(self, self.index().cloned())
    }

    pub(crate) fn state(&self) -> &StreamBuilderState {
        &self.state
    }

    pub(crate) fn state_mut(&mut self) -> &mut StreamBuilderState {
        &mut self.state
    }

    pub(crate) fn new_from_index(root: Option<Index<T>>, state: StreamBuilderState) -> Self {
        Self {
            root,
            state,
            _p: PhantomData,
        }
    }

    pub(crate) fn set_index(&mut self, index: Option<Index<T>>) {
        self.root = index
    }
}

pub struct StreamTransaction<'a, T: TreeTypes, V> {
    builder: &'a mut StreamBuilder<T, V>,
    restore: Option<Option<Index<T>>>,
}

impl<'a, T: TreeTypes, V> StreamTransaction<'a, T, V> {
    fn new(builder: &'a mut StreamBuilder<T, V>, index: Option<Index<T>>) -> Self {
        Self {
            builder,
            restore: Some(index),
        }
    }

    pub fn commit(mut self) {
        self.restore.take();
    }
}

impl<'a, T: TreeTypes, V> Deref for StreamTransaction<'a, T, V> {
    type Target = StreamBuilder<T, V>;

    fn deref(&self) -> &Self::Target {
        &self.builder
    }
}

impl<'a, T: TreeTypes, V> DerefMut for StreamTransaction<'a, T, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.builder
    }
}

impl<'a, T: TreeTypes, V> Drop for StreamTransaction<'a, T, V> {
    fn drop(&mut self) {
        if let Some(index) = self.restore.take() {
            self.builder.set_index(index);
        }
    }
}
