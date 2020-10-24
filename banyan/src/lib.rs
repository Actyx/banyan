//! # Banyan trees
//!
//! ![](https://i.imgur.com/6icLbdz.jpg)
//!
//! Banyan trees in nature are the trees with the widest canopy. They also sprout new roots, which after some time can become indistinguishable from the original root.
//!
//! The banyan trees in this library are persistent trees with a high branching factor (wide canopy) that can be used to efficiently store and query large sequences of key value pairs.
//!
//! Banyan trees are optimized for *appending* new entries and filtered, streaming reading of values. They naturally support addressing elements by index.
//! They **do not** support arbitrary insertion or deletion of elements, so they are **not** a general purpose map data structure.
//!
//! They are not balanced trees, but they share some properties with [B-Trees].
//!
//! ## Persistence
//!
//! Banyan trees are persistent, using a content-addressed storage system such as [ipfs] or a key value store.
//! Data is [CBOR] encoded and [zstd] compressed for space efficient persistent storage and replication. It is also encrypted using the [salsa20] stream cipher.
//!
//! # Indexing
//!
//! Each banyan tree entry consists of a key part and a value part.
//! The value part is an opaque value that can be serialized as [cbor]. This can not be used for filtering in queries.
//! The key part can be used for efficient querying by defining how to compute summaries from keys.
//! These summaries will be stored higher up in the tree to allow for efficient querying and filtered streaming.
//!
//! In addition to querying by key content, elements can always be efficiently accessed by offset.
//!
//! # Sealing and packing
//!
//! ## Sealing
//!
//! A leaf node of the tree is considered *sealed* as soon as the compressed data exceeds a certain size.
//! This depends on configuration and compression rate.
//! A branch node is considered *sealed* as soon as it has a certain, configurable number of child nodes.
//!
//! ## Packing
//!
//! Banyan trees are not balanced, since they do not have to support arbitrary insertion and deletion.
//! There is a concept somewhat related to balancing called packing. A tree is considered packed
//! if all but the last leaf node is packed, and branch nodes are packed to the left as much as possible.
//! This is usually the most space efficient representation of the sequence of key value pairs, and also
//! the most efficient one for subsequent appending.
//!
//! ## Unpacked trees
//!
//! Packing a tree after each append of a single entry or a small number of entries would be inefficient, since
//! it would involve recreating a new branch node for each level and a leaf node.
//!
//! To allow for quick append, it is possible to add elements without packing. This creates a linked list of trees,
//! and in the case of adding individual elements, a degenerate tree resembling a linked list.
//!
//! Unpacked trees can be packed at any time without changing the content.
//!
//! [CBOR]: https://en.wikipedia.org/wiki/CBOR
//! [zstd]: https://en.wikipedia.org/wiki/Zstandard
//! [salsa20]: https://en.wikipedia.org/wiki/Salsa20
//! [ipfs]: https://ipfs.io/
//! [B-Trees]: https://en.wikipedia.org/wiki/B-tree
pub mod forest;
pub mod index;
pub mod memstore;
pub mod query;
pub mod store;
mod thread_local_zstd;
pub mod tree;
mod util;
mod zstd_array;

#[cfg(test)]
extern crate quickcheck;
#[cfg(test)]
#[macro_use(quickcheck)]
extern crate quickcheck_macros;
