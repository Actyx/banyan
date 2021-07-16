# Banyan &emsp; [![Build Status]][travis] [![Latest Version]][crates.io] [![Docs Badge]][docs.rs]

[Build Status]: https://api.travis-ci.org/actyx/banyan.svg?branch=master
[travis]: https://travis-ci.org/actyx/banyan
[Latest Version]: https://img.shields.io/crates/v/banyan.svg
[crates.io]: https://crates.io/crates/banyan
[Docs Badge]: https://img.shields.io/badge/docs-docs.rs-green
[docs.rs]: https://docs.rs/banyan

![banyan tree](https://ipfs.io/ipfs/QmaEDTjHSdCKyGQ3cFMCf73kE67NvffLA5agquLW5qSEVn/banyan.jpg)

Banyan trees are the trees with the widest canopy. They also sprout new roots, which after some time can become indistinguishable from the original root. Seems appropriate.

# License

[Apache2](LICENSE-APACHE2.md) or [MIT](LICENSE-MIT.md)

# Design goals

- fat leafs, containing as much data as possible and being roughly the same size
- low depth, fat branches
- quick traversal, filtering and random access
- ~cheap append~ (possible, but would require more complex interaction with block store)
- no insert/splice
- possibility for removal

# Tree structure

The tree should have a structure that is fully determined by the content, unlike e.g. a balanced tree which has a memory of its history.

It should support append and bulk append.

![](https://i.imgur.com/CJdliPU.jpg)

# Node structure

A node is split into two parts. An index part that lives in the parent for quick access, and a value part that is linked from the parent.

![](https://i.imgur.com/ZZHqMRS.jpg)

# Leaf builder

Leafs are the biggest things. The builder incrementally encodes and compresses data. The state of a builder should *at any time* be able to be persisted. The storage format should be also incremental (adding stuff at the end does not require changes at the start)

![](https://i.imgur.com/NTlQTBv.jpg)

# Branch builder

Whereas leafs are always appended to, branches are most of the time updated at their last index. Branches are stored as valid IPLD so they can be traversed by ipfs. The non-link data is zstd compressed cbor.

![](https://i.imgur.com/aWKkcsR.jpg)
