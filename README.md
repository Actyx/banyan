# Banyan

![](https://i.imgur.com/6icLbdz.jpg)

Banyan trees are the trees with the widest canopy. They also sprout new roots, which after some time can become indistinguishable from the original root. Seems appropriate.

# Design goals

- fat leafs, containing as much data as possible and being roughly the same size
- low depth, fat branches
- quick traversal and random access
- cheap append
- no insert/splice
- possibility for removal

# Leaf builder

Leafs are the biggest things. The builder incrementally encodes, compresses and encrypts data. The state of a builder should *at any time* be able to be persisted. The storage format should be also incremental.

![](https://i.imgur.com/NTlQTBv.jpg)

# Branch builder

Whereas leafs are always appended to, branches are most of the time updated at their last index.

![](https://i.imgur.com/aWKkcsR.jpg)

# Tree structure

The tree should have a structure that is fully determined by the content, unlike e.g. a balanced tree which has a memory of its history.

It should support append.

![](https://i.imgur.com/CJdliPU.jpg)

# Node structure

A node is split into two parts. An index part that is 

![](https://i.imgur.com/ZZHqMRS.jpg)

