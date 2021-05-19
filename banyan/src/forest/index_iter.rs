use super::{Forest, Secrets, TreeTypes};
use crate::{
    index::{CompactSeq, Index, NodeInfo},
    query::Query,
    store::ReadOnlyStore,
};
use anyhow::Result;
use smallvec::{smallvec, SmallVec};

#[derive(PartialEq)]
enum Mode {
    Forward,
    Backward,
}

pub(crate) struct IndexIter<T: TreeTypes, R, Q: Query<T>> {
    forest: Forest<T, R>,
    secrets: Secrets,
    offset: u64,
    query: Q,
    stack: SmallVec<[TraverseState<T>; 5]>,
    mode: Mode,
}

struct TraverseState<T: TreeTypes> {
    index: Index<T>,
    // If `index` points to a branch node, `position` points to the currently
    // traversed child
    position: isize,
    // For each child, indicates whether it should be visited or not. This is
    // initially empty, and initialized whenver we hit a branch.
    // Branches can not have zero children, so when this is empty we know that we have
    // to initialize it.
    filter: SmallVec<[bool; 64]>,
}

impl<T: TreeTypes> TraverseState<T> {
    fn new(index: Index<T>) -> Self {
        Self {
            index,
            position: 0,
            filter: smallvec![],
        }
    }
    fn is_exhausted(&self, mode: &Mode) -> bool {
        match mode {
            Mode::Forward => !self.filter.is_empty() && self.position >= self.filter.len() as isize,
            Mode::Backward => self.position < 0,
        }
    }
    fn next_pos(&mut self, mode: &Mode) {
        match mode {
            Mode::Forward => self.position += 1,
            Mode::Backward => self.position -= 1,
        }
    }
}

impl<T, R, Q> IndexIter<T, R, Q>
where
    T: TreeTypes,
    R: ReadOnlyStore<T::Link>,
    Q: Query<T> + Clone + Send + 'static,
{
    pub(crate) fn new(forest: Forest<T, R>, secrets: Secrets, query: Q, index: Index<T>) -> Self {
        let mode = Mode::Forward;
        let stack = smallvec![TraverseState::new(index)];

        Self {
            forest,
            secrets,
            offset: 0,
            query,
            stack,
            mode,
        }
    }
    pub(crate) fn new_rev(
        forest: Forest<T, R>,
        secrets: Secrets,
        query: Q,
        index: Index<T>,
    ) -> Self {
        let offset = index.count();
        let mode = Mode::Backward;
        let stack = smallvec![TraverseState::new(index)];

        Self {
            forest,
            secrets,
            offset,
            query,
            stack,
            mode,
        }
    }
}

impl<T, R, Q> Iterator for IndexIter<T, R, Q>
where
    T: TreeTypes,
    R: ReadOnlyStore<T::Link>,
    Q: Query<T> + Clone + Send + 'static,
{
    type Item = Result<Index<T>>;

    fn next(&mut self) -> Option<Self::Item> {
        let res = loop {
            let head = match self.stack.last_mut() {
                Some(i) => i,
                // Nothing to do ..
                _ => return None,
            };

            //  Branch is exhausted: Ascend.
            if head.is_exhausted(&self.mode) {
                // Ascend to parent's node
                self.stack.pop();

                // increase last stack ptr, if there is still something left to
                // traverse
                if let Some(last) = self.stack.last_mut() {
                    last.next_pos(&self.mode);
                }
                continue;
            }

            match self.forest.load_node(&self.secrets, &head.index) {
                NodeInfo::Branch(index, branch) => {
                    let branch = match branch.load_cached() {
                        Ok(branch) => branch,
                        Err(cause) => return Some(Err(cause)),
                    };
                    if head.filter.is_empty() {
                        // we hit this branch node for the first time. Apply the
                        // query on its children and store it
                        head.filter = smallvec![true; index.summaries.len()];
                        head.position = match self.mode {
                            Mode::Forward => 0,
                            Mode::Backward => branch.children.len() as isize - 1,
                        };
                        let start_offset = match self.mode {
                            Mode::Forward => self.offset,
                            Mode::Backward => self.offset - index.count,
                        };
                        self.query
                            .intersecting(start_offset, &index, &mut head.filter);
                        debug_assert_eq!(branch.children.len(), head.filter.len());

                        break head.index.clone();
                    }

                    let next_idx = head.position as usize;
                    if head.filter[next_idx] {
                        // Descend into next child
                        self.stack
                            .push(TraverseState::new(branch.children[next_idx].clone()));
                        continue;
                    } else {
                        let index = &branch.children[next_idx];
                        match self.mode {
                            Mode::Forward => {
                                self.offset += index.count();
                            }
                            Mode::Backward => {
                                self.offset -= index.count();
                            }
                        }
                        head.next_pos(&self.mode);
                    }
                }

                NodeInfo::Leaf(index, _) => {
                    match self.mode {
                        Mode::Forward => {
                            self.offset += index.keys.count();
                        }
                        Mode::Backward => {
                            self.offset -= index.keys.count();
                        }
                    }
                    // Ascend to parent's node, if it exists
                    let this_index = self.stack.pop().expect("not empty").index;
                    if let Some(last) = self.stack.last_mut() {
                        last.next_pos(&self.mode);
                    }
                    break this_index;
                }

                // even for purged leafs and branches or ignored chunks,
                // produce a placeholder.
                _ => {
                    let TraverseState { index, .. } = self.stack.pop().expect("not empty");
                    // Ascend to parent's node. This might be none in case the
                    // tree's root node is a `PurgedBranch`.
                    if let Some(last) = self.stack.last_mut() {
                        last.next_pos(&self.mode);
                    };
                    match self.mode {
                        Mode::Forward => {
                            self.offset += index.count();
                        }
                        Mode::Backward => {
                            self.offset -= index.count();
                        }
                    };
                    break index;
                }
            };
        };
        Some(Ok(res))
    }
}
