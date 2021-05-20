use core::fmt::Debug;
use std::collections::BTreeMap;

use banyan::{
    store::{ReadOnlyStore, ZstdDagCborSeq},
    Tree, {Forest, TreeTypes},
};
use libipld::{cbor::DagCbor, codec::Codec, json::DagJsonCodec};

type Node<'a> = &'a NodeDescriptor;
type Edge<'a> = (usize, usize);
struct ForestWithTree {
    nodes: BTreeMap<usize, NodeDescriptor>,
    edges: Vec<(usize, usize)>,
}

#[allow(dead_code)]
enum NodeDescriptor {
    Branch {
        level: usize,
        id: usize,
        sealed: bool,
    },
    Leaf {
        id: usize,
        items: u32,
        bytes: u64,
        sealed: bool,
    },
}

impl<'a> dot::Labeller<'a, Node<'a>, Edge<'a>> for ForestWithTree {
    fn graph_id(&'a self) -> dot::Id<'a> {
        dot::Id::new("thetree").unwrap()
    }

    fn node_id(&'a self, n: &&'a NodeDescriptor) -> dot::Id<'a> {
        let id = match n {
            NodeDescriptor::Branch { id, .. } => id,
            NodeDescriptor::Leaf { id, .. } => id,
        };
        dot::Id::new(format!("N{}", id)).unwrap()
    }
    fn node_label(&'a self, n: &&'a NodeDescriptor) -> dot::LabelText<'a> {
        let id = match n {
            NodeDescriptor::Branch { id, .. } => id.to_string(),
            NodeDescriptor::Leaf { items, .. } => items.to_string(),
        };
        dot::LabelText::label(id)
    }

    fn node_shape(&'a self, n: &Node<'a>) -> Option<dot::LabelText<'a>> {
        let id = match n {
            NodeDescriptor::Branch { .. } => "box",
            NodeDescriptor::Leaf { .. } => "circle",
        };
        Some(dot::LabelText::label(id))
    }

    fn node_color(&'a self, n: &Node<'a>) -> Option<dot::LabelText<'a>> {
        let sealed = match n {
            NodeDescriptor::Branch { sealed, .. } => sealed,
            NodeDescriptor::Leaf { sealed, .. } => sealed,
        };
        if *sealed {
            Some(dot::LabelText::label("grey"))
        } else {
            None
        }
    }

    fn node_style(&'a self, n: &Node<'a>) -> dot::Style {
        let sealed = match n {
            NodeDescriptor::Branch { sealed, .. } => sealed,
            NodeDescriptor::Leaf { sealed, .. } => sealed,
        };
        if *sealed {
            dot::Style::Filled
        } else {
            dot::Style::None
        }
    }
}

impl<'a> dot::GraphWalk<'a, Node<'a>, Edge<'a>> for ForestWithTree {
    fn nodes(&'a self) -> dot::Nodes<'a, Node<'a>> {
        self.nodes.values().collect()
    }

    fn edges(&'a self) -> dot::Edges<'a, Edge<'a>> {
        self.edges.iter().cloned().collect()
    }

    fn source(&'a self, edge: &Edge<'a>) -> Node<'a> {
        &self.nodes[&edge.0]
    }

    fn target(&'a self, edge: &Edge<'a>) -> Node<'a> {
        &self.nodes[&edge.1]
    }
}

pub fn graph<TT, V, R>(
    forest: &Forest<TT, R>,
    tree: &Tree<TT, V>,
    mut out: impl std::io::Write,
) -> anyhow::Result<()>
where
    TT: TreeTypes,
    V: Clone + Send + Sync + Debug + DagCbor + 'static,
    R: ReadOnlyStore<TT::Link> + Clone + Send + Sync + 'static,
{
    let (edges, nodes) = forest.dump_graph(&tree, |(id, node)| match node {
        banyan::index::NodeInfo::Branch(idx, _) | banyan::index::NodeInfo::PurgedBranch(idx) => {
            NodeDescriptor::Branch {
                level: idx.level as usize,
                id,
                sealed: idx.sealed,
            }
        }
        banyan::index::NodeInfo::Leaf(idx, _) | banyan::index::NodeInfo::PurgedLeaf(idx) => {
            NodeDescriptor::Leaf {
                id,
                items: idx.keys().fold(0, |acc, _| acc + 1),
                bytes: idx.value_bytes,
                sealed: idx.sealed,
            }
        }
    })?;
    let forest_with_tree = ForestWithTree { nodes, edges };
    dot::render(&forest_with_tree, &mut out)?;
    Ok(())
}

/// Takes a hash to a dagcbor encoded blob, and write it as dag-json to `writer`,
/// each item separated by newlines.
pub fn dump_json<Link: 'static>(
    store: impl ReadOnlyStore<Link>,
    hash: Link,
    value_key: chacha20::Key,
    mut writer: impl std::io::Write,
) -> anyhow::Result<()> {
    let bytes = store.get(&hash)?;
    let (dag_cbor, _) = ZstdDagCborSeq::decrypt(&bytes, &value_key)?;
    let ipld_ast = dag_cbor.items::<libipld::Ipld>()?;
    for x in ipld_ast {
        let json = DagJsonCodec.encode(&x)?;
        writeln!(writer, "{}", std::str::from_utf8(&json)?)?;
    }
    Ok(())
}
