use libipld::{
    cbor::{
        decode::read_u8,
        DagCborCodec,
    },
    codec::{Decode, Encode},
};
use maplit::btreeset;
use reduce::Reduce;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use smol_str::SmolStr;
use std::{
    cmp::Ord,
    collections::BTreeSet,
    ops::{BitAnd, BitOr},
};
use vec_collections::{vecset, VecSet};
/// An index set is a set of u32 indices into the string table that will not allocate for up to 4 indices.
/// The size of a non-spilled IndexSet is 32 bytes on 64 bit architectures, so just 8 bytes more than a Vec.
pub type Tag = smol_str::SmolStr;
pub type IndexSet = VecSet<[u32; 4]>;
pub type TagSet = VecSet<[Tag; 4]>;

/// a compact index
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagIndex {
    /// the strings table
    pub(crate) tags: TagSet,
    /// indices in these sets are guaranteed to correspond to strings in the strings table
    pub(crate) elements: Vec<IndexSet>,
}

impl Encode<DagCborCodec> for TagIndex {
    fn encode<W: std::io::Write>(&self, c: DagCborCodec, w: &mut W) -> anyhow::Result<()> {
        // allocates like crazy
        let tags: Vec<String> = self.tags.iter().map(|x| x.to_string()).collect();
        let elements: Vec<Vec<u32>> = self
            .elements
            .iter()
            .map(|e| e.iter().cloned().collect())
            .collect();
        // a bit low level since libipld does not have tuple support yet
        w.write_all(&[0x82])?;
        tags.encode(c, w)?;
        elements.encode(c, w)?;
        Ok(())
    }
}

impl Decode<DagCborCodec> for TagIndex {
    fn decode<R: std::io::Read + std::io::Seek>(
        c: DagCborCodec,
        r: &mut R,
    ) -> anyhow::Result<Self> {
        // a bit low level since libipld does not have tuple support yet
        anyhow::ensure!(read_u8(r)? == 0x82);
        let tags: Vec<String> = Decode::decode(c, r)?;
        let elements: Vec<Vec<u32>> = Decode::decode(c, r)?;
        // allocates like crazy
        let tags: VecSet<[SmolStr; 4]> = tags.into_iter().map(SmolStr::new).collect();
        let elements: Vec<VecSet<[u32; 4]>> = elements
            .into_iter()
            .map(|i| i.into_iter().collect())
            .collect();
        for s in &elements {
            for x in s {
                anyhow::ensure!((*x as usize) < tags.len(), "invalid string index");
            }
        }
        Ok(Self { tags, elements })
    }
}

impl Serialize for TagIndex {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        // serialize as a tuple so it is guaranteed that the strings table is before the indices,
        // in case we ever want to write a clever visitor that matches without building an AST
        // of the deserialized result.
        (&self.tags, &self.elements).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for TagIndex {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let (tags, elements) = <(TagSet, Vec<IndexSet>)>::deserialize(deserializer)?;
        // ensure valid indices
        for s in &elements {
            for x in s {
                if *x as usize >= tags.len() {
                    return Err(serde::de::Error::custom("invalid string index"));
                }
            }
        }
        Ok(Self { tags, elements })
    }
}

/// Lookup all tags in a TagSet `tags` in a TagSet `table`.
/// If all lookups are successful, translate into a set of indices into the `table`
/// If any lookup is unsuccessful, return None
pub fn map_to_index_set(table: &TagSet, tags: &TagSet) -> Option<IndexSet> {
    tags.iter()
        .map(|t| table.as_ref().binary_search(t).ok().map(|x| x as u32))
        .collect::<Option<_>>()
}

impl TagIndex {
    /// given a query expression in Dnf form, returns all matching indices
    pub fn matching(&self, query: Dnf) -> Vec<usize> {
        // lookup all strings and translate them into indices.
        // if a single index does not match, the query can not match at all.
        let lookup = |s: &TagSet| -> Option<IndexSet> {
            s.iter()
                .map(|t| self.tags.as_ref().binary_search(t).ok().map(|x| x as u32))
                .collect::<Option<_>>()
        };
        // translate the query from strings to indices
        let query = query.0.iter().filter_map(lookup).collect::<Vec<_>>();
        // not a single query can possibly match, no need to iterate.
        if query.is_empty() {
            return Vec::new();
        }
        // check the remaining queries
        self.elements
            .iter()
            .enumerate()
            .filter_map(|(i, e)| {
                if query.iter().any(|x| x.is_subset(e)) {
                    Some(i)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn as_elements(&self) -> Vec<TagSet> {
        self.iter_elements().collect()
    }

    pub fn iter_elements(&self) -> impl Iterator<Item = TagSet> + '_ {
        self.elements.iter().map(move |is| {
            is.iter()
                .map(|i| self.tags.as_ref()[*i as usize].clone())
                .collect::<TagSet>()
        })
    }

    pub fn from_elements(e: &[TagSet]) -> Self {
        let mut tags = TagSet::default();
        for a in e.iter() {
            tags.extend(a.iter().cloned());
        }
        let elements = e
            .iter()
            .map(|a| {
                a.iter()
                    .map(|e| tags.as_ref().binary_search(e).unwrap() as u32)
                    .collect::<IndexSet>()
            })
            .collect::<Vec<_>>();
        Self { tags, elements }
    }

    pub fn get(&self, index: usize) -> Option<TagSet> {
        self.elements.get(index).map(|is| {
            is.iter()
                .map(|i| self.tags.as_ref()[*i as usize].clone())
                .collect()
        })
    }
}

/// a boolean expression, consisting of literals, union and intersection.
///
/// no attempt of simplification is made, except flattening identical operators.
///
/// `And([And([a,b]),c])` will be flattened to `And([a,b,c])`.
#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq)]
pub enum Expression {
    Literal(SmolStr),
    And(Vec<Expression>),
    Or(Vec<Expression>),
}

/// prints the expression with a minimum of brackets
impl std::fmt::Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        fn child_to_string(x: &Expression) -> String {
            match x {
                Expression::Or(v) if v.len() > 1 => format!("( {} )", x),
                _ => x.to_string(),
            }
        }
        write!(
            f,
            "{}",
            match self {
                Expression::Literal(text) => format!("'{}'", text.replace("'", "''")),
                Expression::And(es) => es
                    .iter()
                    .map(child_to_string)
                    .collect::<Vec<_>>()
                    .join(" & "),
                Expression::Or(es) => es
                    .iter()
                    .map(Expression::to_string)
                    .collect::<Vec<_>>()
                    .join(" | "),
            }
        )
    }
}

impl From<Dnf> for Expression {
    fn from(value: Dnf) -> Self {
        value.expression()
    }
}

impl Expression {
    pub fn literal(text: SmolStr) -> Self {
        Self::Literal(text)
    }

    pub fn or(e: Vec<Expression>) -> Self {
        Self::Or(
            e.into_iter()
                .flat_map(|c| match c {
                    Self::Or(es) => es,
                    x => vec![x],
                })
                .collect(),
        )
    }

    pub fn and(e: Vec<Expression>) -> Self {
        Self::And(
            e.into_iter()
                .flat_map(|c| match c {
                    Self::And(es) => es,
                    x => vec![x],
                })
                .collect(),
        )
    }

    pub fn simplify(&self) -> Self {
        match self {
            Expression::Literal(_) => self.clone(),
            Expression::And(v) => {
                let v = v
                    .iter()
                    .flat_map(|e| {
                        let e = e.simplify();
                        if let Expression::And(inner) = e {
                            inner
                        } else {
                            vec![e]
                        }
                    })
                    .collect::<Vec<_>>();
                if v.len() == 1 {
                    v.into_iter().next().unwrap()
                } else {
                    Expression::And(v)
                }
            }
            Expression::Or(v) => {
                let v = v
                    .iter()
                    .flat_map(|e| {
                        let e = e.simplify();
                        if let Expression::Or(inner) = e {
                            inner
                        } else {
                            vec![e]
                        }
                    })
                    .collect::<Vec<_>>();
                if v.len() == 1 {
                    v.into_iter().next().unwrap()
                } else {
                    Expression::Or(v)
                }
            }
        }
    }

    /// convert the expression into disjunctive normal form
    ///
    /// careful, for some expressions this can have exponential runtime. E.g. the disjunctive normal form
    /// of `(a | b) & (c | d) & (e | f) & ...` will be very complex.
    pub fn dnf(self) -> Dnf {
        match self {
            Expression::Literal(x) => Dnf::literal(x),
            Expression::Or(es) => es.into_iter().map(|x| x.dnf()).reduce(Dnf::bitor).unwrap(),
            Expression::And(es) => es.into_iter().map(|x| x.dnf()).reduce(Dnf::bitand).unwrap(),
        }
    }
}

impl BitOr for Expression {
    type Output = Expression;
    fn bitor(self, that: Self) -> Self {
        Expression::or(vec![self, that])
    }
}

impl BitAnd for Expression {
    type Output = Expression;
    fn bitand(self, that: Self) -> Self {
        Expression::and(vec![self, that])
    }
}
/// Disjunctive normal form of a boolean query expression
///
/// https://en.wikipedia.org/wiki/Disjunctive_normal_form
///
/// This is an unique represenation of a query using literals, union and intersection.
#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq)]
pub struct Dnf(pub BTreeSet<TagSet>);

impl Dnf {
    fn literal(text: SmolStr) -> Self {
        Self(btreeset![vecset! {text}])
    }

    /// converts the disjunctive normal form back to an expression
    pub fn expression(self) -> Expression {
        self.0
            .into_iter()
            .map(Dnf::and_expr)
            .reduce(Expression::bitor)
            .unwrap()
    }

    fn and_expr(v: TagSet) -> Expression {
        v.into_iter()
            .map(Expression::literal)
            .reduce(Expression::bitand)
            .unwrap()
    }
}

fn insert_unless_redundant(aa: &mut BTreeSet<TagSet>, b: TagSet) {
    let mut to_remove = None;
    for a in aa.iter() {
        if a.is_subset(&b) {
            // a is larger than b. E.g. x | x&y
            // keep a, b is redundant
            return;
        } else if a.is_superset(&b) {
            // a is smaller than b, E.g. x&y | x
            // remove a, keep b
            to_remove = Some(a.clone());
        }
    }
    if let Some(r) = to_remove {
        aa.remove(&r);
    }
    aa.insert(b);
}

impl From<Expression> for Dnf {
    fn from(value: Expression) -> Self {
        value.dnf()
    }
}

impl BitAnd for Dnf {
    type Output = Dnf;
    fn bitand(self, that: Self) -> Self {
        let mut rs = BTreeSet::new();
        for a in self.0.iter() {
            for b in that.0.iter() {
                let mut r = TagSet::default();
                r.extend(a.iter().cloned());
                r.extend(b.iter().cloned());
                insert_unless_redundant(&mut rs, r);
            }
        }
        Dnf(rs)
    }
}

impl BitOr for Dnf {
    type Output = Dnf;
    fn bitor(self, that: Self) -> Self {
        let mut rs = self.0;
        for b in that.0 {
            insert_unless_redundant(&mut rs, b);
        }
        Dnf(rs)
    }
}

#[cfg(test)]
macro_rules! tags {
    () => ({
        $crate::VecSet::default()
    });
    ($($x:expr),*$(,)*) => ({
        let mut set = TagSet::default();
        $(set.insert($x.into());)*
        set
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck::{quickcheck, Arbitrary, Gen};

    fn l(x: &str) -> Expression {
        Expression::literal(x.into())
    }

    #[test]
    fn test_dnf_intersection_1() {
        let a = l("a");
        let b = l("b");
        let c = l("c");
        let expr = c & (a | b);
        let c = expr.dnf().expression().to_string();
        assert_eq!(c, "'a' & 'c' | 'b' & 'c'");
    }

    #[test]
    fn test_dnf_intersection_2() {
        let a = l("a");
        let b = l("b");
        let c = l("c");
        let d = l("d");
        let expr = (d | c) & (b | a);
        let c = expr.dnf().expression().to_string();
        assert_eq!(c, "'a' & 'c' | 'a' & 'd' | 'b' & 'c' | 'b' & 'd'");
    }

    #[test]
    fn test_dnf_simplify_1() {
        let a = l("a");
        let b = l("b");
        let expr = (a.clone() | b) & a;
        let c = expr.dnf().expression().to_string();
        assert_eq!(c, "'a'");
    }

    #[test]
    fn test_dnf_simplify_2() {
        let a = l("a");
        let b = l("b");
        let expr = (a.clone() & b) | a;
        let c = expr.dnf().expression().to_string();
        assert_eq!(c, "'a'");
    }

    #[test]
    fn test_dnf_simplify_3() {
        let a = l("a");
        let b = l("b");
        let expr = (a.clone() | b) | a;
        let c = expr.dnf().expression().to_string();
        assert_eq!(c, "'a' | 'b'");
    }

    #[test]
    fn test_matching_1() {
        let index = TagIndex::from_elements(&[
            tags! {"a"},
            tags! {"a", "b"},
            tags! {"a"},
            tags! {"a", "b"},
        ]);
        let expr = l("a") | l("b");
        assert_eq!(index.matching(expr.dnf()), vec![0, 1, 2, 3]);
        let expr = l("a") & l("b");
        assert_eq!(index.matching(expr.dnf()), vec![1, 3]);
        let expr = l("c") & l("d");
        assert!(index.matching(expr.dnf()).is_empty());
    }

    #[test]
    fn test_matching_2() {
        let index = TagIndex::from_elements(&[
            tags! {"a", "b"},
            tags! {"b", "c"},
            tags! {"c", "a"},
            tags! {"a", "b"},
        ]);
        let expr = l("a") | l("b") | l("c");
        assert_eq!(index.matching(expr.dnf()), vec![0, 1, 2, 3]);
        let expr = l("a") & l("b");
        assert_eq!(index.matching(expr.dnf()), vec![0, 3]);
        let expr = l("a") & l("b") & l("c");
        assert!(index.matching(expr.dnf()).is_empty());
    }

    #[test]
    fn test_deser_error() {
        // negative index - serde should catch this
        let e1 = r#"[["a","b"],[[0],[0,1],[0],[0,-1]]]"#;
        let x: std::result::Result<TagIndex, _> = serde_json::from_str(e1);
        assert!(x.is_err());

        // index too large - we must catch this in order to uphold the invariants of the index
        let e1 = r#"[["a","b"],[[0],[0,1],[0],[0,2]]]"#;
        let x: std::result::Result<TagIndex, _> = serde_json::from_str(e1);
        assert!(x.is_err());
    }

    const STRINGS: &[&str] = &["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"];

    #[derive(Clone, PartialOrd, Ord, PartialEq, Eq)]
    struct IndexString(&'static str);

    impl Arbitrary for IndexString {
        fn arbitrary(g: &mut Gen) -> Self {
            IndexString(g.choose(STRINGS).unwrap())
        }
    }

    impl Arbitrary for TagIndex {
        fn arbitrary(g: &mut Gen) -> Self {
            let xs: Vec<BTreeSet<IndexString>> = Arbitrary::arbitrary(g);
            let xs: Vec<TagSet> = xs
                .iter()
                .map(|e| e.iter().map(|x| x.0.into()).collect())
                .collect();
            Self::from_elements(&xs)
        }
    }

    quickcheck! {
        fn serde_json_roundtrip(index: TagIndex) -> bool {
            let json = serde_json::to_string(&index).unwrap();
            let index2: TagIndex = serde_json::from_str(&json).unwrap();
            index == index2
        }
    }
}
