fn offset(index: u64) -> u64 {
    index * 2 - (index.count_ones() as u64)
}

fn roots0(index: u64, size: u64, res: &mut Vec<u64>) {
    if index < size {
        res.push(index);
    } else {
        for child in left_child(index) {
            roots0(child, size, res);
        }
        for child in right_child(index) {
            roots0(child, size, res);
        }
    }
}

fn roots(size: u64) -> Vec<u64> {
    // traverse the imaginary tree from a guesstimated full root and abort as soon as the child index is within bounds
    let mut res = Vec::new();
    let index = all_one(size + 1) - 1;
    roots0(index, size, &mut res);
    res
}

fn all_one(x: u64) -> u64 {
    if x != 0 {
        u64::MAX >> x.leading_zeros()
    } else {
        0
    }
}

fn info(o: u64) -> (u64, u64) {
    let mut k = o; // todo: figure out lower bound to start the iteration
    loop {
        let o_k = offset(k);
        if o_k <= o {
            return (k, o - o_k);
        }
        k -= 1;
    }
}

fn level(o: u64) -> u64 {
    info(o).1
}

fn is_leaf(o: u64) -> bool {
    level(o) == 0
}

fn left_child(offset: u64) -> Option<u64> {
    let (_, level) = info(offset);
    if level > 0 {
        let leafs = 1 << (level - 1);
        let branches = leafs - 1;
        Some(offset - leafs - branches - 1)
    } else {
        None
    }
}

fn right_child(offset: u64) -> Option<u64> {
    let (_, level) = info(offset);
    if level > 0 {
        Some(offset - 1)
    } else {
        None
    }
}

pub fn test() {
    let mut res = Vec::<String>::new();
    for i in 0..100 {
        res.push("i".into());
    }
    for i in 0..30 {
        res[offset(i) as usize] = i.to_string()
    }
    println!("{:?}", res);
    for i in 0..50 {
        println!(
            "{}\t|{}\t{}\t{}\t{}\t{:?}",
            i,
            res[i],
            level(i as u64),
            left_child(i as u64)
                .map(|x| x.to_string())
                .unwrap_or_default(),
            right_child(i as u64)
                .map(|x| x.to_string())
                .unwrap_or_default(),
            roots(i as u64 + 1),
        );
    }
}
