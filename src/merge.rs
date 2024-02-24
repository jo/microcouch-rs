pub struct StemmedRevs {}

pub struct Tree {}

pub struct Conflicts {}

pub struct Path {}

pub struct MergeResult {
    pub tree: Tree,
    pub stemmed_revs: StemmedRevs,
    pub conflicts: Conflicts,
}

pub struct StemResult {
    pub tree: Tree,
    pub revs: StemmedRevs,
}

fn do_merge(tree: Tree, path: Path) -> MergeResult {
    MergeResult {
        tree,
        stemmed_revs: StemmedRevs {},
        conflicts: Conflicts {},
    }
}

fn stem(tree: Tree, depth: u32) -> StemResult {
    StemResult {
        tree,
        revs: StemmedRevs {},
    }
}

pub fn merge(tree: Tree, path: Path, depth: u32) -> MergeResult {
    let new_tree = do_merge(tree, path);
    let stemmed = stem(new_tree.tree, depth);

    MergeResult {
        tree: stemmed.tree,
        stemmed_revs: stemmed.revs,
        conflicts: new_tree.conflicts,
    }
}
