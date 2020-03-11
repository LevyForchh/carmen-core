#![allow(dead_code)]
use ordered_float::OrderedFloat;
use std::borrow::Borrow;
use std::cmp::Reverse;
use std::collections::HashSet;
use std::fmt::Debug;

use crate::gridstore::builder::*;
use crate::gridstore::common::MatchPhrase::Range;
use crate::gridstore::common::*;
use crate::gridstore::store::*;

#[derive(Debug, Clone)]
pub struct StackableNode<T: Borrow<GridStore> + Clone + Debug> {
    pub phrasematch: Option<PhrasematchResults<T>>,
    pub children: Vec<StackableNode<T>>,
    pub nmask: u32,
    pub bmask: HashSet<u32>,
    pub mask: u32,
    pub idx: u32,
    pub max_relev: f64,
    pub adjusted_relev: f64,
    pub zoom: u16,
}

//tree traversal used only for tests
pub fn bfs<T: Borrow<GridStore> + Clone + Debug>(root: StackableNode<T>) -> Vec<StackableNode<T>> {
    let mut node_vec: Vec<StackableNode<T>> = vec![];
    let mut stack: Vec<_> = vec![];

    stack.push(root);

    while stack.len() > 0 {
        let node = stack.pop().unwrap();
        node_vec.push(node.clone());
        for child in node.children {
            stack.push(child);
        }
    }
    return node_vec;
}

pub fn stackable<'a, T: Borrow<GridStore> + Clone + Debug>(
    phrasematch_results: &Vec<Vec<PhrasematchResults<T>>>,
    phrasematch_result: Option<PhrasematchResults<T>>,
    nmask: u32,
    bmask: HashSet<u32>,
    mask: u32,
    idx: u32,
    max_relev: f64,
    adjusted_relev: f64,
    zoom: u16,
) -> StackableNode<T> {
    let mut node = StackableNode {
        phrasematch: phrasematch_result,
        children: vec![],
        mask: mask,
        bmask: bmask,
        nmask: nmask,
        idx: idx,
        max_relev: max_relev,
        adjusted_relev: adjusted_relev,
        zoom: zoom,
    };

    for phrasematch_per_index in phrasematch_results.iter() {
        for phrasematches in phrasematch_per_index.iter() {
            if node.phrasematch.is_some() {
                if node.zoom > phrasematches.zoom {
                    continue;
                } else if node.zoom == phrasematches.zoom {
                    if node.idx > phrasematches.idx {
                        continue;
                    }
                }
            }

            if (node.nmask & phrasematches.nmask) == 0
                && (node.mask & phrasematches.mask) == 0
                && phrasematches.bmask.contains(&node.idx) == false
            {
                let target_nmask = &phrasematches.nmask | node.nmask;
                let target_mask = &phrasematches.mask | node.mask;
                let mut target_bmask: HashSet<u32> = node.bmask.iter().cloned().collect();
                let phrasematch_bmask: HashSet<u32> = phrasematches.bmask.iter().cloned().collect();
                target_bmask.extend(&phrasematch_bmask);
                let target_relev = 0.0 + &phrasematches.weight;
                let target_adjusted_relev =
                    node.adjusted_relev + (&phrasematches.weight * &phrasematches.edit_multiplier);

                node.children.push(stackable(
                    &phrasematch_results,
                    Some(phrasematches.clone()),
                    target_nmask,
                    target_bmask,
                    target_mask,
                    phrasematches.idx,
                    target_relev,
                    target_adjusted_relev,
                    phrasematches.zoom,
                ));
            }
        }
    }

    node.children.sort_by_key(|node| Reverse(OrderedFloat(node.max_relev)));

    if !node.children.is_empty() {
        node.max_relev = node.max_relev + node.children[0].max_relev;
    }

    node
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn simple_stackable_test() {
        let directory: tempfile::TempDir = tempfile::tempdir().unwrap();
        let mut builder = GridStoreBuilder::new(directory.path()).unwrap();

        let key = GridKey { phrase_id: 1, lang_set: 1 };

        let entries = vec![
            GridEntry { id: 2, x: 2, y: 2, relev: 0.8, score: 3, source_phrase_hash: 0 },
            GridEntry { id: 3, x: 3, y: 3, relev: 1., score: 1, source_phrase_hash: 1 },
            GridEntry { id: 1, x: 1, y: 1, relev: 1., score: 7, source_phrase_hash: 2 },
        ];
        builder.insert(&key, entries).expect("Unable to insert record");
        builder.finish().unwrap();
        let store = GridStore::new(directory.path()).unwrap();

        let a1 = PhrasematchResults {
            store: &store,
            scorefactor: 0,
            prefix: 0,
            weight: 0.5,
            match_key: MatchKey { match_phrase: Range { start: 0, end: 1 }, lang_set: 0 },
            idx: 0,
            zoom: 0,
            nmask: 0,
            mask: 2,
            bmask: HashSet::new(),
            edit_multiplier: 1.0,
            subquery_edit_distance: 0,
        };

        let b1 = PhrasematchResults {
            store: &store,
            scorefactor: 0,
            prefix: 0,
            weight: 0.5,
            match_key: MatchKey { match_phrase: Range { start: 0, end: 1 }, lang_set: 0 },
            idx: 1,
            zoom: 1,
            nmask: 1,
            mask: 1,
            bmask: HashSet::new(),
            edit_multiplier: 1.0,
            subquery_edit_distance: 0,
        };

        let b2 = PhrasematchResults {
            store: &store,
            scorefactor: 0,
            prefix: 0,
            weight: 0.5,
            match_key: MatchKey { match_phrase: Range { start: 0, end: 1 }, lang_set: 0 },
            idx: 1,
            zoom: 6,
            nmask: 1,
            mask: 1,
            bmask: HashSet::new(),
            edit_multiplier: 1.0,
            subquery_edit_distance: 0,
        };

        let phrasematch_results = vec![vec![a1, b1, b2]];
        let tree = stackable(&phrasematch_results, None, 0, HashSet::new(), 0, 129, 0.0, 0.0, 0);
        let result_ids: Vec<u32> = bfs(tree).iter().map(|node| node.idx).collect();
        assert_eq!(
            result_ids,
            [129, 1, 1, 0, 1, 1],
            "idx stacked in the right order, no duplicate branches"
        );
        let tree = stackable(&phrasematch_results, None, 0, HashSet::new(), 0, 129, 0.0, 0.0, 0);
        let result_max_relev: Vec<f64> = bfs(tree).iter().map(|node| node.max_relev).collect();
        assert_eq!(
            result_max_relev,
            [1.0, 0.5, 0.5, 1.0, 0.5, 0.5],
            "max_relev for parent nodes is the summation of it's relevance and max of the children"
        );
    }
}
