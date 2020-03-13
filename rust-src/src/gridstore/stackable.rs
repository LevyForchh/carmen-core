#![allow(dead_code)]
use ordered_float::OrderedFloat;
use std::borrow::Borrow;
use std::cmp::Reverse;
use std::collections::HashSet;
use std::fmt::Debug;

use crate::gridstore::common::*;
use crate::gridstore::store::*;

#[derive(Debug, Clone)]
pub struct StackableNode<T: Borrow<GridStore> + Clone + Debug> {
    pub phrasematch: Option<PhrasematchSubquery<T>>,
    pub children: Vec<StackableNode<T>>,
    pub nmask: u32,
    pub bmask: HashSet<u16>,
    pub mask: u32,
    pub idx: u16,
    pub max_relev: f64,
    pub zoom: u16,
}

pub fn stackable<'a, T: Borrow<GridStore> + Clone + Debug>(
    phrasematch_results: &Vec<Vec<PhrasematchSubquery<T>>>,
    phrasematch_result: Option<PhrasematchSubquery<T>>,
    nmask: u32,
    bmask: HashSet<u16>,
    mask: u32,
    idx: u16,
    max_relev: f64,
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
        zoom: zoom,
    };

    for phrasematch_per_index in phrasematch_results.iter() {
        for phrasematches in phrasematch_per_index.iter() {
            if node.phrasematch.is_some() {
                if node.zoom > phrasematches.store.borrow().zoom {
                    continue;
                } else if node.zoom == phrasematches.store.borrow().zoom {
                    if node.idx > phrasematches.store.borrow().idx {
                        continue;
                    }
                }
            }

            if (node.nmask & (1u32 << phrasematches.store.borrow().type_id)) == 0
                && (node.mask & phrasematches.mask) == 0
                && phrasematches.store.borrow().non_overlapping_indexes.contains(&node.idx) == false
            {
                let target_nmask = &(1u32 << phrasematches.store.borrow().type_id) | node.nmask;
                let target_mask = &phrasematches.mask | node.mask;
                let mut target_bmask: HashSet<u16> = node.bmask.iter().cloned().collect();
                let phrasematch_bmask: HashSet<u16> =
                    phrasematches.store.borrow().non_overlapping_indexes.iter().cloned().collect();
                target_bmask.extend(&phrasematch_bmask);
                let target_relev = 0.0 + &phrasematches.weight;

                node.children.push(stackable(
                    &phrasematch_results,
                    Some(phrasematches.clone()),
                    target_nmask,
                    target_bmask,
                    target_mask,
                    phrasematches.store.borrow().idx,
                    target_relev,
                    phrasematches.store.borrow().zoom,
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
    fn stackable_test() {
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

        let a1 = PhrasematchSubquery {
            id: 0,
            store: &store,
            weight: 0.5,
            match_key: MatchKey { match_phrase: Range { start: 0, end: 1 }, lang_set: 0 },
            mask: 2,
        };

        let b1 = PhrasematchSubquery {
            id: 0,
            store: &store,
            weight: 0.5,
            match_key: MatchKey { match_phrase: Range { start: 0, end: 1 }, lang_set: 0 },
            mask: 1,
        };

        let b2 = PhrasematchSubquery {
            id: 0,
            store: &store,
            weight: 0.5,
            match_key: MatchKey { match_phrase: Range { start: 0, end: 1 }, lang_set: 0 },
            mask: 1,
        };

        let phrasematch_results = vec![vec![a1, b1, b2]];
        stackable(&phrasematch_results, None, 0, HashSet::new(), 0, 129, 0.0, 0);
    }
}
