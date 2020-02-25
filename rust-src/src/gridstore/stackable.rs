#![allow(dead_code)]
use std::borrow::Borrow;
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
    pub bmask: Vec<u32>,
    pub mask: u32,
}

pub fn stackable<'a, T: Borrow<GridStore> + Clone + Debug>(
    phrasematch_results: &Vec<Vec<PhrasematchResults<T>>>,
    phrasematch_result: Option<PhrasematchResults<T>>,
    nmask: u32,
    bmask: Vec<u32>,
    mask: u32,
) -> StackableNode<T> {
    let mut node = StackableNode {
        phrasematch: phrasematch_result,
        children: vec![],
        mask: nmask,
        bmask: bmask,
        nmask: mask,
    };

    for phrasematch_per_index in phrasematch_results.iter() {
        for phrasematches in phrasematch_per_index.iter() {
            if (node.nmask & phrasematches.nmask) == 0 && (node.mask & phrasematches.mask) == 0 {
                let target_nmask = &phrasematches.nmask | node.nmask;
                let target_mask = &phrasematches.idx | node.mask;
                node.children.push(stackable(
                    &phrasematch_results,
                    Some(phrasematches.clone()),
                    target_nmask,
                    phrasematches.clone().bmask,
                    target_mask,
                ));
            }
        }
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

        let phrasematch_1 = PhrasematchResults {
            store: &store,
            scorefactor: 1,
            prefix: 0,
            weight: 1.0,
            match_key: MatchKey { match_phrase: Range { start: 0, end: 3 }, lang_set: 1 },
            idx: 14,
            zoom: 6,
            nmask: 4,
            mask: 1,
            bmask: vec![0],
            edit_multiplier: 1.0,
            subquery_edit_distance: 0,
        };

        let phrasematch_2 = PhrasematchResults {
            store: &store,
            scorefactor: 1,
            prefix: 0,
            weight: 1.0,
            match_key: MatchKey { match_phrase: Range { start: 0, end: 3 }, lang_set: 1 },
            idx: 14,
            zoom: 6,
            nmask: 6,
            mask: 1,
            bmask: vec![0],
            edit_multiplier: 1.0,
            subquery_edit_distance: 0,
        };

        let phrasematch_results = vec![vec![phrasematch_1, phrasematch_2]];
        stackable(&phrasematch_results, None, 0, vec![0], 0);
    }
}
