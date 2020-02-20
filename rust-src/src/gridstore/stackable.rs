#![allow(dead_code)]
use std::fmt::Debug;
use std::borrow::Borrow;

use std::rc::Rc;
use std::cell::RefCell;
use crate::gridstore::store::*;
use crate::gridstore::builder::*;
use crate::gridstore::common::*;
use crate::gridstore::common::MatchPhrase::Range;

#[derive(Debug, Clone)]
pub struct StackableNode<'a, T: Borrow<GridStore> + Clone + Debug> {
    pub phrasematch: Option<PhrasematchResults<'a, T>>,
    pub children: Vec<StackableNode<'a, T>>,
    pub nmask: u32,
    pub bmask: Vec<u32>,
    pub mask: u32
}

impl<'a, T: Borrow<GridStore> + Clone + Debug> StackableNode<'a, T> {
    pub fn new(phrasematch_results: Option<PhrasematchResults<'a, T>>, nmask: u32, bmask: Vec<u32>, mask: u32) -> StackableNode<'a, T> {
        StackableNode {
            phrasematch: phrasematch_results,
            children: vec![],
            nmask: nmask,
            bmask: bmask,
            mask: mask
        }
    }

    pub fn is_leaf(&self) -> bool {
        self.children.len() == 0
    }
}

pub fn stackable<T: Borrow<GridStore> + Clone + Debug>(phrasematch_results: Vec<Vec<PhrasematchResults<T>>>) {
    let mut root: Rc<StackableNode<T>> = Rc::new(StackableNode::new(None, 0, vec![0], 0));
    let mut nodes_to_visit: Vec<Rc<StackableNode<T>>> = vec![Rc::clone(&root)];
    while nodes_to_visit.len() != 0 {
        let top_node = nodes_to_visit.pop();
        let mut node = top_node.unwrap();
        for phrasematch_per_index in phrasematch_results.iter() {
            for phrasematches in phrasematch_per_index.iter() {
                if ((node.nmask & phrasematches.nmask) != 0 && (node.mask & phrasematches.mask) != 0) || node.phrasematch.is_none() {
                let nm = node.nmask & phrasematches.nmask;
                let m = node.mask & phrasematches.mask;
                    let target_nmask = phrasematches.nmask | node.nmask;
                    let target_mask = phrasematches.idx | node.mask;
                    let new_child_node = Rc::new(StackableNode::new(Some(phrasematches.clone()), target_nmask, phrasematches.clone().bmask, target_mask));
                    nodes_to_visit.push(Rc::clone(&new_child_node));
                    node.children.push(RefCell::new(Rc::clone(&new_child_node)));
                }
            }
        }
    }
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
        subquery: vec!["main", "street"],
        phrase: "main street",
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
        prox_match: false,
        cat_match: false,
        partial_number: false,
        subquery_edit_distance: 0,
        original_phrase: vec!["main", "street"],
        original_phrase_ender: 0,
        original_phrase_mask: 14
    };

    let phrasematch_2 = PhrasematchResults {
        store: &store,
        subquery: vec!["nw", "street"],
        phrase: "nw street",
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
        prox_match: false,
        cat_match: false,
        partial_number: false,
        subquery_edit_distance: 0,
        original_phrase: vec!["nw", "street"],
        original_phrase_ender: 0,
        original_phrase_mask: 14
    };
    let phrasematch_results = vec![vec![phrasematch_1, phrasematch_2]];
    stackable(phrasematch_results);

    }
}
