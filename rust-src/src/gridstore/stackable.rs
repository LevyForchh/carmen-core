use std::fmt::Debug;
use std::borrow::Borrow;

use crate::gridstore::store::GridStore;
use crate::gridstore::common::*;


#[derive(Debug, Clone)]
pub struct StackableNode<T: Borrow<GridStore> + Clone + Debug> {
    pub phrasematch: PhrasematchResults<T>,
    pub children: Vec<StackableNode<T>>,
    pub nmask: u16,
    pub bmask: Vec<u16>,
    pub mask: u16
}

pub fn stackable<T: Borrow<GridStore> + Clone + Debug>(phrasematch_results: Vec<Vec<PhrasematchResults<T>>>) {
    for phrasematch_index in phrasematch_results.iter() {
        for phrasematch in phrasematch_index.iter() {
            if (phrasematch.nmask | )
        }
    }
}
