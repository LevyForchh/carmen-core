use std::collections::BTreeMap;
 use std::error::Error;

use itertools::Itertools;
use morton::interleave_morton;

use crate::gridstore::common::*;

#[derive(Debug, PartialOrd, Ord, PartialEq, Eq)]
struct RelevScore {
    relev: u8,
    score: u8
}

type BuilderEntry = BTreeMap<RelevScore, BTreeMap<u32, Vec<u32>>>;

pub struct GridStoreBuilder {
    filename: String,
    data: BTreeMap<GridKey, BuilderEntry>
}

fn extend_entries(builder_entry: &mut BuilderEntry, values: &[GridEntry]) -> () {
    for (rs, values) in &values.into_iter().group_by(|value| RelevScore { relev: value.relev, score: value.score }) {
        let rs_entry = builder_entry
            .entry(rs)
            .or_insert_with(|| BTreeMap::new());
        for (zcoord, values) in &values.into_iter().group_by(|value| interleave_morton(value.x, value.y)) {
            let zcoord_entry = rs_entry
                .entry(zcoord)
                .or_insert_with(|| Vec::new());
            for value in values {
                let id_phrase: u32 = (value.id << 8) | (value.source_phrase_hash as u32);
                zcoord_entry.push(id_phrase);
            }
        }
    }
}

impl GridStoreBuilder {
    pub fn new(filename: &str) -> Result<GridStoreBuilder, Box<dyn Error>> {
        Ok(GridStoreBuilder { filename: filename.to_owned(), data: BTreeMap::new() })
    }

    pub fn insert(&mut self, key: &GridKey, values: &[GridEntry]) -> Result<(), Box<dyn Error>> {
        let mut to_insert = BuilderEntry::new();
        extend_entries(&mut to_insert, values);
        self.data.insert(key.to_owned(), to_insert);
        Ok(())
    }

    pub fn append(&mut self, key: &GridKey, values: &[GridEntry]) -> Result<(), Box<dyn Error>> {
        let mut to_append = self.data.entry(key.to_owned()).or_insert_with(|| BuilderEntry::new());
        extend_entries(&mut to_append, values);
        Ok(())
    }
}