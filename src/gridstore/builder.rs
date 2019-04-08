use std::collections::BTreeMap;
 use std::error::Error;

use itertools::Itertools;
use morton::interleave_morton;

use crate::gridstore::common::*;
use crate::gridstore::gridstore_generated::*;

type BuilderEntry = BTreeMap<u8, BTreeMap<u32, Vec<u32>>>;

pub struct GridStoreBuilder {
    filename: String,
    data: BTreeMap<GridKey, BuilderEntry>
}

#[inline]
fn relev_float_to_int(relev: f32) -> u8 {
    match relev {
        0.4 => 0,
        0.6 => 1,
        0.8 => 2,
        _ => 3
    }
}

fn extend_entries(builder_entry: &mut BuilderEntry, values: &[GridEntry]) -> () {
    for (rs, values) in &values.into_iter().group_by(|value| (relev_float_to_int(value.relev) << 4) | value.score) {
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

    pub fn finish(mut self) -> Result<(), Box<Error>> {
        for (grid_key, value) in self.data.iter_mut() {
            let mut fb_builder = flatbuffers::FlatBufferBuilder::new();
            let mut rses: Vec<_> = Vec::new();
            for (rs, coord_group) in value.iter_mut() {
                let mut coords: Vec<_> = Vec::new();
                for (coord, ids) in coord_group.iter_mut() {
                    // reverse sort
                    ids.sort_by(|a, b| b.cmp(a));
                    ids.dedup();

                    let fb_ids = fb_builder.create_vector(&ids);
                    let fb_coord = Coord::create(&mut fb_builder, &CoordArgs{coord: *coord, ids: Some(fb_ids)});
                    coords.push(fb_coord);
                }
                let fb_coords = fb_builder.create_vector(&coords);
                let fb_rs = RelevScore::create(&mut fb_builder, &RelevScoreArgs{relev_score: *rs, coords: Some(fb_coords)});
                rses.push(fb_rs);
            }
            let fb_rses = fb_builder.create_vector(&rses);
            let record = PhraseRecord::create(&mut fb_builder, &PhraseRecordArgs{relev_scores: Some(fb_rses)});
            fb_builder.finish(record, None);

            println!("{:?}", fb_builder.finished_data());
        }
        Ok(())
    }
}

#[test]
fn basic_test() {
    let mut builder = GridStoreBuilder::new("whatever.rocksdb").unwrap();
    // memcache._set('1', [
    builder.insert(&GridKey { phrase_id: 1, lang_set: 1 }, &vec![
        GridEntry {
            id: 2,
            x: 2,
            y: 2,
            relev: 0.8,
            score: 3,
            source_phrase_hash: 0
        },
        GridEntry {
            id: 3,
            x: 3,
            y: 3,
            relev: 1.,
            score: 1,
            source_phrase_hash: 1
        },
        GridEntry {
            id: 1,
            x: 1,
            y: 1,
            relev: 1.,
            score: 7,
            source_phrase_hash: 2
        }
    ]);

    builder.finish().unwrap();
}