mod builder;
mod coalesce;
mod common;
mod gridstore_generated;
mod spatial;
mod store;

pub use builder::*;
pub use coalesce::coalesce;
pub use common::*;
pub use store::*;

#[cfg(test)]
mod tests {
    use super::*;

    fn round(value: f64, digits: i32) -> f64 {
        let multiplier = 10.0_f64.powi(digits);
        (value * multiplier).round() / multiplier
    }

    fn langarray_to_langfield(array: &[u32]) -> u128 {
        let mut out = 0u128;
        for lang in array {
            out = out | (1 << *lang as usize);
        }
        out
    }

    /// Mapping of GridKey to all of the grid entries to insert into a store for that GridKey
    struct StoreEntryBuildingBlock {
        grid_key: GridKey,
        entries: Vec<GridEntry>,
    }

    /// Utility to create stores
    /// Takes an vector, with each item mapping to a store to create
    /// Each item is a vector with maps of grid keys to the entries to insert into the store for that grid key
    fn create_store(store_entries: Vec<StoreEntryBuildingBlock>) -> GridStore {
        let directory: tempfile::TempDir = tempfile::tempdir().unwrap();
        let mut builder = GridStoreBuilder::new(directory.path()).unwrap();
        for building_block in store_entries {
            builder
                .insert(&building_block.grid_key, &building_block.entries)
                .expect("Unable to insert record");
        }
        builder.finish().unwrap();
        GridStore::new(directory.path()).unwrap()
    }

    #[test]
    fn combined_test() {
        let directory: tempfile::TempDir = tempfile::tempdir().unwrap();
        let mut builder = GridStoreBuilder::new(directory.path()).unwrap();

        let key = GridKey { phrase_id: 1, lang_set: 1 };

        let mut entries = vec![
            GridEntry { id: 2, x: 2, y: 2, relev: 0.8, score: 3, source_phrase_hash: 0 },
            GridEntry { id: 3, x: 3, y: 3, relev: 1., score: 1, source_phrase_hash: 1 },
            GridEntry { id: 1, x: 1, y: 1, relev: 1., score: 7, source_phrase_hash: 2 },
        ];
        builder.insert(&key, &entries).expect("Unable to insert record");

        builder.finish().unwrap();

        let reader = GridStore::new(directory.path()).unwrap();
        let record: Vec<_> = reader.get(&key).unwrap().unwrap().collect();

        entries.sort_by(|a, b| b.partial_cmp(a).unwrap());
        assert_eq!(
            record, entries,
            "identical entries come out as went in, in reverse-sorted order"
        );

        {
            let key = GridKey { phrase_id: 2, lang_set: 1 };
            let record = reader.get(&key).expect("Failed to get key");
            assert!(record.is_none(), "Retrieved no results");
        }
    }

    #[test]
    fn phrase_hash_test() {
        let directory: tempfile::TempDir = tempfile::tempdir().unwrap();
        let mut builder = GridStoreBuilder::new(directory.path()).unwrap();

        let key = GridKey { phrase_id: 1, lang_set: 1 };

        let mut entries = vec![
            GridEntry { id: 1, x: 1, y: 1, relev: 1.0, score: 1, source_phrase_hash: 0 },
            GridEntry { id: 1, x: 1, y: 1, relev: 0.6, score: 1, source_phrase_hash: 2 },
            GridEntry { id: 1, x: 1, y: 1, relev: 0.4, score: 1, source_phrase_hash: 3 },
        ];
        builder.insert(&key, &entries).expect("Unable to insert record");

        builder.finish().unwrap();

        let reader = GridStore::new(directory.path()).unwrap();
        let record: Vec<_> = reader.get(&key).unwrap().unwrap().collect();

        entries.sort_by(|a, b| b.partial_cmp(a).unwrap());
        assert_eq!(
            record, entries,
            "identical entries come out as went in, in reverse-sorted order"
        );
    }

    #[test]
    fn cover_test() {
        let directory: tempfile::TempDir = tempfile::tempdir().unwrap();
        let mut builder = GridStoreBuilder::new(directory.path()).unwrap();

        let key = GridKey { phrase_id: 1, lang_set: 1 };

        let entries = vec![
            GridEntry { id: 1, x: 1, y: 1, relev: 1., score: 1, source_phrase_hash: 0 },
            GridEntry { id: 1, x: 1, y: 2, relev: 1., score: 1, source_phrase_hash: 0 },
            GridEntry { id: 1, x: 2, y: 1, relev: 1., score: 1, source_phrase_hash: 0 },
        ];
        builder.insert(&key, &entries).expect("Unable to insert record");

        builder.finish().unwrap();

        let reader = GridStore::new(directory.path()).unwrap();
        let record: Vec<_> = reader.get(&key).unwrap().unwrap().collect();

        // Results come back morton order. Maybe we should implement a custom partial_cmp
        assert_eq!(record[0], entries[1], "expected first result");
        assert_eq!(record[1], entries[2], "expected second result");
        assert_eq!(record[2], entries[0], "expected second result");
    }

    #[test]
    fn score_test() {
        let directory: tempfile::TempDir = tempfile::tempdir().unwrap();
        let mut builder = GridStoreBuilder::new(directory.path()).unwrap();

        let key = GridKey { phrase_id: 1, lang_set: 1 };

        let mut entries = vec![
            GridEntry { id: 1, x: 1, y: 1, relev: 1., score: 1, source_phrase_hash: 0 },
            GridEntry { id: 1, x: 1, y: 1, relev: 1., score: 7, source_phrase_hash: 0 },
        ];
        builder.insert(&key, &entries).expect("Unable to insert record");

        builder.finish().unwrap();

        let reader = GridStore::new(directory.path()).unwrap();
        let record: Vec<_> = reader.get(&key).unwrap().unwrap().collect();

        entries.sort_by(|a, b| b.partial_cmp(a).unwrap());
        assert_eq!(
            record, entries,
            "identical entries come out as went in, in reverse-sorted order"
        );
    }

    #[test]
    fn matching_test() {
        let directory: tempfile::TempDir = tempfile::tempdir().unwrap();
        let mut builder = GridStoreBuilder::new(directory.path()).unwrap();

        let keys = vec![
            GridKey { phrase_id: 1, lang_set: 1 },
            GridKey { phrase_id: 1, lang_set: 2 },
            GridKey { phrase_id: 2, lang_set: 1 },
            GridKey { phrase_id: 1, lang_set: 1 },
        ];

        let mut i = 0;
        for key in keys {
            for _j in 0..2 {
                #[cfg_attr(rustfmt, rustfmt::skip)]
                let entries = vec![
                    GridEntry { id: i, x: (2 * i) as u16, y: 1, relev: 1., score: 1, source_phrase_hash: 0 },
                    GridEntry { id: i + 1, x: ((2 * i) + 1) as u16, y: 1, relev: 1., score: 7, source_phrase_hash: 0 },
                    GridEntry { id: i + 2, x: ((2 * i) + 2) as u16, y: 1, relev: 1., score: 7, source_phrase_hash: 0 },
                    GridEntry { id: i + 3, x: ((2 * i) + 1) as u16, y: 1, relev: 1., score: 7, source_phrase_hash: 0 },
                ];
                i += 4;

                builder.insert(&key, &entries).expect("Unable to insert record");
            }
        }

        builder.finish().unwrap();

        let reader = GridStore::new(directory.path()).unwrap();

        let search_key =
            MatchKey { match_phrase: MatchPhrase::Range { start: 1, end: 2 }, lang_set: 1 };
        let records: Vec<_> =
            reader.get_matching(&search_key, &MatchOpts::default()).unwrap().collect();
        #[cfg_attr(rustfmt, rustfmt::skip)]
        assert_eq!(
            records,
            [
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 58, y: 1, id: 30, source_phrase_hash: 0 }, matches_language: true },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 57, y: 1, id: 31, source_phrase_hash: 0 }, matches_language: true },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 57, y: 1, id: 29, source_phrase_hash: 0 }, matches_language: true },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 1, x: 56, y: 1, id: 28, source_phrase_hash: 0 }, matches_language: true },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 26, y: 1, id: 14, source_phrase_hash: 0 }, matches_language: false },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 25, y: 1, id: 15, source_phrase_hash: 0 }, matches_language: false },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 25, y: 1, id: 13, source_phrase_hash: 0 }, matches_language: false },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 1, x: 24, y: 1, id: 12, source_phrase_hash: 0 }, matches_language: false }
            ]
        );

        let search_key =
            MatchKey { match_phrase: MatchPhrase::Range { start: 1, end: 3 }, lang_set: 1 };
        let records: Vec<_> =
            reader.get_matching(&search_key, &MatchOpts::default()).unwrap().collect();
        #[cfg_attr(rustfmt, rustfmt::skip)]
        assert_eq!(
            records,
            [
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 58, y: 1, id: 30, source_phrase_hash: 0 }, matches_language: true },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 57, y: 1, id: 31, source_phrase_hash: 0 }, matches_language: true },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 57, y: 1, id: 29, source_phrase_hash: 0 }, matches_language: true },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 42, y: 1, id: 22, source_phrase_hash: 0 }, matches_language: true },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 41, y: 1, id: 23, source_phrase_hash: 0 }, matches_language: true },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 41, y: 1, id: 21, source_phrase_hash: 0 }, matches_language: true },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 1, x: 56, y: 1, id: 28, source_phrase_hash: 0 }, matches_language: true },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 1, x: 40, y: 1, id: 20, source_phrase_hash: 0 }, matches_language: true },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 26, y: 1, id: 14, source_phrase_hash: 0 }, matches_language: false },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 25, y: 1, id: 15, source_phrase_hash: 0 }, matches_language: false },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 25, y: 1, id: 13, source_phrase_hash: 0 }, matches_language: false },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 1, x: 24, y: 1, id: 12, source_phrase_hash: 0 }, matches_language: false }
            ]
        );

        let search_key =
            MatchKey { match_phrase: MatchPhrase::Range { start: 1, end: 3 }, lang_set: 0 };
        let records: Vec<_> =
            reader.get_matching(&search_key, &MatchOpts::default()).unwrap().collect();
        #[cfg_attr(rustfmt, rustfmt::skip)]
        assert_eq!(
            records,
            [
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 58, y: 1, id: 30, source_phrase_hash: 0 }, matches_language: false },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 57, y: 1, id: 31, source_phrase_hash: 0 }, matches_language: false },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 57, y: 1, id: 29, source_phrase_hash: 0 }, matches_language: false },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 42, y: 1, id: 22, source_phrase_hash: 0 }, matches_language: false },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 41, y: 1, id: 23, source_phrase_hash: 0 }, matches_language: false },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 41, y: 1, id: 21, source_phrase_hash: 0 }, matches_language: false },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 26, y: 1, id: 14, source_phrase_hash: 0 }, matches_language: false },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 25, y: 1, id: 15, source_phrase_hash: 0 }, matches_language: false },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 25, y: 1, id: 13, source_phrase_hash: 0 }, matches_language: false },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 1, x: 56, y: 1, id: 28, source_phrase_hash: 0 }, matches_language: false },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 1, x: 40, y: 1, id: 20, source_phrase_hash: 0 }, matches_language: false },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 1, x: 24, y: 1, id: 12, source_phrase_hash: 0 }, matches_language: false }
            ]
        );

        let search_key =
            MatchKey { match_phrase: MatchPhrase::Range { start: 1, end: 3 }, lang_set: 2 };
        let records: Vec<_> =
            reader.get_matching(&search_key, &MatchOpts::default()).unwrap().collect();
        #[cfg_attr(rustfmt, rustfmt::skip)]
        assert_eq!(
            records,
            [
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 26, y: 1, id: 14, source_phrase_hash: 0 }, matches_language: true },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 25, y: 1, id: 15, source_phrase_hash: 0 }, matches_language: true },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 25, y: 1, id: 13, source_phrase_hash: 0 }, matches_language: true },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 1, x: 24, y: 1, id: 12, source_phrase_hash: 0 }, matches_language: true },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 58, y: 1, id: 30, source_phrase_hash: 0 }, matches_language: false },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 57, y: 1, id: 31, source_phrase_hash: 0 }, matches_language: false },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 57, y: 1, id: 29, source_phrase_hash: 0 }, matches_language: false },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 42, y: 1, id: 22, source_phrase_hash: 0 }, matches_language: false },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 41, y: 1, id: 23, source_phrase_hash: 0 }, matches_language: false },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 41, y: 1, id: 21, source_phrase_hash: 0 }, matches_language: false },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 1, x: 56, y: 1, id: 28, source_phrase_hash: 0 }, matches_language: false },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 1, x: 40, y: 1, id: 20, source_phrase_hash: 0 }, matches_language: false }
            ]
        );

        let search_key =
            MatchKey { match_phrase: MatchPhrase::Range { start: 1, end: 3 }, lang_set: 3 };
        let records: Vec<_> =
            reader.get_matching(&search_key, &MatchOpts::default()).unwrap().collect();
        #[cfg_attr(rustfmt, rustfmt::skip)]
        assert_eq!(
            records,
            [
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 58, y: 1, id: 30, source_phrase_hash: 0 }, matches_language: true },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 57, y: 1, id: 31, source_phrase_hash: 0 }, matches_language: true },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 57, y: 1, id: 29, source_phrase_hash: 0 }, matches_language: true },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 42, y: 1, id: 22, source_phrase_hash: 0 }, matches_language: true },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 41, y: 1, id: 23, source_phrase_hash: 0 }, matches_language: true },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 41, y: 1, id: 21, source_phrase_hash: 0 }, matches_language: true },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 26, y: 1, id: 14, source_phrase_hash: 0 }, matches_language: true },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 25, y: 1, id: 15, source_phrase_hash: 0 }, matches_language: true },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 25, y: 1, id: 13, source_phrase_hash: 0 }, matches_language: true },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 1, x: 56, y: 1, id: 28, source_phrase_hash: 0 }, matches_language: true },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 1, x: 40, y: 1, id: 20, source_phrase_hash: 0 }, matches_language: true },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 1, x: 24, y: 1, id: 12, source_phrase_hash: 0 }, matches_language: true }
            ]
        );

        let search_key =
            MatchKey { match_phrase: MatchPhrase::Range { start: 1, end: 1 }, lang_set: 1 };
        let records: Vec<_> =
            reader.get_matching(&search_key, &MatchOpts::default()).unwrap().collect();
        assert_eq!(records, []);

        let search_key =
            MatchKey { match_phrase: MatchPhrase::Range { start: 3, end: 4 }, lang_set: 1 };
        let records: Vec<_> =
            reader.get_matching(&search_key, &MatchOpts::default()).unwrap().collect();
        assert_eq!(records, []);

        let search_key =
            MatchKey { match_phrase: MatchPhrase::Range { start: 1, end: 3 }, lang_set: 1 };
        let records: Vec<_> = reader
            .get_matching(
                &search_key,
                &MatchOpts { bbox: Some([26, 0, 41, 2]), ..MatchOpts::default() },
            )
            .unwrap()
            .collect();
        #[cfg_attr(rustfmt, rustfmt::skip)]
        assert_eq!(
            records,
            [
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 41, y: 1, id: 23, source_phrase_hash: 0 }, matches_language: true },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 41, y: 1, id: 21, source_phrase_hash: 0 }, matches_language: true },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 1, x: 40, y: 1, id: 20, source_phrase_hash: 0 }, matches_language: true },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 26, y: 1, id: 14, source_phrase_hash: 0 }, matches_language: false }
            ]
        );

        // Search just below existing records where z-order curve overlaps with bbox, but we do not
        // want records.
        let search_key =
            MatchKey { match_phrase: MatchPhrase::Range { start: 1, end: 3 }, lang_set: 1 };
        let records: Vec<_> = reader
            .get_matching(
                &search_key,
                &MatchOpts { bbox: Some([0, 2, 100, 2]), proximity: None, ..MatchOpts::default() },
            )
            .unwrap()
            .collect();
        assert_eq!(records.len(), 0, "no matching recods in bbox");

        // Search where neigther z-order curve or actual x,y overlap with bbox.
        let search_key =
            MatchKey { match_phrase: MatchPhrase::Range { start: 1, end: 3 }, lang_set: 1 };
        let records: Vec<_> = reader
            .get_matching(
                &search_key,
                &MatchOpts {
                    bbox: Some([100, 100, 100, 100]),
                    proximity: None,
                    ..MatchOpts::default()
                },
            )
            .unwrap()
            .collect();
        assert_eq!(records.len(), 0, "no matching recods in bbox");

        let search_key =
            MatchKey { match_phrase: MatchPhrase::Range { start: 1, end: 3 }, lang_set: 2 };
        let records: Vec<_> = reader
            .get_matching(
                &search_key,
                &MatchOpts {
                    bbox: None,
                    proximity: Some(Proximity { point: [26, 1], radius: 1000. }),
                    ..MatchOpts::default()
                },
            )
            .unwrap()
            .collect();
        #[cfg_attr(rustfmt, rustfmt::skip)]
        assert_eq!(
            records,
            [
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 26, y: 1, id: 14, source_phrase_hash: 0 }, matches_language: true },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 25, y: 1, id: 15, source_phrase_hash: 0 }, matches_language: true },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 25, y: 1, id: 13, source_phrase_hash: 0 }, matches_language: true },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 1, x: 24, y: 1, id: 12, source_phrase_hash: 0 }, matches_language: true },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 57, y: 1, id: 31, source_phrase_hash: 0 }, matches_language: false },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 57, y: 1, id: 29, source_phrase_hash: 0 }, matches_language: false },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 58, y: 1, id: 30, source_phrase_hash: 0 }, matches_language: false },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 41, y: 1, id: 23, source_phrase_hash: 0 }, matches_language: false },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 41, y: 1, id: 21, source_phrase_hash: 0 }, matches_language: false },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 42, y: 1, id: 22, source_phrase_hash: 0 }, matches_language: false },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 1, x: 56, y: 1, id: 28, source_phrase_hash: 0 }, matches_language: false },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 1, x: 40, y: 1, id: 20, source_phrase_hash: 0 }, matches_language: false }
            ]
        );

        let search_key =
            MatchKey { match_phrase: MatchPhrase::Range { start: 1, end: 3 }, lang_set: 2 };
        let records: Vec<_> = reader
            .get_matching(
                &search_key,
                &MatchOpts {
                    bbox: Some([10, 0, 41, 2]),
                    proximity: Some(Proximity { point: [26, 1], radius: 1000. }),
                    ..MatchOpts::default()
                },
            )
            .unwrap()
            .collect();
        #[cfg_attr(rustfmt, rustfmt::skip)]
        assert_eq!(
            records,
            [
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 26, y: 1, id: 14, source_phrase_hash: 0 }, matches_language: true },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 25, y: 1, id: 15, source_phrase_hash: 0 }, matches_language: true },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 25, y: 1, id: 13, source_phrase_hash: 0 }, matches_language: true },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 1, x: 24, y: 1, id: 12, source_phrase_hash: 0 }, matches_language: true },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 41, y: 1, id: 23, source_phrase_hash: 0 }, matches_language: false },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 7, x: 41, y: 1, id: 21, source_phrase_hash: 0 }, matches_language: false },
                MatchEntry { grid_entry: GridEntry { relev: 1.0, score: 1, x: 40, y: 1, id: 20, source_phrase_hash: 0 }, matches_language: false }
            ]
        );
    }
}
