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
    fn renumber_test() {
        let directory: tempfile::TempDir = tempfile::tempdir().unwrap();
        let mut builder = GridStoreBuilder::new(directory.path()).unwrap();

        // phrase IDs are descending, grid IDs are ascending
        let items = vec![
            (
                GridKey { phrase_id: 2, lang_set: 1 },
                GridEntry { id: 0, x: 1, y: 1, relev: 1., score: 7, source_phrase_hash: 2 },
            ),
            (
                GridKey { phrase_id: 1, lang_set: 1 },
                GridEntry { id: 1, x: 1, y: 1, relev: 1., score: 7, source_phrase_hash: 2 },
            ),
            (
                GridKey { phrase_id: 0, lang_set: 1 },
                GridEntry { id: 2, x: 1, y: 1, relev: 1., score: 7, source_phrase_hash: 2 },
            ),
        ];

        for (key, val) in items {
            builder.insert(&key, &vec![val]).expect("Unable to insert record");
        }
        builder.renumber(&vec![2, 1, 0]).unwrap();
        // after renumbering, the IDs should match
        builder.finish().unwrap();

        let reader = GridStore::new(directory.path()).unwrap();

        for id in 0..=2 {
            let entries: Vec<_> =
                reader.get(&GridKey { phrase_id: id, lang_set: 1 }).unwrap().unwrap().collect();
            assert_eq!(id, entries[0].id);
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
        for key in keys.iter() {
            for _j in 0..2 {
                #[cfg_attr(rustfmt, rustfmt::skip)]
                let entries = vec![
                    GridEntry { id: i, x: (2 * i) as u16, y: 1, relev: 1., score: 1, source_phrase_hash: 0 },
                    GridEntry { id: i + 1, x: ((2 * i) + 1) as u16, y: 1, relev: 1., score: 7, source_phrase_hash: 0 },
                    GridEntry { id: i + 2, x: ((2 * i) + 2) as u16, y: 1, relev: 1., score: 7, source_phrase_hash: 0 },
                    GridEntry { id: i + 3, x: ((2 * i) + 1) as u16, y: 1, relev: 1., score: 7, source_phrase_hash: 0 },
                ];
                i += 4;

                builder.insert(key, &entries).expect("Unable to insert record");
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

        let listed_keys: Result<Vec<_>, _> = reader.keys().collect();
        let mut orig_keys = keys.clone();
        orig_keys.sort();
        orig_keys.dedup();
        assert_eq!(listed_keys.unwrap(), orig_keys);
    }

    #[test]
    fn coalesce_test_ns_bias() {
        let directory: tempfile::TempDir = tempfile::tempdir().unwrap();
        let mut builder = GridStoreBuilder::new(directory.path()).unwrap();

        let key = GridKey { phrase_id: 1, lang_set: 1 };

        let entries = vec![
            GridEntry { id: 1, x: 200, y: 200, relev: 1., score: 1, source_phrase_hash: 0 },
            GridEntry { id: 2, x: 200, y: 0, relev: 1., score: 1, source_phrase_hash: 0 },
            GridEntry { id: 3, x: 0, y: 0, relev: 1., score: 1, source_phrase_hash: 0 },
            GridEntry { id: 4, x: 0, y: 200, relev: 1., score: 1, source_phrase_hash: 0 },
        ];
        builder.insert(&key, &entries).expect("Unable to insert record");

        builder.finish().unwrap();

        let store = GridStore::new(directory.path()).unwrap();
        let subquery = PhrasematchSubquery {
            store: &store,
            weight: 1.,
            match_key: MatchKey {
                match_phrase: MatchPhrase::Range { start: 1, end: 3 },
                lang_set: 1,
            },
            idx: 1,
            zoom: 14,
            mask: 1 << 0,
        };
        let stack = vec![subquery];
        let match_opts = MatchOpts {
            zoom: 14,
            proximity: Some(Proximity { point: [110, 115], radius: 200. }),
            ..MatchOpts::default()
        };
        let result = coalesce(stack.clone(), &match_opts).unwrap();
        let result_ids: Vec<u32> =
            result.iter().map(|context| context.entries[0].grid_entry.id).collect();
        // TODO: is this description correct?
        assert_eq!(
            result_ids,
            [1, 4, 2, 3],
            "Results should favor N/S proximity over E/W proximity for consistency"
        );
        let result_distances: Vec<f64> =
            result.iter().map(|context| round(context.entries[0].distance, 0)).collect();
        assert_eq!(
            result_distances,
            [124.0, 139.0, 146.0, 159.0],
            "Result distances are calculated correctly"
        );
        // TODO: figure out what these tests are trying to test in carmen-cache, and port the rest
    }

    #[test]
    #[cfg_attr(rustfmt, rustfmt::skip)]
    fn coalesce_test_proximity_basic() {
        let directory: tempfile::TempDir = tempfile::tempdir().unwrap();
        let mut builder = GridStoreBuilder::new(directory.path()).unwrap();

        let key = GridKey { phrase_id: 1, lang_set: 1 };

        let entries = vec![
            GridEntry { id: 1, x: 2, y: 2, relev: 1., score: 1, source_phrase_hash: 0 },
            GridEntry { id: 2, x: 2, y: 0, relev: 1., score: 1, source_phrase_hash: 0 },
            GridEntry { id: 3, x: 0, y: 0, relev: 1., score: 1, source_phrase_hash: 0 },
            GridEntry { id: 4, x: 0, y: 2, relev: 1., score: 1, source_phrase_hash: 0 },
        ];
        builder.insert(&key, &entries).expect("Unable to insert record");

        builder.finish().unwrap();

        let store = GridStore::new(directory.path()).unwrap();
        let subquery = PhrasematchSubquery {
            store: &store,
            weight: 1.,
            match_key: MatchKey { match_phrase: MatchPhrase::Range { start: 1, end: 3 }, lang_set: 1 },
            idx: 1,
            zoom: 14,
            mask: 1 << 0,
        };
        let stack = vec![subquery.clone()];
        let match_opts = MatchOpts {
            zoom: 14,
            proximity: Some(Proximity { point: [2, 2], radius: 400. }),
            ..MatchOpts::default()
        };
        let result = coalesce(stack.clone(), &match_opts).unwrap();
        let result_ids: Vec<u32> =
            result.iter().map(|context| context.entries[0].grid_entry.id).collect();
        assert_eq!(result_ids, [1, 2, 4, 3], "Results with the same relev and score should be ordered by distance");

        let result_distances: Vec<f64> =
            result.iter().map(|context| round(context.entries[0].distance, 2)).collect();
        assert_eq!(result_distances, [0.00, 2.00, 2.00, 2.83], "Results with the same relev and score should be ordered by distance");
    }

    #[test]
    #[cfg_attr(rustfmt, rustfmt::skip)]
    fn coalesce_test_language_penalty() {
        let directory: tempfile::TempDir = tempfile::tempdir().unwrap();
        let mut builder = GridStoreBuilder::new(directory.path()).unwrap();

        let key = GridKey { phrase_id: 1, lang_set: 1 };

        let entries = vec![
            GridEntry { id: 1, x: 2, y: 2, relev: 1., score: 1, source_phrase_hash: 0 },
            GridEntry { id: 2, x: 2, y: 0, relev: 1., score: 1, source_phrase_hash: 0 },
            GridEntry { id: 3, x: 0, y: 0, relev: 1., score: 1, source_phrase_hash: 0 },
            GridEntry { id: 4, x: 0, y: 2, relev: 1., score: 1, source_phrase_hash: 0 },
        ];
        builder.insert(&key, &entries).expect("Unable to insert record");
        builder.finish().unwrap();

        let store = GridStore::new(directory.path()).unwrap();
        let subquery = PhrasematchSubquery {
            store: &store,
            weight: 1.,
            match_key: MatchKey { match_phrase: MatchPhrase::Range { start: 1, end: 3 }, lang_set: 2 },
            idx: 1,
            zoom: 14,
            mask: 1 << 0,
        };
        let stack = vec![subquery.clone()];
        let match_opts = MatchOpts {
            zoom: 14,
            proximity: Some(Proximity { point: [2, 2], radius: 1. }),
            ..MatchOpts::default()
        };
        let result = coalesce(stack.clone(), &match_opts).unwrap();
        assert_eq!(result[0].relev, 1., "Contexts inside the proximity radius don't get a cross langauge penalty");
        assert_eq!(result[0].entries[0].grid_entry.relev, 1., "Grids inside the proximity radius don't get a cross language penalty");
        assert_eq!(result[0].entries[0].matches_language, false, "Matches language property is correctly set on CoalesceEntry");
        assert_eq!(result[1].relev, 0.96, "Contexts outside the proximity radius get a cross langauge penalty");
        assert_eq!(result[1].entries[0].grid_entry.relev, 0.96, "Grids outside the proximity radius get a cross language penalty");
        assert_eq!(result[1].entries[0].matches_language, false, "Matches language property is correctly set on CoalesceEntry");
        let match_opts = MatchOpts {
            zoom: 14,
            ..MatchOpts::default()
        };
        let stack = vec![subquery.clone()];
        let result = coalesce(stack.clone(), &match_opts).unwrap();
        assert_eq!(result[0].relev, 0.96, "With no proximity, cross language contexts get a penalty");
        assert_eq!(result[0].entries[0].grid_entry.relev, 0.96, "With no proximity, cross language grids get a penalty");
        assert_eq!(result[0].entries[0].matches_language, false, "Matches language property is correctly set on CoalesceEntry");
    }

    #[test]
    #[cfg_attr(rustfmt, rustfmt::skip)]
    fn coalesce_multi_test_language_penalty() {
        // Set up 2 GridStores
        let directory1: tempfile::TempDir = tempfile::tempdir().unwrap();
        let directory2: tempfile::TempDir = tempfile::tempdir().unwrap();
        let mut builder1 = GridStoreBuilder::new(directory1.path()).unwrap();
        let mut builder2 = GridStoreBuilder::new(directory2.path()).unwrap();

        // Add more specific layer into a store
        let mut grid_key = GridKey { phrase_id: 1, lang_set: 1 };
        let mut entries = vec![
               GridEntry { id: 1, x: 2, y: 2, relev: 1., score: 1, source_phrase_hash: 0 },
               GridEntry { id: 2, x: 12800, y: 12800, relev: 1., score: 1, source_phrase_hash: 0 },
        ];
        builder1.insert(&grid_key, &entries).expect("Unable to insert record");
        builder1.finish().unwrap();

        // Add less specific layer into a store
        grid_key = GridKey { phrase_id: 2, lang_set: 1 };
        entries = vec![
            GridEntry { id: 3, x: 0, y: 0, relev: 1., score: 1, source_phrase_hash: 0 },
            GridEntry { id: 4, x: 50, y: 50, relev: 1., score: 1, source_phrase_hash: 0 },
        ];
        builder2.insert(&grid_key, &entries).expect("Unable to insert record");
        builder2.finish().unwrap();

        let store1 = GridStore::new(directory1.path()).unwrap();
        let store2 = GridStore::new(directory2.path()).unwrap();

        let stack = vec![
            PhrasematchSubquery {
                store: &store1,
                weight: 0.5,
                match_key: MatchKey { match_phrase: MatchPhrase::Range { start: 1, end: 3 }, lang_set: 2 },
                idx: 1,
                zoom: 14,
                mask: 1 << 0,
            },
            PhrasematchSubquery {
                store: &store2,
                weight: 0.5,
                match_key: MatchKey { match_phrase: MatchPhrase::Range { start: 1, end: 3 }, lang_set: 2 },
                idx: 2,
                zoom: 6,
                mask: 1 << 1,
            },
        ];

        let match_opts = MatchOpts {
            zoom: 14,
            proximity: Some(Proximity { point: [2, 2], radius: 1. }),
            ..MatchOpts::default()
        };
        let result = coalesce(stack.clone(), &match_opts).unwrap();
        assert_eq!(result[0].relev, 1., "Contexts inside the proximity radius don't get a cross langauge penalty");
        assert_eq!(result[0].entries[0].grid_entry.relev, 0.5, "Grids inside the proximity radius don't get a cross language penalty");
        assert_eq!(result[0].entries[0].matches_language, false, "Matches language property is correctly set on CoalesceEntry");
        assert_eq!(result[1].relev, 0.96, "Contexts outside the proximity radius get a cross langauge penalty");
        assert_eq!(result[1].entries[0].grid_entry.relev, 0.48, "1st grid outside the proximity radius get a cross language penalty");
        assert_eq!(result[1].entries[1].grid_entry.relev, 0.48, "2nd grid outside the proximity radius gets a cross language penalty");
        assert_eq!(result[1].entries[0].matches_language, false, "Matches language property is correctly set on 1st CoalesceEntry in context");
        assert_eq!(result[1].entries[1].matches_language, false, "Matches language property is correctly set on 2nd CoalesceEntry in context");
        let match_opts = MatchOpts {
            zoom: 14,
            ..MatchOpts::default()
        };
        let result = coalesce(stack.clone(), &match_opts).unwrap();
        assert_eq!(result[0].relev, 0.96, "With no proximity, cross language contexts get a penalty");
        assert_eq!(result[0].entries[0].grid_entry.relev, 0.48, "With no proximity, cross language grids get a penalty");
        assert_eq!(result[0].entries[0].matches_language, false, "Matches language property is correctly set on CoalesceEntry");
    }

    #[test]
    #[cfg_attr(rustfmt, rustfmt::skip)]
    fn coalesce_single_test() {
        // TODO: break this setup out, and break sets of tests into separate functions?
        let directory: tempfile::TempDir = tempfile::tempdir().unwrap();
        let mut builder = GridStoreBuilder::new(directory.path()).unwrap();

        let key = GridKey { phrase_id: 1, lang_set: 1 };

        let entries = vec![
            GridEntry { id: 1, x: 1, y: 1, relev: 1., score: 3, source_phrase_hash: 0 },
            GridEntry { id: 2, x: 2, y: 2, relev: 0.8, score: 3, source_phrase_hash: 0 },
            GridEntry { id: 3, x: 3, y: 3, relev: 1., score: 1, source_phrase_hash: 0 },
        ];
        builder.insert(&key, &entries).expect("Unable to insert record");
        builder.finish().unwrap();

        let store = GridStore::new(directory.path()).unwrap();
        let subquery = PhrasematchSubquery {
            store: &store,
            weight: 1.,
            match_key: MatchKey { match_phrase: MatchPhrase::Range { start: 1, end: 3 }, lang_set: 1 },
            idx: 1,
            zoom: 6,
            mask: 1 << 0,
        };
        let stack = vec![subquery];

        // Test default opts - no proximity or bbox
        let match_opts = MatchOpts {
            zoom: 6,
            ..MatchOpts::default()
        };
        let result = coalesce(stack.clone(), &match_opts).unwrap();
        assert_eq!(result[0].relev, 1., "No prox no bbox - 1st result has relevance 1");
        assert_eq!(result[0].entries.len(), 1, "No prox no bbox - 1st result has one coalesce entry");
        assert_eq!(result[0].entries[0].matches_language, true, "No prox no bbox - 1st result is a language match");
        assert_eq!(result[0].entries[0].distance, 0., "No prox no bbox - 1st result has distance 0");
        assert_eq!(result[0].entries[0].idx, 1, "No prox no bbox - 1st result has idx of subquery");
        assert_eq!(result[0].entries[0].mask, 1 << 0, "No prox no bbox - 1st result has original mask");
        assert_eq!(result[0].entries[0].scoredist, 3., "No prox no bbox - 1st result scoredist is the grid score");
        assert_eq!(result[0].entries[0].grid_entry, GridEntry {
                id: 1,
                x: 1,
                y: 1,
                relev: 1.,
                score: 3,
                source_phrase_hash: 0,
            }, "No prox no bbox - 1st result grid entry is the highest relevance and score");
        assert_eq!(result[1].relev, 1., "No prox no bbox - 2nd result has relevance 1");
        assert_eq!(result[1].entries.len(), 1, "No prox no bbox - 2nd result has one coalesce entry");
        assert_eq!(result[1].entries[0].matches_language, true, "No prox no bbox - 2nd result is a language match");
        assert_eq!(result[1].entries[0].distance, 0., "No prox no bbox - 2nd result has distance 0");
        assert_eq!(result[1].entries[0].idx, 1, "No prox no bbox - 2nd result has idx of subquery");
        assert_eq!(result[1].entries[0].mask, 1 << 0, "No prox no bbox - 2nd result has original mask");
        assert_eq!(result[1].entries[0].scoredist, 1., "No prox no bbox - 2nd result scoredist is the grid score");
        assert_eq!(result[1].entries[0].grid_entry, GridEntry {
                id: 3,
                x: 3,
                y: 3,
                relev: 1.,
                score: 1,
                source_phrase_hash: 0,
            }, "No prox no bbox - 2nd result grid entry is the highest relevance, lower score");
        assert_eq!(result[2].relev, 0.8, "No prox no bbox - 3rd result has relevance 0.8");
        assert_eq!(result[2].entries.len(), 1, "No prox no bbox - 3rd result has one coalesce entry");
        assert_eq!(result[2].entries[0].matches_language, true, "No prox no bbox - 3rd result is a language match");
        assert_eq!(result[2].entries[0].distance, 0., "No prox no bbox - 3rd result has distance 0");
        assert_eq!(result[2].entries[0].idx, 1, "No prox no bbox - 3rd result has idx of subquery");
        assert_eq!(result[2].entries[0].mask, 1 << 0, "No prox no bbox - 3rd result has original mask");
        assert_eq!(result[2].entries[0].scoredist, 3., "No prox no bbox - 3rd result scoredist is the grid score");
        assert_eq!(result[2].entries[0].grid_entry, GridEntry {
                id: 2,
                x: 2,
                y: 2,
                relev: 0.8,
                score: 3,
                source_phrase_hash: 0,
            }, "No prox no bbox - 3rd result grid entry is the lowest relevance, even though score is higher than 2nd");

        // Test opts with proximity
        let match_opts = MatchOpts {
            zoom: 6,
            proximity: Some(Proximity {
                point: [3,3],
                radius: 40.,
            }),
            ..MatchOpts::default()
        };
        let result = coalesce(stack.clone(), &match_opts).unwrap();
        assert_eq!(result[0].entries[0].grid_entry.id, 3, "With proximity - 1st result is the closest, even if its a slightly lower score");
        assert_eq!(result[1].entries[0].grid_entry.id, 1, "With proximity - 2nd result is farther away than 3rd but has a higher relevance");
        assert_eq!(result[2].entries[0].grid_entry.id, 2, "With proximity - 3rd is closer but has a lower relevance");
        assert_eq!(result[0], CoalesceContext {
            mask: 1 << 0,
            relev: 1.,
            entries: vec![CoalesceEntry {
                matches_language: true,
                idx: 1,
                tmp_id: 33554435,
                mask: 1 << 0,
                distance: 0.,
                scoredist: 1.5839497841387566,
                grid_entry: GridEntry {
                    id: 3,
                    x: 3,
                    y: 3,
                    relev: 1.,
                    score: 1,
                    source_phrase_hash: 0,
                }
            }],
        }, "With proximity - 1st result has expected properties");
        assert_eq!(result[1], CoalesceContext {
            mask: 1 << 0,
            relev: 1.,
            entries: vec![CoalesceEntry {
                matches_language: true,
                idx: 1,
                tmp_id: 33554433,
                mask: 1 << 0,
                distance: 2.8284271247461903,
                scoredist: 1.109893833332405,
                grid_entry: GridEntry {
                    id: 1,
                    x: 1,
                    y: 1,
                    relev: 1.,
                    score: 3,
                    source_phrase_hash: 0,
                }
            }],
        }, "With proximity - 2nd result has expected properties");
        assert_eq!(result[2], CoalesceContext {
            mask: 1 << 0,
            relev: 0.8,
            entries: vec![CoalesceEntry {
                matches_language: true,
                idx: 1,
                tmp_id: 33554434,
                mask: 1 << 0,
                distance: 1.4142135623730951,
                scoredist: 1.109893833332405, // Has the same scoredist as 2nd result because they're both beyond proximity radius
                grid_entry: GridEntry {
                    id: 2,
                    x: 2,
                    y: 2,
                    relev: 0.8,
                    score: 3,
                    source_phrase_hash: 0,
                }
            }],
        }, "With proximity - 2nd result has expected properties");

        // Test with bbox
        let match_opts = MatchOpts {
            zoom: 6,
            bbox: Some([1,1,1,1]),
            ..MatchOpts::default()
        };
        let result = coalesce(stack.clone(), &match_opts).unwrap();
        assert_eq!(result[0].entries.len(), 1, "With bbox - only one result is within the bbox, so only one result is returned");
        assert_eq!(result[0].entries[0].grid_entry.id, 1, "With bbox - result is the one that's within the bbox");
        assert_eq!(result[0], CoalesceContext {
            mask: 1 << 0,
            relev: 1.,
            entries: vec![CoalesceEntry {
                matches_language: true,
                idx: 1,
                tmp_id: 33554433,
                mask: 1 << 0,
                distance: 0.,
                scoredist: 3.,
                grid_entry: GridEntry {
                    id: 1,
                    x: 1,
                    y: 1,
                    relev: 1.,
                    score: 3,
                    source_phrase_hash: 0,
                }
            }],
        }, "With bbox - result has expected properties");

        // Test with bbox and proximity
        let match_opts = MatchOpts {
            zoom: 6,
            bbox: Some([1,1,1,1]),
            proximity: Some(Proximity {
                point: [1,1],
                radius: 40.,
            }),
        };
        let result = coalesce(stack.clone(), &match_opts).unwrap();
        assert_eq!(result[0].entries.len(), 1, "With bbox and prox - only one result is within the bbox, so only one result is returned");
           assert_eq!(result[0], CoalesceContext {
            mask: 1 << 0,
            relev: 1.,
            entries: vec![CoalesceEntry {
                matches_language: true,
                idx: 1,
                tmp_id: 33554433,
                mask: 1 << 0,
                distance: 0.,
                scoredist: 1.7322531402718835,
                grid_entry: GridEntry {
                    id: 1,
                    x: 1,
                    y: 1,
                    relev: 1.,
                    score: 3,
                    source_phrase_hash: 0,
                }
            }],
        }, "With bbox and prox - result has expected properties, including scoredist");
    }
    // TODO: test with more than one result within bbox, to make sure results are still ordered by proximity?
}

// TODO: language tests
// TODO: add proximity test with max score
// TODO: add sort tests?
