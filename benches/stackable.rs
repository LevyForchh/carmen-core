use lz4::Decoder;
use serde_json;
use std::fs::File;
use std::io::{self, BufRead};
use tempfile;

use criterion::{black_box, Bencher, Criterion, Fun};

use carmen_core::gridstore::MatchPhrase::Range;
use carmen_core::gridstore::*;
use std::collections::HashSet;
use test_utils::*;

pub fn benchmark(c: &mut Criterion) {
    let mut to_bench = Vec::new();
    let directory: tempfile::TempDir = tempfile::tempdir().unwrap();
    let mut builder = GridStoreBuilder::new(directory.path()).unwrap();
    let dl_path = ensure_downloaded("us_midwest_new.gridstore.dat.lz4");
    let decoder = Decoder::new(File::open(dl_path).unwrap()).unwrap();
    let file = io::BufReader::new(decoder);

    let us_records: Vec<StoreEntryBuildingBlock> = file
        .lines()
        .filter_map(|l| {
            let record = l.unwrap();
            if record.is_empty() {
                None
            } else {
                Some(serde_json::from_str(&record).unwrap())
            }
        })
        .collect();
    for record in us_records {
        builder.insert(&record.grid_key, record.entries).expect("Unable to insert record");
    }
    builder.finish().unwrap();
    let store = GridStore::new(directory.path()).unwrap();

    to_bench.push(Fun::new("stackable_single", move |b: &mut Bencher, _i| {
        let a1 = PhrasematchSubquery {
            id: 0,
            store: &store,
            weight: 0.5,
            match_key: MatchKey { match_phrase: Range { start: 0, end: 1 }, lang_set: 3 },
            mask: 1,
        };

        let phrasematch_results = vec![a1.clone()];

        b.iter(|| {
            stackable(
                black_box(&phrasematch_results),
                black_box(None),
                black_box(0),
                black_box(HashSet::new()),
                black_box(0),
                black_box(129),
                black_box(0.0),
                black_box(0),
            )
        })
    }));

    c.bench_functions("stackable", to_bench, ());
}
