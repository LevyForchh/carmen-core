use std::borrow::Borrow;
use std::env;
use std::fs;
use std::rc::Rc;

use criterion::{black_box, Bencher, Criterion, Fun};

use carmen_core::gridstore::*;
use test_utils::*;

pub fn benchmark(c: &mut Criterion) {
    // make a vector to fill with closures to bench-test
    let mut to_bench = Vec::new();
    let path = env::current_dir().expect("Error getting current dir");
    // TODO: this may not be necessary
    let mut filepath = fs::canonicalize(&path).expect("Error getting cannonicalized current dir");

    // TODO: don't hard-code this?
    filepath.push("benches/data/decoded-coalesce-bench-multi-1965155344.json");

    // TODO error handling
    let grid_entries = load_grids_from_json(&filepath).unwrap();

    let store_rc = Rc::new(create_store(vec![StoreEntryBuildingBlock {
        grid_key: GridKey { phrase_id: 1, lang_set: 1 },
        entries: grid_entries,
    }]));

    let store = store_rc.clone();
    to_bench.push(Fun::new("coalesce_single", move |b: &mut Bencher, _i| {
        let subquery = PhrasematchSubquery {
            store: store.borrow(),
            weight: 1.,
            match_key: MatchKey {
                match_phrase: MatchPhrase::Range { start: 1, end: 3 },
                lang_set: 1,
            },
            idx: 1,
            zoom: 14,
            mask: 1 << 0,
        };
        let stack = vec![subquery.clone()];
        let match_opts = MatchOpts { zoom: 14, ..MatchOpts::default() };
        // this is the part that is timed
        b.iter(|| coalesce(black_box(stack.clone()), black_box(&match_opts)))
    }));

    let store = store_rc.clone();
    to_bench.push(Fun::new("coalesce_single_proximity", move |b: &mut Bencher, _i| {
        let subquery = PhrasematchSubquery {
            store: store.borrow(),
            weight: 1.,
            match_key: MatchKey {
                match_phrase: MatchPhrase::Range { start: 1, end: 3 },
                lang_set: 1,
            },
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
        //this is the part that is timed
        b.iter(|| coalesce(black_box(stack.clone()), black_box(&match_opts)))
    }));
    c.bench_functions("coalesce", to_bench, ());
}
