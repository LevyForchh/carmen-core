use std::borrow::Borrow;
use std::path::Path;
use std::rc::Rc;

use criterion::{black_box, Bencher, Criterion, Fun};

use carmen_core::gridstore::*;
use test_utils::*;

pub fn benchmark(c: &mut Criterion) {
    // Make a vector to fill with closures to bench-test
    let mut to_bench = Vec::new();

    // Load data for coalesce single from json into a store
    let filepath = Path::new("benches/data/coalesce-bench-single-3848571113.json");
    let grid_entries = load_simple_grids_from_json(&filepath).unwrap();
    let store_single_rc = Rc::new(create_store(vec![StoreEntryBuildingBlock {
        grid_key: GridKey { phrase_id: 1, lang_set: 1 },
        entries: grid_entries,
    }]));

    let store_single = store_single_rc.clone();
    to_bench.push(Fun::new("coalesce_single", move |b: &mut Bencher, _i| {
        let subquery = PhrasematchSubquery {
            store: store_single.borrow(),
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

        b.iter(|| coalesce(black_box(stack.clone()), black_box(&match_opts)))
    }));

    let store_single = store_single_rc.clone();
    to_bench.push(Fun::new("coalesce_single_proximity", move |b: &mut Bencher, _i| {
        let subquery = PhrasematchSubquery {
            store: store_single.borrow(),
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
            proximity: Some(Proximity { point: [4893, 6001], radius: 40. }),
            ..MatchOpts::default()
        };

        b.iter(|| coalesce(black_box(stack.clone()), black_box(&match_opts)))
    }));

    let store_single = store_single_rc.clone();
    to_bench.push(Fun::new("coalesce_single_bbox", move |b: &mut Bencher, _i| {
        let subquery = PhrasematchSubquery {
            store: store_single.borrow(),
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
        let match_opts =
            MatchOpts { zoom: 14, bbox: Some([958, 1660, 958, 1665]), ..MatchOpts::default() };

        b.iter(|| coalesce(black_box(stack.clone()), black_box(&match_opts)))
    }));

    // Load data for coalesce multi from json into stores
    let filepath = Path::new("benches/data/coalesce-bench-multi-1965155344.json");
    let grid_entries = load_simple_grids_from_json(&filepath).unwrap();
    let store_multi1_rc = Rc::new(create_store(vec![StoreEntryBuildingBlock {
        grid_key: GridKey { phrase_id: 1, lang_set: 1 },
        entries: grid_entries,
    }]));

    let filepath = Path::new("benches/data/coalesce-bench-multi-3848571113.json");
    let grid_entries = load_simple_grids_from_json(&filepath).unwrap();
    let store_multi2_rc = Rc::new(create_store(vec![StoreEntryBuildingBlock {
        grid_key: GridKey { phrase_id: 2, lang_set: 1 },
        entries: grid_entries,
    }]));

    let store_multi1 = store_multi1_rc.clone();
    let store_multi2 = store_multi2_rc.clone();
    to_bench.push(Fun::new("coalesce_multi", move |b: &mut Bencher, _i| {
        let stack = vec![
            PhrasematchSubquery {
                store: store_multi1.borrow(),
                weight: 0.25,
                match_key: MatchKey {
                    match_phrase: MatchPhrase::Range { start: 1, end: 3 },
                    lang_set: 1,
                },
                idx: 0,
                zoom: 12,
                mask: 1 << 0,
            },
            PhrasematchSubquery {
                store: store_multi2.borrow(),
                weight: 0.75,
                match_key: MatchKey {
                    match_phrase: MatchPhrase::Range { start: 1, end: 3 },
                    lang_set: 1,
                },
                idx: 1,
                zoom: 12,
                mask: 1 << 1,
            },
        ];

        let match_opts = MatchOpts { zoom: 14, ..MatchOpts::default() };
        b.iter(|| coalesce(black_box(stack.clone()), black_box(&match_opts)))
    }));

    let store_multi1 = store_multi1_rc.clone();
    let store_multi2 = store_multi2_rc.clone();
    to_bench.push(Fun::new("coalesce_multi_proximity", move |b: &mut Bencher, _i| {
        let stack = vec![
            PhrasematchSubquery {
                store: store_multi1.borrow(),
                weight: 0.25,
                match_key: MatchKey {
                    match_phrase: MatchPhrase::Range { start: 1, end: 3 },
                    lang_set: 1,
                },
                idx: 0,
                zoom: 12,
                mask: 1 << 0,
            },
            PhrasematchSubquery {
                store: store_multi2.borrow(),
                weight: 0.75,
                match_key: MatchKey {
                    match_phrase: MatchPhrase::Range { start: 1, end: 3 },
                    lang_set: 1,
                },
                idx: 1,
                zoom: 12,
                mask: 1 << 1,
            },
        ];

        let match_opts = MatchOpts {
            zoom: 14,
            proximity: Some(Proximity { point: [4893, 6001], radius: 40. }),
            ..MatchOpts::default()
        };
        b.iter(|| coalesce(black_box(stack.clone()), black_box(&match_opts)))
    }));

    let store_multi1 = store_multi1_rc.clone();
    let store_multi2 = store_multi2_rc.clone();
    to_bench.push(Fun::new("coalesce_multi_bbox", move |b: &mut Bencher, _i| {
        let stack = vec![
            PhrasematchSubquery {
                store: store_multi1.borrow(),
                weight: 0.25,
                match_key: MatchKey {
                    match_phrase: MatchPhrase::Range { start: 1, end: 3 },
                    lang_set: 1,
                },
                idx: 0,
                zoom: 12,
                mask: 1 << 0,
            },
            PhrasematchSubquery {
                store: store_multi2.borrow(),
                weight: 0.75,
                match_key: MatchKey {
                    match_phrase: MatchPhrase::Range { start: 1, end: 3 },
                    lang_set: 1,
                },
                idx: 1,
                zoom: 12,
                mask: 1 << 1,
            },
        ];

        let match_opts =
            MatchOpts { zoom: 14, bbox: Some([958, 1660, 958, 1665]), ..MatchOpts::default() };
        b.iter(|| coalesce(black_box(stack.clone()), black_box(&match_opts)))
    }));

    let store_multi1 = store_multi1_rc.clone();
    let store_multi2 = store_multi2_rc.clone();
    to_bench.push(Fun::new("coalesce_multi_bbox_prox", move |b: &mut Bencher, _i| {
        let stack = vec![
            PhrasematchSubquery {
                store: store_multi1.borrow(),
                weight: 0.25,
                match_key: MatchKey {
                    match_phrase: MatchPhrase::Range { start: 1, end: 3 },
                    lang_set: 1,
                },
                idx: 1,
                zoom: 12,
                mask: 1 << 0,
            },
            PhrasematchSubquery {
                store: store_multi2.borrow(),
                weight: 0.75,
                match_key: MatchKey {
                    match_phrase: MatchPhrase::Range { start: 1, end: 3 },
                    lang_set: 1,
                },
                idx: 2,
                zoom: 14,
                mask: 1 << 1,
            },
        ];

        let match_opts = MatchOpts {
            zoom: 14,
            proximity: Some(Proximity { point: [4893, 6001], radius: 40. }),
            bbox: Some([3495, 5770, 4955, 6050]),
            ..MatchOpts::default()
        };
        b.iter(|| coalesce(black_box(stack.clone()), black_box(&match_opts)))
    }));

    c.bench_functions("coalesce", to_bench, ());
}
