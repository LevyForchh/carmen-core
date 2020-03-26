use std::borrow::Borrow;
use std::path::Path;
use std::rc::Rc;

use criterion::{black_box, Bencher, Criterion, Fun};

use carmen_core::gridstore::*;
use test_utils::*;
use std::collections::HashSet;

// let store2 = create_store(
// vec![StoreEntryBuildingBlock {
//     grid_key: GridKey { phrase_id: 2, lang_set: 1 },
//     entries: vec![
//         GridEntry { id: 3, x: 0, y: 0, relev: 1., score: 1, source_phrase_hash: 0 },
//         GridEntry { id: 4, x: 50, y: 50, relev: 1., score: 1, source_phrase_hash: 0 },
//     ],
// }],
// 2,
// 6,
// 1,
// HashSet::new(),
// 200.

pub fn benchmark(c: &mut Criterion) {
    let mut to_bench = Vec::new();
    let filepath = Path::new("benches/data/coalesce-bench-single-3848571113.json");
    let grid_entries = load_simple_grids_from_json(&filepath).unwrap();
    let grid_store_rc = Rc::new(create_store(vec![StoreEntryBuildingBlock {
        grid_key: GridKey { phrase_id: 1, lang_set: 1 },
        entries: grid_entries,
    }], 2, 6, 1, HashSet::new(), 200.));

    let grid_store: = grid_store_rc.clone();

    to_bench.push(Fun::new("single_stack", move |b: &mut Bencher, _i| {
        let stack_a1 = PhrasematchSubquery {
            id: 1,
            store: store_single.borrow(),
            weight: 0.33,
            match_key: MatchKey {
                match_phrase: MatchPhrase::Range { start: 1, end: 3 },
                lang_set: 1,
            },
            mask: 1,
        };
        let phrasematch_results = vec![stack_a1.clone()];

        b.iter(|| stackable(black_box(&phrasematch_results.clone()), black_box(None), black_box(0), black_box(HashSet::new()), black_box(0), black_box(129), black_box(0.0), black_box(0)))
    }));
}
