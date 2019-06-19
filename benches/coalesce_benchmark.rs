#[macro_use]
extern crate criterion;

use criterion::black_box;
use criterion::Criterion;

use carmen_core::gridstore::*;
use test_utils::*;

fn criterion_benchmark(c: &mut Criterion) {
    let store = create_store(vec![StoreEntryBuildingBlock {
        grid_key: GridKey { phrase_id: 1, lang_set: 1 },
        entries: vec![
            GridEntry { id: 1, x: 200, y: 200, relev: 1., score: 1, source_phrase_hash: 0 }, // ne
            GridEntry { id: 2, x: 200, y: 0, relev: 1., score: 1, source_phrase_hash: 0 },   // se
            GridEntry { id: 3, x: 0, y: 0, relev: 1., score: 1, source_phrase_hash: 0 },     // sw
            GridEntry { id: 4, x: 0, y: 200, relev: 1., score: 1, source_phrase_hash: 0 },   // nw
        ],
    }]);

    let match_opts = MatchOpts {
        zoom: 14,
        proximity: Some(Proximity { point: [2, 2], radius: 1. }),
        ..MatchOpts::default()
    };

    c.bench_function("coalesce single 1", move |b| {
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
        b.iter(|| coalesce(black_box(stack.clone()), black_box(&match_opts)))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
