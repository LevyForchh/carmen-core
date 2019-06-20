#[macro_use]
extern crate criterion;

use criterion::Criterion;

mod coalesce;

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = coalesce::benchmark
}
criterion_main!(benches);
