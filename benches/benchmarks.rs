#[macro_use]
extern crate criterion;

use criterion::Criterion;

mod coalesce;
mod prod_data;
mod stackable;

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = stackable::benchmark
}
criterion_main!(benches);
