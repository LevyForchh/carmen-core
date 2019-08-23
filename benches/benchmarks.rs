#[macro_use]
extern crate criterion;

use criterion::Criterion;

mod coalesce;
mod prod_data;

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = coalesce::benchmark, prod_data::benchmark
}
criterion_main!(benches);
