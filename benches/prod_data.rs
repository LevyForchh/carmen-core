use std::borrow::Borrow;
use std::path::Path;
use std::rc::Rc;

use criterion::{black_box, Bencher, Criterion, Fun};

use carmen_core::gridstore::*;
use test_utils::*;

pub fn benchmark(c: &mut Criterion) {
    let mut to_bench = Vec::new();

    to_bench.push(Fun::new("coalesce_global", move |b: &mut Bencher, _i| {
        let stacks = prepare_coalesce_stacks("gb_address_global.ljson.lz4");

        let mut cycle = stacks.iter().cycle();

        b.iter(|| {
            let (stack, opts) = cycle.next().unwrap();
            coalesce(stack.clone(), opts);
        })
    }));

    to_bench.push(Fun::new("coalesce_prox", move |b: &mut Bencher, _i| {
        let stacks = prepare_coalesce_stacks("gb_address_with_proximity.ljson.lz4");

        let mut cycle = stacks.iter().cycle();

        b.iter(|| {
            let (stack, opts) = cycle.next().unwrap();
            coalesce(stack.clone(), opts);
        })
    }));

    to_bench.push(Fun::new("coalesce_ac_global", move |b: &mut Bencher, _i| {
        let stacks = prepare_coalesce_stacks("gb_address_ac_global.ljson.lz4");

        let mut cycle = stacks.iter().cycle();

        b.iter(|| {
            let (stack, opts) = cycle.next().unwrap();
            coalesce(stack.clone(), opts);
        })
    }));

    to_bench.push(Fun::new("coalesce_ac_prox", move |b: &mut Bencher, _i| {
        let stacks = prepare_coalesce_stacks("gb_address_ac_with_proximity.ljson.lz4");

        let mut cycle = stacks.iter().cycle();

        b.iter(|| {
            let (stack, opts) = cycle.next().unwrap();
            coalesce(stack.clone(), opts);
        })
    }));

    c.bench_functions("prod_data", to_bench, ());
}