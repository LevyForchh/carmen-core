use std::fs::File;
use std::io::{self, BufRead};

use criterion::{Bencher, Criterion, Fun, BatchSize};
use lz4::Decoder;
use once_cell::unsync::Lazy;
use serde_json;
use tempfile;

use carmen_core::gridstore::*;
use test_utils::*;

pub fn benchmark(c: &mut Criterion) {
    let mut to_bench = Vec::new();

    to_bench.push(Fun::new("coalesce_global", move |b: &mut Bencher, _i| {
        let stacks = prepare_coalesce_stacks("gb_address_global.ljson.lz4");

        let mut cycle = stacks.iter().cycle();

        b.iter(|| {
            let (stack, opts) = cycle.next().unwrap();
            coalesce(stack.clone(), opts).unwrap();
        })
    }));

    to_bench.push(Fun::new("coalesce_prox", move |b: &mut Bencher, _i| {
        let stacks = prepare_coalesce_stacks("gb_address_with_proximity.ljson.lz4");

        let mut cycle = stacks.iter().cycle();

        b.iter(|| {
            let (stack, opts) = cycle.next().unwrap();
            coalesce(stack.clone(), opts).unwrap();
        })
    }));

    to_bench.push(Fun::new("coalesce_ac_global", move |b: &mut Bencher, _i| {
        let stacks = prepare_coalesce_stacks("gb_address_ac_global.ljson.lz4");

        let mut cycle = stacks.iter().cycle();

        b.iter(|| {
            let (stack, opts) = cycle.next().unwrap();
            coalesce(stack.clone(), opts).unwrap();
        })
    }));

    to_bench.push(Fun::new("coalesce_ac_prox", move |b: &mut Bencher, _i| {
        let stacks = prepare_coalesce_stacks("gb_address_ac_with_proximity.ljson.lz4");

        let mut cycle = stacks.iter().cycle();

        b.iter(|| {
            let (stack, opts) = cycle.next().unwrap();
            coalesce(stack.clone(), opts).unwrap();
        })
    }));

    let eur_records = Lazy::new(|| {
        let dl_path = ensure_downloaded("europen-place-both-740ed51f45-d775d2eb65.gridstore.dat.lz4");
        let decoder = Decoder::new(File::open(dl_path).unwrap()).unwrap();
        let file = io::BufReader::new(decoder);

        let records: Vec<StoreEntryBuildingBlock> = file.lines().filter_map(|l| {
            let record = l.unwrap();
            if record.is_empty() {
                None
            } else {
                Some(serde_json::from_str(&record).unwrap())
            }
        }).collect();
        records
    });
    to_bench.push(Fun::new("builder_insert", move |b: &mut Bencher, _i| {
        Lazy::force(&eur_records);

        let mut dir: Option<tempfile::TempDir> = None;
        let mut builder: Option<GridStoreBuilder> = None;
        let mut i = 0;

        b.iter(|| {
            if i == 0 {
                // every time we're at the beginning of the list, start a new builder
                // and throw away the old one
                dir.replace(tempfile::tempdir().unwrap());
                builder.replace(GridStoreBuilder::new(dir.as_mut().unwrap().path()).unwrap());
            }
            let record = &eur_records[i];
            builder.as_mut().unwrap().insert(&record.grid_key, &record.entries).unwrap();

            i = (i + 1) % (eur_records.len());
        })
    }));

    let asi_records = Lazy::new(|| {
        let dl_path = ensure_downloaded("asiopen_place_appends.ljson.lz4");
        let decoder = Decoder::new(File::open(dl_path).unwrap()).unwrap();
        let file = io::BufReader::new(decoder);

        let records: Vec<(GridKey, Vec<GridEntry>)> = file.lines().filter_map(|l| {
            let record = l.unwrap();
            if record.is_empty() {
                None
            } else {
                Some(serde_json::from_str(&record).unwrap())
            }
        }).collect();
        records
    });
    to_bench.push(Fun::new("builder_append_logged", move |b: &mut Bencher, _i| {
        Lazy::force(&asi_records);

        let mut dir: Option<tempfile::TempDir> = None;
        let mut builder: Option<GridStoreBuilder> = None;
        let mut i = 0;

        b.iter_batched(|| {}, |_| {
            if i == 0 {
                // every time we're at the beginning of the list, start a new builder
                // and throw away the old one
                dir.replace(tempfile::tempdir().unwrap());
                builder.replace(GridStoreBuilder::new(dir.as_mut().unwrap().path()).unwrap());
            }
            let record = &asi_records[i];
            builder.as_mut().unwrap().append(&record.0, &record.1).unwrap();

            i = (i + 1) % (asi_records.len());
        }, BatchSize::NumIterations(100_000))
    }));

    c.bench_functions("prod_data", to_bench, ());
}