extern crate carmen_core;
extern crate failure;
extern crate serde;
extern crate serde_json;

use carmen_core::gridstore::*;

use failure::Error;
use lz4::Decoder;
use rusoto_core::Region;
use rusoto_s3::{GetObjectRequest, S3Client, S3};
use serde::{Deserialize, Serialize};

use std::env;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, Read, Write, BufRead, BufWriter};
use std::path::{Path, PathBuf};
use std::sync::Arc;

// Util functions for tests and benchmarks

/// Round a float to a number of digits past the decimal point
pub fn round(value: f64, digits: i32) -> f64 {
    let multiplier = 10.0_f64.powi(digits);
    (value * multiplier).round() / multiplier
}

/// Convert an array of language ids into the langfield to use for GridKey or MatchKey
pub fn langarray_to_langfield(array: &[u32]) -> u128 {
    let mut out = 0u128;
    for lang in array {
        out = out | (1 << *lang as usize);
    }
    out
}

/// Mapping of GridKey to all of the grid entries to insert into a store for that GridKey
#[derive(Serialize, Deserialize, Debug)]
pub struct StoreEntryBuildingBlock {
    pub grid_key: GridKey,
    pub entries: Vec<GridEntry>,
}

/// Utility to create stores
/// Takes an vector, with each item mapping to a store to create
/// Each item is a vector with maps of grid keys to the entries to insert into the store for that grid key
pub fn create_store(store_entries: Vec<StoreEntryBuildingBlock>) -> GridStore {
    let directory: tempfile::TempDir = tempfile::tempdir().unwrap();
    let mut builder = GridStoreBuilder::new(directory.path()).unwrap();
    for build_block in store_entries {
        builder.insert(&build_block.grid_key, &build_block.entries).expect("Unable to insert");
    }
    builder.finish().unwrap();
    GridStore::new(directory.path()).unwrap()
}

// Gets the absolute path for a path relative to the carmen-core dir
pub fn get_absolute_path(relative_path: &Path) -> Result<PathBuf, Error> {
    let dir = env::current_dir().expect("Error getting current dir");
    let mut filepath = fs::canonicalize(&dir).expect("Error getting cannonicalized current dir");
    filepath.push(relative_path);
    Ok(filepath)
}

/// Load grid data from a local JSON path
pub fn load_db_from_json(json_path: &str, store_path: &str) {
    // Open json file
    let path = Path::new(json_path);
    let f = File::open(path).expect("Error opening file");
    let file = io::BufReader::new(f);

    load_db_from_json_reader(file, store_path);
}

fn load_db_from_json_reader<T: BufRead>(json_source: T, store_path: &str) {
    // Set up new gridstore
    let directory = Path::new(store_path);
    let mut builder = GridStoreBuilder::new(directory).unwrap();
    json_source.lines().for_each(|l| {
        let record = l.unwrap();
        if !record.is_empty() {
            let deserialized: StoreEntryBuildingBlock =
                serde_json::from_str(&record).expect("Error deserializing json from string");
            builder
                .insert(&deserialized.grid_key, &deserialized.entries)
                .expect("Unable to insert");
        }
    });
    builder.finish().unwrap();
}

/// Takes an absolute path (in string form) to a rocksdb dir, and an absolute path for the output file,
/// reads the data from the db, and writes a json representation of the data to a file
pub fn dump_db_to_json(store_path: &str, json_path: &str) {
    let reader = GridStore::new(store_path).unwrap();
    let output_file = File::create(json_path).unwrap();
    let mut writer = BufWriter::new(output_file);
    for item in reader.iter() {
        let (grid_key, entries) = item.unwrap();
        let key_record_pair = StoreEntryBuildingBlock { grid_key, entries };
        let line = serde_json::to_string(&key_record_pair).expect("Unable to serialize record");
        let bytes = line.as_bytes();
        writer.write(&bytes).unwrap();
        writer.write(b"\n").unwrap();
    }
}

pub fn ensure_downloaded(datafile: &str) -> PathBuf {
    let tmp = std::env::temp_dir().join("carmen_core_data/downloads");
    std::fs::create_dir_all(&tmp);
    let path = tmp.join(Path::new(datafile));
    if !path.exists() {
        let client = S3Client::new(Region::UsEast1);
        let request = GetObjectRequest {
            bucket: "mapbox".to_owned(),
            key: ("playground/apendleton/gridstore_bench/".to_owned() + datafile),
            ..Default::default()
        };
        println!("{:?}", &request);

        let result = client.get_object(request).sync().unwrap();

        let stream = result.body.unwrap();
        let mut body: Vec<u8> = Vec::new();
        stream.into_blocking_read().read_to_end(&mut body).unwrap();

        let mut file = File::create(&path).expect("create failed");
        file.write_all(&body).expect("failed to write body");
    }

    path
}

pub fn ensure_store(datafile: &str) -> PathBuf {
    let tmp = std::env::temp_dir().join("carmen_core_data/indexes");
    std::fs::create_dir_all(&tmp);
    let idx_path = tmp.join(Path::new(&datafile.replace(".dat.lz4", ".rocksdb")));
    if !idx_path.exists() {
        let dl_path = ensure_downloaded(datafile);
        let decoder = Decoder::new(File::open(dl_path).unwrap()).unwrap();
        let file = io::BufReader::new(decoder);
        load_db_from_json_reader(file, idx_path.to_str().unwrap());
    }

    idx_path
}

#[derive(Deserialize, Debug)]
struct SubqueryPlaceholder {
    store: String,
    weight: f64,
    match_key: MatchKey,
    idx: u16,
    zoom: u16,
    mask: u32,
}

fn load_stack(
    placeholders: &[SubqueryPlaceholder],
    stores: &mut HashMap<String, Arc<GridStore>>
) -> Vec<PhrasematchSubquery<Arc<GridStore>>> {
    placeholders.iter().map(|placeholder| {
        let store = stores.entry(placeholder.store.clone()).or_insert_with(|| {
            let store_name = placeholder.store
                .rsplit("/").next().unwrap()
                .replace(".rocksdb", ".dat.lz4");
            let store_path = ensure_store(&store_name);
            let gs = GridStore::new(store_path).unwrap();
            Arc::new(gs)
        });
        PhrasematchSubquery {
            store: store.clone(),
            weight: placeholder.weight,
            match_key: placeholder.match_key.clone(),
            idx: placeholder.idx,
            zoom: placeholder.zoom,
            mask: placeholder.mask,
        }
    }).collect()
}

pub fn prepare_coalesce_stacks(datafile: &str) ->
    Vec<(Vec<PhrasematchSubquery<Arc<GridStore>>>, MatchOpts)>
{
    let path = ensure_downloaded(datafile);
    let decoder = Decoder::new(File::open(path).unwrap()).unwrap();
    let file = io::BufReader::new(decoder);
    let mut stores: HashMap<String, Arc<GridStore>> = HashMap::new();
    let out: Vec<(Vec<PhrasematchSubquery<Arc<GridStore>>>, MatchOpts)> = file.lines().filter_map(|l| {
        let record = l.unwrap();
        if !record.is_empty() {
            let deserialized: (Vec<SubqueryPlaceholder>, MatchOpts) =
                serde_json::from_str(&record).expect("Error deserializing json from string");
            let stack = load_stack(&deserialized.0, &mut stores);
            Some((stack, deserialized.1))
        } else {
            None
        }
    }).collect();
    out
}

pub fn prepare_grouped_stacks(datafile: &str) ->
    Vec<Vec<(Vec<PhrasematchSubquery<Arc<GridStore>>>, MatchOpts)>>
{
    let path = ensure_downloaded(datafile);
    let decoder = Decoder::new(File::open(path).unwrap()).unwrap();
    let file = io::BufReader::new(decoder);
    let mut stores: HashMap<String, Arc<GridStore>> = HashMap::new();
    let out: Vec<Vec<(Vec<PhrasematchSubquery<Arc<GridStore>>>, MatchOpts)>> = file.lines().filter_map(|l| {
        let record = l.unwrap();
        if !record.is_empty() {
            let deserialized: Vec<(Vec<SubqueryPlaceholder>, MatchOpts)> =
                serde_json::from_str(&record).expect("Error deserializing json from string");
            Some(
                deserialized.into_iter().map(|(stack, opts)|
                    (load_stack(&stack, &mut stores), opts)
                ).collect()
            )
        } else {
            None
        }
    }).collect();
    out
}

/// Loads json from a file into a Vector of GridEntrys
/// The input file should be line-delimited JSON with all of the fields of a GridEntry
/// The path should be relative to the carmen-core directory
///
/// Example:
/// {"relev": 1, "score": 1, "x": 1, "y": 2, "id": 1, "source_phrase_hash": 0}
pub fn load_simple_grids_from_json(path: &Path) -> Result<Vec<GridEntry>, Error> {
    let dir = env::current_dir().expect("Error getting current dir");
    let mut filepath = fs::canonicalize(&dir).expect("Error getting cannonicalized current dir");
    filepath.push(path);
    let f = File::open(path).expect("Error opening file");
    let file = io::BufReader::new(f);
    let entries: Vec<GridEntry> = file
        .lines()
        .filter_map(|l| match l.unwrap() {
            ref t if t.len() == 0 => None,
            t => {
                let deserialized: GridEntry =
                    serde_json::from_str(&t).expect("Error deserializing json from string");
                Some(deserialized)
            }
        })
        .collect::<Vec<GridEntry>>();

    Ok(entries)
}