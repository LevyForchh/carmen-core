extern crate carmen_core;
extern crate failure;
extern crate serde;
extern crate serde_json;

use carmen_core::gridstore::*;
use failure::Error;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs::{self, File};
use std::io::Write;
use std::io::{self, BufRead, BufWriter};
use std::path::{Path, PathBuf};

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

/// Loads json from a file into a Vector of GridEntrys
/// The input file should be line-delimited JSON with all of the fields of a GridEntry
/// The path should be an absolute path
///
/// Example:
/// {"relev": 1, "score": 1, "x": 1, "y": 2, "id": 1, "source_phrase_hash": 0}
pub fn load_grids_from_json_to_store(path_name: &String) -> Result<GridStore, Error> {
    // Open json file
    let path = Path::new(path_name);
    let f = File::open(path).expect("Error opening file");
    let file = io::BufReader::new(f);

    // Set up new gridstore
    let directory: tempfile::TempDir = tempfile::tempdir().unwrap();
    let mut builder = GridStoreBuilder::new(directory.path()).unwrap();
    file.lines().for_each(|l| {
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
    Ok(GridStore::new(directory.path()).unwrap())
}

/// Takes an absolute path (in string form) to a rocksdb dir, and an absolute path for the output file,
/// reads the data from the db, and writes a json representation of the data to a file
pub fn dump_db_to_json(input_path: &String, output_path: &String) {
    let reader = GridStore::new(input_path).unwrap();
    let output_file = File::create(output_path).unwrap();
    let mut writer = BufWriter::new(output_file);
    let keys = reader.keys();
    for key in keys {
        let grid_key = key.unwrap();
        let record: Vec<_> = reader.get(&grid_key).unwrap().unwrap().collect();
        let key_record_pair = StoreEntryBuildingBlock { grid_key: grid_key, entries: record };
        let line =
            serde_json::to_string(&key_record_pair).expect("Unable to serialize record") + "\n";
        let bytes = line.as_bytes();
        writer.write(&bytes).unwrap();
    }
}
