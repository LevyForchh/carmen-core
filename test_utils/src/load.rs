use ::test_utils::load_db_from_json;
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        panic!("Expected 2 arguments: an output path and a gridstore")
    }
    load_db_from_json(&args[1], &args[2]);
}
