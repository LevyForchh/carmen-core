use ::test_utils::dump_db_to_json;
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        panic!("Expected 2 arguments: a gridstore and an output path")
    }
    dump_db_to_json(&args[1], &args[2]);
}
