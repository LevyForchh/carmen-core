extern crate cpp_build;

use std::env;
use std::path::{PathBuf, Path};
use std::process::Command;

fn main() {
    if !Path::new("carmen-cache/.git").exists() || !Path::new("protozero/.git").exists() {
        let _ = Command::new("git").args(&["submodule", "update", "--init"])
                                   .status();
    }

    cpp_build::Config::new()
        .include("protozero/include")

        .file("carmen-cache/src/coalesce.cpp")
        .file("carmen-cache/src/cpp_util.cpp")
        .file("carmen-cache/src/memorycache.cpp")
        .file("carmen-cache/src/rocksdbcache.cpp")

        .flag("-Wall")
        .flag("-Wextra")
        .flag("-Weffc++")
        .flag("-Wconversion")
        .flag("-pedantic")
        .flag("-Wconversion")
        .flag("-Wshadow")
        .flag("-Wfloat-equal")
        .flag("-Wuninitialized")
        .flag("-Wunreachable-code")
        .flag("-Wold-style-cast")
        .flag("-Wno-error=unused-variable")
        .flag("-Wno-error=unused-value")
        .flag("-std=c++14")
        .flag("-O3")

        .build("lib.rs");
}

