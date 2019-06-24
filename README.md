## carmen-core
Carmen-core is a backend storage library for [Carmen](https://github.com/mapbox/carmen) written in Rust. This repository contains both a Rust library (in `rust-src`) and a Javascript wrapper around that library (in `index.js` and `native`).

## Development

### Rust
The latest stable version of Rust to build this project.

To install rust, follow instructions at https://www.rust-lang.org/tools/install .

To compile, run:

```
cargo build
```

To run Rust tests, run:

```
cargo test
```

### JS bindings

As per above, the current stable Rust release must be installed to work on the Node bindings.

To get started, run:

```
yarn install
```

which will download all Javascript dependencies and, if one is available, download a prebuilt binary of the current version of the library from S3 for your platform.

If you make local changes, you can build locally using:

```
yarn build
```

Note that `carmen-core` requires node-gyp, which [only works with python 2](https://github.com/nodejs/node-gyp/issues/1337). If you use [pyenv](https://github.com/pyenv/pyenv) to manage python versions, the `.python-version ` file checked into this repo should be sufficient. Otherwise, try the instructions in [this comment](https://github.com/nodejs/node-gyp/issues/1337#issuecomment-370135532) (modified to use yarn):

```
brew install python@2
# follow the instructions printed after installing `python@2` to get python 2 on your PATH
# e.g. echo 'export PATH="/usr/local/opt/python@2/bin:$PATH"' >> ~/.bashrc
yarn config set python python2.7
```

To run Javascript tests, run:

```
yarn test
```

## Publishing

This project includes `script/publish.sh`, which publishes built binaries of the Javascript bindings of `carmen-core`. Generally, this script will be run automatically from Travis, and can be triggered with a special commit message.

Once you're ready to publish a binary (either a release or a development version):
* If a release-ready version, merge your branch into master
* Update the version number in `package.json` (for development versions, add a `-[your-branch-name]-1` tag after the number)
* Commit your changes with a commit message that includes `[publish binary]`
* Push and wait for Travis to run; the Javascript builders should include information about publishing builds at the end of the build log

## Benching
This project uses [Criterion](http://bheisler.github.io/criterion.rs/criterion/) benchmarks. Criterion is a statistics-driven benchmarking library that generates visual reports with [gnuplot](http://www.gnuplot.info/index.html). To enable the report generation, make sure you have gnuplot installed (`brew install gnuplot` on a mac).

To run benchmarks:
```
cargo bench
```

Html reports will be generated in `target/criterion/report/index.html`

Criterion will measure the statistical significance of the difference between two different bench runs, so to measure the impact of a change, you can checkout master, run a bench, and then check out a feature branch and run a bench. Note: the results are sensitive to other resource usage on your machine. For more accurate results, run in an isolated environment.

