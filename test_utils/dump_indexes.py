import sys, subprocess

for f in open(sys.argv[1]).read().strip().split('\n'):
    outfile = "out/" + f.split("/")[-1].replace(".rocksdb", ".dat")
    cmd = "cargo run --release --bin dump_store".split(" ") + [f, outfile]
    subprocess.run(cmd)