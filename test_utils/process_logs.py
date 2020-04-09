import sys, json, re

for f in sys.argv[1:]:
    started = False
    indexes = set()

    outf = open(re.sub(r"\.log$", "", f) + ".ljson", 'w')
    for row in open(f):
        if not started:
            if "dev server ready" in row:
                started = True
            continue
        if row.startswith("PHRASEMATCH"):
            row = re.sub("^PHRASEMATCH ", "", row)
            phrasematches = json.loads(row)[0]
            for pm in phrasematches:
                indexes.add(pm['store']['path'])
            print(row.strip(), file=outf)
    
    idx_file = open(re.sub(r"\.log$", "", f) + ".idx", 'w')
    print('\n'.join(sorted(indexes)), file=idx_file)