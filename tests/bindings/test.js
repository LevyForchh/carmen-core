'use strict';

const addon = require('../../');
const tape = require('tape');
const tmp = require('tmp');
const rimraf = require('rimraf').sync;

tape('JsGridStoreBuilder init', (t) => {
    const tmpDir = tmp.dirSync();
    t.throws(() => new addon.GridStoreBuilder(), 'not enough arguments');
    t.throws(() => new addon.GridStoreBuilder({}), 'throws on wrong argument type');
    t.throws(() => new addon.GridStoreBuilder(7), 'throws on wrong argument type');
    const builder = new addon.GridStoreBuilder(tmpDir.name);
    t.ok(builder);
    rimraf(tmpDir.name);
    t.end();
});

tape('GridStoreBuilder insert()', (t) => {
    const tmpDir = tmp.dirSync();
    const builder = new addon.GridStoreBuilder(tmpDir.name);
    t.throws(() => builder.insert(), 'not enough arguments');
    t.throws(() => builder.insert({}), 'not enough arguments');
    builder.insert({ phrase_id: 0, lang_set: [0] }, [{ id: 0, x: 0, y: 0, relev: 0.5, score: 2, source_phrase_hash: 0 }]);
    builder.insert({ phrase_id: 1, lang_set: [0, 1, 2, 3] }, [{ id: 2, x: 2, y: 2, relev: 0.6, score: 3, source_phrase_hash: 0 }]);
    builder.finish();

    const reader = new addon.GridStore(tmpDir.name);
    t.deepEquals(reader.get({ phrase_id: 0, lang_set: [0] }), [ { relev: 1, score: 2, x: 0, y: 0, id: 0, source_phrase_hash: 0 } ], 'able to find the correct gridEntry inserted by insert()');
    t.deepEquals(reader.get({ phrase_id: 1, lang_set: [0, 1, 2, 3] }), [ { relev: 0.6, score: 3, x: 2, y: 2, id: 2, source_phrase_hash: 0 } ], 'able to find the correct gridEntry inserted by insert()');
    t.notOk(reader.get({ phrase_id: 3, lang_set: [3] }), 'cannot retrieve a grid that has not been inserted');
    t.end();
});

tape('GridStoreBuilder append()', (t) => {
    const tmpDir = tmp.dirSync();
    const builder = new addon.GridStoreBuilder(tmpDir.name);
    t.throws(() => builder.append(), 'not enough arguments');
    t.throws(() => builder.append({}), 'not enough arguments');
    builder.insert({ phrase_id: 0, lang_set: [0] }, [{ id: 0, x: 0, y: 0, relev: 0.5, score: 2, source_phrase_hash: 0 }]);
    builder.append({ phrase_id: 1, lang_set: [0, 2] }, [{ id: 2, x: 2, y: 2, relev: 0.8, score: 3, source_phrase_hash: 0 }]);
    builder.finish();

    const reader = new addon.GridStore(tmpDir.name);
    let list = Array.from(reader.keys());
    t.deepEquals(list, [ { phrase_id: 0, lang_set: [ 0 ] }, { phrase_id: 1, lang_set: [ 0, 2 ] } ], 'GridStore contains the key inserted and the key appended');
    t.end();
});

tape('GridStoreBuilder finish()', (t) => {
    const tmpDir = tmp.dirSync();
    const builder = new addon.GridStoreBuilder(tmpDir.name);
    builder.insert({ phrase_id: 0, lang_set: [0] }, [{ id: 0, x: 0, y: 0, relev: 0.5, score: 2, source_phrase_hash: 0 }]);
    builder.insert({ phrase_id: 1, lang_set: [0, 1, 2, 3] }, [{ id: 2, x: 2, y: 2, relev: 0.8, score: 3, source_phrase_hash: 0 }]);
    t.throws(() => new addon.GridStore(tmpDir.name), 'throws if you attempt to read without calling finish()');

    builder.finish();
    const reader = new addon.GridStore(tmpDir.name);
    t.ok(reader, 'can read only after finish() is called');
    t.end();
});

tape('GridStore reader', (t) => {
    const tmpDir = tmp.dirSync();
    const builder = new addon.GridStoreBuilder(tmpDir.name);
    builder.insert({ phrase_id: 0, lang_set: [0] }, [{ id: 0, x: 0, y: 0, relev: 0.5, score: 2, source_phrase_hash: 0 }]);
    builder.insert({ phrase_id: 1, lang_set: [0, 1, 2, 3] }, [{ id: 2, x: 2, y: 2, relev: 0.8, score: 3, source_phrase_hash: 0 }]);
    builder.finish();

    const reader = new addon.GridStore(tmpDir.name);
    let list = Array.from(reader.keys());
    t.deepEquals(list, [ { phrase_id: 0, lang_set: [ 0 ] }, { phrase_id: 1, lang_set: [ 0, 1, 2, 3 ] } ], 'GridStore is able to retrieve keys, reader works as expected');
    t.end();
});

tape('Coalesce tests - invalid inputs', (t) => {
    t.throws(() => {
        addon.coalesce();
    }, 'throws, incorrect arguments');

    t.throws(() => {
        addon.coalesce([]);
    }, 'throws, incorrect arguments');

    t.throws(() => {
        addon.coalesce([], {} );
    }, 'throws, incorrect arguments');

    t.throws(() => {
        addon.coalesce([{}], {}, () => {} );
    }, 'throws, incorrect arguments');

    t.throws(() => {
        addon.coalesce([-1], {}, () => {} );
    }, 'throws, incorrect argument type');

    t.throws(() => {
        addon.coalesce(undefined, {}, () => {} );
    }, 'throws, incorrect argument type');

    t.throws(() => {
        addon.coalesce([], {}, () => {} );
    }, 'throws, incorrect argument type');

    t.throws(() => {
        addon.coalesce([undefined], {}, () => {} );
    }, 'throws, incorrect argument type');

    t.throws(() => {
        addon.coalesce([null], {}, () => {} );
    }, 'throws, incorrect argument type');

    const tmpDir = tmp.dirSync();
    const builder = new addon.GridStoreBuilder(tmpDir.name);
    builder.insert({ phrase_id: 1, lang_set: [1] }, [{ id: 1, x: 2, y: 2, relev: 1., score: 1, source_phrase_hash: 0 }]);
    builder.finish();

    const reader = new addon.GridStore(tmpDir.name);

    // no weight
    const no_weight = [{
        store: reader,
        match_key: {
            lang_set: [0],
            match_phrase: {
                "Range": {
                    start: 1,
                    end: 2
                }
            }
        },
        idx: 2,
        zoom: 6,
        mask: 1 << 1,
    }];
    t.throws(() => {addon.coalesce(no_weight, {}, () => {})}, 'no weight assigned in stack');

    // invalid type for gridstore
    const no_store = [{
        store: 'x',
        weight: 0.5,
        match_key: {
            lang_set: [0],
            match_phrase: {
                "Range": {
                    start: 1,
                    end: 2
                }
            }
        },
        idx: 2,
        zoom: 6,
        mask: 1 << 1,
    }];
    t.throws(() => {addon.coalesce(no_store, {}, () => {})}, /failed downcast to JsGridStore/, 'invalid stack');

    const no_match_key = [{
        store: reader,
        weight: 0.5,
        idx: 2,
        zoom: 6,
        mask: 1 << 1,
    }];
    t.throws(() => {addon.coalesce(no_match_key, {}, () => {})}, 'no match_key');

    const no_idx = [{
        store: reader,
        weight: 0.5,
        match_key: {
            lang_set: [0],
            match_phrase: {
                "Range": {
                    start: 1,
                    end: 2
                }
            }
        },
        zoom: 6,
        mask: 1 << 1,
    }];
    t.throws(() => {addon.coalesce(no_idx, {}, () => {})}, 'no idx');

    const no_mask = [{
        store: reader,
        weight: 0.5,
        match_key: {
            lang_set: [0],
            match_phrase: {
                "Range": {
                    start: 1,
                    end: 2
                }
            }
        },
        zoom: 6,
    }];
    t.throws(() => {addon.coalesce(no_idx, {}, () => {})}, 'no mask');
    t.end();
});

tape('Coalesce single valid stack - Valid inputs', (t) => {
    const tmpDir = tmp.dirSync();
    const builder = new addon.GridStoreBuilder(tmpDir.name);
    builder.insert({ phrase_id: 1, lang_set: [1] },
        [
            { id: 1, x: 2, y: 2, relev: 1., score: 1, source_phrase_hash: 0 },
            { id: 2, x: 2, y: 0, relev: 1., score: 1, source_phrase_hash: 0 },
            { id: 3, x: 0, y: 0, relev: 1., score: 1, source_phrase_hash: 0 }
        ]
    );
    builder.finish();
    const reader = new addon.GridStore(tmpDir.name);

    const valid_stack = [{
        store: reader,
        weight: 1,
        match_key: {
            lang_set: [1],
            match_phrase: {
                "Range": {
                    start: 1,
                    end: 3
                }
            }
        },
        idx: 1,
        zoom: 14,
        mask: 1 << 0,
    }];

    addon.coalesce(valid_stack, { zoom: 14 }, (err, res) => {
        t.deepEqual(res[0].entries,
            [
                { grid_entry:
                    { relev: 1, score: 1, x: 2, y: 2, id: 1, source_phrase_hash: 0 },
                    matches_language: true,
                    idx: 1,
                    tmp_id: 33554433,
                    mask: 1,
                    distance: 0,
                    scoredist: 1
                }
            ], 'Ok, finds the right grid entry');
        t.equal(res.length, 3, 'Result set has 3 grid entries');
        t.end();
    });
});

tape('Coalesce multi valid stack - Valid inputs', (t) => {
    const tmpDir1 = tmp.dirSync();
    const builder1 = new addon.GridStoreBuilder(tmpDir1.name);
    builder1.insert({ phrase_id: 1, lang_set: [1] },
        [
            { id: 1, x: 1, y: 1, relev: 1., score: 1, source_phrase_hash: 0 }
        ]
    );
    builder1.finish();
    const reader1 = new addon.GridStore(tmpDir1.name);

    const tmpDir2 = tmp.dirSync();
    const builder2 = new addon.GridStoreBuilder(tmpDir2.name);
    builder2.insert({ phrase_id: 2, lang_set: [1] },
        [
            { id: 1, x: 1, y: 1, relev: 1., score: 3, source_phrase_hash: 0 },
            { id: 2, x: 2, y: 2, relev: 1., score: 3, source_phrase_hash: 0 }
        ]
    );
    builder2.finish();
    const reader2 = new addon.GridStore(tmpDir2.name);

    const valid_coalesce_multi = [{
        store: reader1,
        weight: 0.5,
        match_key: {
            lang_set: [1],
            match_phrase: {
                "Range": {
                    start: 1,
                    end: 3
                }
            }
        },
        idx: 0,
        zoom: 1,
        mask: 1 << 1,
    },{
        store: reader2,
        weight: 0.5,
        match_key: {
            lang_set: [1],
            match_phrase: {
                "Range": {
                    start: 1,
                    end: 3
                }
            }
        },
        idx: 1,
        zoom: 2,
        mask: 1 << 0,
    }];
    addon.coalesce(valid_coalesce_multi, { zoom: 2 }, (err, res) => {
        t.deepEqual(res[0].entries[0].grid_entry, { relev: 0.5, score: 3, x: 2, y: 2, id: 2, source_phrase_hash: 0 }, '1st result highest score from the higher zoom index');
        t.deepEqual(res[0].entries.length, 2, 'Result set has 2 grid entries');
        t.end();
    });
});

tape('lang_set >= 128', (t) => {
    const tmpDir = tmp.dirSync();
    const builder = new addon.GridStoreBuilder(tmpDir.name);
    builder.insert({ phrase_id: 1, lang_set: [1] },
        [
            { id: 1, x: 2, y: 2, relev: 1., score: 1, source_phrase_hash: 0 }
        ]
    );
    builder.finish();
    const reader = new addon.GridStore(tmpDir.name);

    const lang_set_stack = [{
        store: reader,
        weight: 1,
        match_key: {
            lang_set: [128],
            match_phrase: {
                "Range": {
                    start: 1,
                    end: 3
                }
            }
        },
        idx: 1,
        zoom: 14,
        mask: 1 << 0,
    }];

    addon.coalesce(lang_set_stack, { zoom: 14 }, (err, res) => {
        t.deepEqual(res[0].entries[0].matches_language, false, 'does not match language - ignore lang_set > 128');
        t.deepEqual(res[0].relev, 0.96, 'relevance penalised - ignore lang_set > 128');
    });
    t.end();
});
