'use strict';

const addon = require('../native');
const tape = require('tape');
const tmp = require('tmp');

tape('JsGridStoreBuilder init', (t) => {
    const tmpDir = tmp.dirSync();
    t.throws(() => new addon.JsGridStoreBuilder(), 'not enough arguments');
    const store = new addon.JsGridStoreBuilder(tmpDir.name);
    t.ok(store);
    t.end();
});

tape('JsGridStoreBuilder insert', (t) => {
    const tmpDir = tmp.dirSync();
    const store = new addon.JsGridStoreBuilder(tmpDir.name);
    t.throws(() => grid.insert(), 'not enough arguments');
    const id = {
        phrase_id: 1,
        lang_set: [0, 1, 2, 3]
    };
    const entries = [{ id: 2, x: 2, y: 2, relev: 0.8, score: 3, source_phrase_hash: 0 }];
    store.insert(id, entries);
    t.ok(store);
    t.end();
});
