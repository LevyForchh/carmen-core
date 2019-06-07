'use strict';

const addon = require('../../');
const tape = require('tape');
const tmp = require('tmp');
const rimraf = require('rimraf').sync;

tape('JsGridbuilderBuilder init', (t) => {
    const tmpDir = tmp.dirSync();
    t.throws(() => new addon.GridbuilderBuilder(), 'not enough arguments');
    t.throws(() => new addon.GridbuilderBuilder({}), 'throws on wrong argument type');
    t.throws(() => new addon.GridbuilderBuilder(7), 'throws on wrong argument type');
    const builder = new addon.GridbuilderBuilder(tmpDir.name);
    t.ok(builder);
    rimraf(tmpDir.name);
    t.end();
});

tape('GridbuilderBuilder insert()', (t) => {
    const tmpDir = tmp.dirSync();
    const builder = new addon.GridbuilderBuilder(tmpDir.name);
    t.throws(() => builder.insert(), 'not enough arguments');
    t.throws(() => builder.insert({}), 'not enough arguments');
    builder.insert({ phrase_id: 0, lang_set: [0] }, [{ id: 0, x: 0, y: 0, relev: 0.5, score: 2, source_phrase_hash: 0 }]);
    builder.insert({ phrase_id: 1, lang_set: [0, 1, 2, 3] }, [{ id: 2, x: 2, y: 2, relev: 0.6, score: 3, source_phrase_hash: 0 }]);
    builder.finish();

    const reader = new addon.Gridbuilder(tmpDir.name);
    t.deepEquals(reader.get({ phrase_id: 0, lang_set: [0] }), [ { relev: 1, score: 2, x: 0, y: 0, id: 0, source_phrase_hash: 0 } ], 'able to find the correct gridEntry inserted by insert()');
    t.deepEquals(reader.get({ phrase_id: 1, lang_set: [0, 1, 2, 3] }), [ { relev: 0.6, score: 3, x: 2, y: 2, id: 2, source_phrase_hash: 0 } ], 'able to find the correct gridEntry inserted by insert()');
    t.notOk(reader.get({ phrase_id: 3, lang_set: [3] }), 'cannot retrieve a grid that has not been inserted');
    t.end();
});

tape('GridbuilderBuilder append()', (t) => {
    const tmpDir = tmp.dirSync();
    const builder = new addon.GridbuilderBuilder(tmpDir.name);
    t.throws(() => builder.append(), 'not enough arguments');
    t.throws(() => builder.append({}), 'not enough arguments');
    builder.insert({ phrase_id: 0, lang_set: [0] }, [{ id: 0, x: 0, y: 0, relev: 0.5, score: 2, source_phrase_hash: 0 }]);
    builder.append({ phrase_id: 1, lang_set: [0, 2] }, [{ id: 2, x: 2, y: 2, relev: 0.8, score: 3, source_phrase_hash: 0 }]);
    builder.finish();

    const reader = new addon.Gridbuilder(tmpDir.name);
    let list = Array.from(reader.keys());
    t.deepEquals(list, [ { phrase_id: 0, lang_set: [ 0 ] }, { phrase_id: 1, lang_set: [ 0, 2 ] } ], 'Gridbuilder contains the key inserted and the key appended');
    t.end();
});

tape('GridbuilderBuilder finish()', (t) => {
    const tmpDir = tmp.dirSync();
    const builder = new addon.GridbuilderBuilder(tmpDir.name);
    builder.insert({ phrase_id: 0, lang_set: [0] }, [{ id: 0, x: 0, y: 0, relev: 0.5, score: 2, source_phrase_hash: 0 }]);
    builder.insert({ phrase_id: 1, lang_set: [0, 1, 2, 3] }, [{ id: 2, x: 2, y: 2, relev: 0.8, score: 3, source_phrase_hash: 0 }]);
    t.throws(() => new addon.Gridbuilder(tmpDir.name), 'throws if you attempt to read without calling finish()');

    builder.finish();
    const reader = new addon.Gridbuilder(tmpDir.name);
    t.ok(reader, 'can read only after finish() is called');
    t.end();
});

tape('Gridbuilder reader', (t) => {
    const tmpDir = tmp.dirSync();
    const builder = new addon.GridbuilderBuilder(tmpDir.name);
    builder.insert({ phrase_id: 0, lang_set: [0] }, [{ id: 0, x: 0, y: 0, relev: 0.5, score: 2, source_phrase_hash: 0 }]);
    builder.insert({ phrase_id: 1, lang_set: [0, 1, 2, 3] }, [{ id: 2, x: 2, y: 2, relev: 0.8, score: 3, source_phrase_hash: 0 }]);
    builder.finish();

    const reader = new addon.Gridbuilder(tmpDir.name);
    let list = Array.from(reader.keys());
    t.deepEquals(list, [ { phrase_id: 0, lang_set: [ 0 ] }, { phrase_id: 1, lang_set: [ 0, 1, 2, 3 ] } ], 'Gridbuilder is able to retrieve keys, reader works as expected');
    t.end();
});
