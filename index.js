var addon = require('./native');

// wire up iterator creation from the JS side
addon.GridStore.prototype.keys = function() {
    const out = {};
    out[Symbol.iterator] = () => new addon.GridStoreKeyIterator(this);
    return out;
}

module.exports = addon;
