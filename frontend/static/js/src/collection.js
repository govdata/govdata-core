/**
 * Must pass metaDataInitializer and queryTranslator in with the opts
 *
 **/
iv.Collection = function(opts) {
    this.metadata = opts.metadata;
    this.queryTranslator = opts.queryTranslator;
    this.countCalculator = opts.countCalculator;
    this._cacheInsertOrder = []; // where in the cache array is the object
    this._cache = []; // data cache
    this._countCache = {}; // query -> count cache object
};

iv.Collection.prototype.clear = function() {
    delete this._cache;
    this._cache = [];
    delete this._cacheInsertOrder;
    this._cacheInsertOrder = [];
}

iv.Collection.prototype.fetch = function(opts, callback) {
    // see if you have the data in cache first
    // else get the data dynamically (view on screen area plus buffer]]]])
    this.queryTranslator(opts, function(data) {
        callback(data);
    });
    // prune old data
};
