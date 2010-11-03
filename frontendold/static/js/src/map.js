iv.Map = function(collection) {
    iv.Module.call(this,collection);
};

_.extend(iv.Map.prototype,iv.Module.prototype);

iv.Map.prototype.view = function() {
    return "Map";
};

