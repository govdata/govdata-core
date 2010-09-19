iv.Timeline = function(opts) {
    iv.Module.call(this,opts);
}

_.extend(iv.Timeline.prototype,iv.Module.prototype);

iv.Timeline.prototype.view = function() {
    return "timeline";
}