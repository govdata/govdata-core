iv.Module = function(opts) {
    this.container = opts.container;
    console.log(this.container);
    this.collection = opts.collection;
    this.metadata = this.collection.metadata;
    this.render();
};

iv.Module.prototype.render = function() {
    this.container.innerHTML = this.view();
};

iv.Module.prototype.view = function() {
    return "no view created override this view prototype function";
};

