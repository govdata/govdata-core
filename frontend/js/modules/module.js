iv.Module = function(opts) {
    this.container = opts.container;
};

iv.Module.prototype.render = function() {
    this.container.innerHTML = this.view();
};

iv.Module.prototype.view = function() {
    return "no view created override this view prototype function";
};

