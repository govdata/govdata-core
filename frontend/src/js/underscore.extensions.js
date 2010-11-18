
define(["underscore"], function() {
	  _.mixin({
			provide : function(ns) {
				var names = ns.split(".");
				var base = root;
				_.each(names, function(name) {
					base = base[name] = base[name] || {};
				});
			}
		});
});
