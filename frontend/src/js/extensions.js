// underscore and jquery extensions

// underscore extensions
(function() {
	var root = this;
	_.mixin({
		provide : function(ns) {
			var names = ns.split(".");
			var base = root;
			_.each(names, function(name) {
				base = base[name] = base[name] || {};
			});
		}
	});
})();


(function( $, undefined ) {
	var root = this;
})(jQuery);
