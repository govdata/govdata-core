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

	//$.Widget.prototype._init = function() {
		//this.id = _.uniqueId();
	//}

	$.Widget.prototype.listenTo = function(target, type, callback) {
		var self = this;
		target.element.bind(
			(target.widgetEventPrefix + type).toLowerCase(),
			function() {
				callback.apply(self, this.arguments);
			}
		);
	}

})(jQuery);
