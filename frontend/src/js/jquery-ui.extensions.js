
define(["jquery-ui"], function() {

		$.Widget.prototype.listenTo = function(target, type, callback) {
			var self = this;
			target.element.bind(
				(target.widgetEventPrefix + type).toLowerCase(),
				function() {
					callback.apply(self, this.arguments);
				}
			);
		}
		
});


