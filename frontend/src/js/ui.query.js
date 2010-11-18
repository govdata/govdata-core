define(["jquery","jquery-ui"], function() {

	$.widget( "ui.query", {
		update : function(value) {
			this.items = value;
			this._trigger("update",null);
			this.submit();
		},
		submit: function() {
			var params = { q : this.items.join(" ") };
			var self = this;
			this.options.submitFn(params, function(data) {
				self._trigger("newData",null,data);
			});
		},
		_create : function() {
			this.items = [];
		},
		destroy : function() {}
	});

});