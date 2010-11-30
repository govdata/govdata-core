define(["jquery","jquery-ui"], function() {

	$.widget( "ui.query", {
	    options : {
	       items : [],
	       filteritems : []
	    },
		update : function(value,filtervalue) {
			this.items = value;
			this._trigger("update",null);
			this.submit();
			if (filtervalue !== undefined){
			   this.filteritems = filtervalue;
			}
		},
		submit: function() {
			var params = { q : this.items.join(" AND "), fq : this.filteritems};
			var self = this;
			this.options.submitFn(params, function(data) {
				self._trigger("newData",null,data);
			});
		},
		_create : function() {
			this.items = this.options.items;
			this.filteritems = this.options.filteritems;
		},
		destroy : function() {}
	});

});