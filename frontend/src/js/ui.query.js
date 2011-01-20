define(["jquery","jquery-ui"], function() {

	$.widget( "ui.query", {
		options : {
		   items : [],
		   filteritems : []
		},
		update : function(valdict) {
			this.items = valdict["qval"];
			if (valdict["fqval"] !== undefined){
			   this.filteritems = valdict["fqval"];
			}
			this._trigger("update",null);
			this.submit();
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
		value : function(){
			var self = this;
			return {'qval': self.items , 'fqval': self.filteritems}
		},
		destroy : function() {}
	});

});
