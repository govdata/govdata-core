define(["jquery","jquery-ui"], function() {

	$.widget( "ui.dict", {
		
		_create : function() {
		  self = this;
		  self.items = self.options.items;
		  self.widgetprefix = self.options.prefix;
		  return self;
		},
		update : function(val){
		  self = this;
		  self.items = val;
		},
		remove : function(val){
		  self = this;
		  delete self.items[val];
		  self._trigger("update",null); 
		
		},
		add : function(key,val){
		  self = this;
		  self.items[key] = val;
		  self._trigger("update",null);
		  
		},
		value : function (){
			var self = this;
			return this.items
		},
		destroy : function() {}
	});

});


