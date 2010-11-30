define(["jquery","jquery-ui"], function() {

	$.widget( "ui.statehandler", {
	    
		_create : function() {
		   var self = this;
           objects = self.options.objects;

		   self.values = {};

		
		   $.each(objects,function(name,obj){
		      self.values[name] = obj.items;
		      
		      self.listenTo(obj,"update",function(){
		         self.values[name] = obj.items;
		         self.changehash();
		      });
		   });

		},
		changehash : function(){
		    var self = this;

		    $.address.jsonhash(this.values)
		},
		destroy : function() {}
	});

});