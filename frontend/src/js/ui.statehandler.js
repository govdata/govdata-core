define(["jquery","jquery-ui"], function() {

	$.widget( "ui.statehandler", {

		_create : function() {
		   var self = this, 
		   
		   objects = self.objects = self.options.objects;
		 
		   self.values = {}

		   $.each(objects,function(name,obj){
		      self.listenTo(obj,"update",function(){
		         self.values[name] = obj.items;
		         self.changehash();
		      });
		   });

		},
		setstate : function(state){
		   var self = this;
           state = state || {};	
           $.each(state,function(name,obj){
		        self.objects[name].update(obj);
           });

		},
		changehash : function(){    
		    var self = this;
		    $.address.jsonhash(self.values);
		},
		destroy : function() {}
	});

});


