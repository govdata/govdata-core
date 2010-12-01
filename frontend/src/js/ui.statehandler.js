define(["jquery","jquery-ui"], function() {

	$.widget( "ui.statehandler", {

		_create : function() {
		   var self = this, 
		   
		   objects = self.objects = self.options.objects;
		 
		   self.values = {}

		   $.each(objects,function(name,obj){
		      self.listenTo(obj,"update",function(){
		         self.values[name] = obj.items;
		         if (!(self.SETTING)){
   		           $.address.jsonhash(self.values);
   		         }
		      });
		   });

		},
		setstate : function(state){
		   var self = this;
           state = state || {};	
           self.SETTING=true;
           $.each(state,function(name,obj){
		        self.objects[name].update(obj);
		        //self.objects[name].items = obj;
           });
           self.SETTING=false;

		},
		destroy : function() {}
	});

});


