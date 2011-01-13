define(["jquery","jquery-ui"], function() {

	$.widget( "ui.statehandler", {
		_create : function() {
		   var self = this
		   self.objects = self.options.objects;		 
		   
		   self.values = {}


		},
        changestate : function(){
           var self = this;   
           objects = self.objects;
		   $.each(objects,function(name,obj){		
		         self.values[name] = obj.value();	      
		      });    
		   $.address.jsonhash(self.values);
		
		},
		setstate : function(state){
		   var self = this;
           $.each(state,function(name,obj){     
		        self.objects[name].update(obj);
           });

		},
		destroy : function() {}
	});

});


