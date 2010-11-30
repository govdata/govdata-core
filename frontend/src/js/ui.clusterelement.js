define(["utils","jquery","jquery-ui","ui.linearchooser","ui.clusterelement"], function(utils) {


   $.widget( "ui.clusterelement", {
	   options : {
		},         
	   _create : function(){
	   			var self = this,
			    key = this.options.key,
				results = this.options.results,
				common = this.options.common,
				collapse = this.options.collapse,
				start = this.options.start,
				renderer = this.options.renderer;
				
				var top = $("<div class='topBar'></div>").appendTo(this.element);
				var key = $("<div class='keyLabel'>" + key.split('|').slice(start).join(' >> ') + "</div>").appendTo(top);
				
				if (common){
				var innerchooser = $("<div class='linearChooser'></div>").appendTo(
				   top).linearchooser({
				       data : {
				          label : "Cluster by:",
				          list : [{label:"front",list:common[0]},{label:"back",list:common[1]}]
				       }
				   });
				}
				
				var result_container = renderer(this.element,results,collapse);
				
				key.click(function(){
				   result_container.toggle();
				   if (key.hasClass("hide")){
				      key.removeClass("hide");
				   } else {
				      key.addClass("hide");
				   }
				});
				
				
				
	   
	   },
	   destroy : function() {
			this.element.empty();
		}					
   });	
});					

