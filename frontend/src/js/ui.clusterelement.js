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
				hidedict = this.options.hidedict,
				renderer = this.options.renderer;
					
				var hide = hidedict.items[key] || false;	
				if (hide === true){
					classtext = "keyLabel hide";
				 
				} else {
					classtext = "keylabel";
				}				
				var top = $("<div class='topBar'></div>").appendTo(this.element);
				var keydiv = $("<div class='" + classtext + "'>" + key.split('|').slice(start).join(' >> ') + "</div>").appendTo(top);
				
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
   			    if (hide === true){
				   result_container.hide();
			    }				
					
				keydiv.click(function(){
				   result_container.toggle();
			
				   if (keydiv.hasClass("hide")){
				      keydiv.removeClass("hide");
				      hidedict.remove(key);
				   } else {
				      keydiv.addClass("hide");
				      hidedict.add(key,true);
				   }
				   $(root).data()['statehandler'].changestate();
				});
				
				
				
	   
	   },
	   destroy : function() {
			this.element.empty();
		}					
   });	
});					

