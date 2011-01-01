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
				renderer = this.options.renderer,
				facet_dict = this.options.facet_dict;
					
				var hide = hidedict.items[key] || false;	
				if (hide === true){
					classtext = "keyLabel hide";
				 
				} else {
					classtext = "keyLabel";
				}				
				var facet_text;
/*				if (key in facet_dict){
					facet_text = " <span class='facet'>(" + facet_dict[key] + ")</span>";
				} else {
					facet_text = '';
				}			*/
				
				facet_text = ''
				
				var top = $("<div class='topBar'></div>").appendTo(this.element);
				var keytext;
				if (collapse != 'QUERY'){
                   keytext = "<div class='" + classtext + "'>" + key.split('|').slice(start).join(' > ') + facet_text +  "</div>"
                  
				} else{
     			   keytext = "<div class='" + classtext + "'>" + key.split('|').slice(start,-1).join(' > ') + facet_text +  "</div>"
				}
				
				var keydiv = $(keytext).appendTo(top);
				
				
/*				if (common){
				var innerchooser = $("<div class='linearChooser'></div>").appendTo(
				   top).linearchooser({
				       data : {
				          label : "Cluster by:",
				          list : [{label:"front",list:common[0]},{label:"back",list:common[1]}]
				       }
				   });
				}*/
				
				var result_container = renderer(this.element,results,collapse,facet_dict[key],key);
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

