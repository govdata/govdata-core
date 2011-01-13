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
				var facet_text = "<div class='facetCount'>(" + facet_dict[key] + " Slices)</div>";
				var top = $("<div class='topBar'></div>").appendTo(this.element);
				
				var keyList;
				if (collapse != 'QUERY'){
                   keyList =  key.split('|').slice(start);
                  
				} else{
     			   keyList = key.split('|').slice(start,-1);
				}				
				
				$.each(keyList,function(ind,val){
				    keyList[ind] = '<span class="value">' + val + '</span>';
				});
	
				$.each(keyList.slice(0,-1),function(ind,val){
				    keyList[ind] = keyList[ind] + ' > ';
				});

                var outerkeydiv = $("<div class='outerkeydiv'></div>").appendTo(top);
                var keytoggle =  $("<div class='keyToggle'>&#9660</div>").appendTo(outerkeydiv);
                
				var keydiv = $("<div class='linearChooser " + classtext + "'></div>").
						appendTo(outerkeydiv).
						linearchooser({
							data : {
								label : "",
								list : [{label: "key", list: keyList}],
								resp : [_.range(keyList.length)]
							}
						});
						
				var facetCount = $(facet_text).appendTo(outerkeydiv);
			
	
	
				
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
					
					
				keytoggle.click(function(){
				   result_container.toggle();
			
				   if (keydiv.hasClass("hide")){
				      keydiv.removeClass("hide");
				      hidedict.remove(key);
				      
				      keytoggle.html("<div class='keyToggle'>&#9660</div>");
				   } else {
				      keydiv.addClass("hide");
				      hidedict.add(key,true);
				      keytoggle.html("<div class='keyToggle'>&#9654</div>");
				   }
                   
				   $(root).data()['statehandler'].changestate();
				});
				
				
				
	   
	   },
	   destroy : function() {
			this.element.empty();
		}					
   });	
});					

