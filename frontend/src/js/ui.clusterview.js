define(["utils","jquery","jquery-ui","ui.linearchooser","ui.clusterelement"], function(utils) {
    function collapseData(data,metadata,collapse){

    	var collapseddata = {};
    	var sourcename;
    	
    	var end = collapse;
    
		_.each(data, function(datum) {

		if (end === 0){
		  sourcename = _.pluck(
			_.values(metadata[datum.collectionName].source),
				"name").join('|');
		}else{
 		  sourcename = _.pluck(
			_.values(metadata[datum.collectionName].source).slice(0,end),
				"name").join('|');
	    } 
				
				
	    collapseddata[sourcename] = (collapseddata[sourcename] || []);
		collapseddata[sourcename].push(datum);
		});
	    return collapseddata;		
    };
    
    
    function jqueryescape(str) {
      return str.replace(/([ #;&,.+*~\':"!^$[\]()=>|\/])/g,'\\$1') 
    };
      
    
    function renderclusters(collapsedata,collapsedict,metadata,start,collapse,context){
    
      var key, data, colnames, subcollapse, newmetadata, elt, newcommon, keyID;
      
      var container = $("<div class='elementContainer'></div>").appendTo(context.widget());
	  
	  $.each(collapsedata,function(key,data){
		subcollapse = collapsedict.items[key] || 0;    
		colnames = _.uniq(_.map(data,function(val){return val["collectionName"][0]; }));
		newmetadata = utils.subdict(metadata,colnames);
		
	    
		if (subcollapse === 0) {
		    if (colnames.length > 1){
		       newcommon = utils.computeCommon(newmetadata,collapse);	
		    } else {
		       newcommon = null;
		    }
		       
			elt = $("<div class='clusterElement'></div>").appendTo(container).
			clusterelement({
					 key : key,
					 results : data,
					 common : newcommon,
					 start : start,
					 collapse : collapse,
					 renderer: context.options.resultsRenderer
			});
			
   		    elt.find(".topBar .chooserSubElement").click(function(e){
   		
		    var num = parseInt($(e.target)[0].id);

		    if (num + 1 !== 0){

		      var subcollapse = collapse + num + 1;
		      collapsedict.items[key] = subcollapse;
		      var newelt = $("<div class='clusterView'></div>");
		      var clusterElement = $(e.target).closest(".clusterElement")
		      clusterElement.replaceWith(newelt);
		      var olddata = collapsedata[key];
		      var colnames = _.uniq(_.map(olddata,function(val){return val["collectionName"][0]; }));
		      var oldmetadata = utils.subdict(metadata,colnames);
		      newelt.clusterview({				    
		            key : key,
					data : olddata,
					metadata : oldmetadata,
					start : start,
					collapsedict : collapsedict,
					resultsRenderer : context.options.resultsRenderer  
			  });
		      
		      newelt.attr("id",key.replace(/ /g,'__'));
		       
		      
		    }
		    
		   
		  });			
											
		} else {
			elt = $("<div class='clusterView'></div>").
				appendTo(container).
				clusterview({
				    key : key,
					data : data,
					metadata : newmetadata,
					start : start,
					collapsedict : collapsedict,
					resultsRenderer : context.options.resultsRenderer
				});
			
		};
		keyID = key.replace(/ /g,'__');
		elt.attr("id",keyID);
	  });

      collapsedict._trigger("update",null);
      return container      
			
	 };
	  

   	$.widget( "ui.clusterview", {
		options : {
		    start : 0,
		},
		_create : function() {
			var self = this,
			    key = this.options.key,
			    data = this.options.data,
				metadata = this.options.metadata,
				collapsedict = this.options.collapsedict;
				start = this.options.start;
					
		
		    var collapse, collapseddata;

		    var common = utils.computeCommon(metadata,utils.count(key,'|'));
	        var commonL = common[0], commonR = common[1];
			
			var top = $("<div class='topBar'></div>").appendTo(this.element);
					
			var keydiv = $("<div class='keyLabel'>" + key.split('|').slice(start).join(' >> ') + "</div>").appendTo(top);
			
			var lc = $("<span class='linearChooser'></span>").
				appendTo(top).
				linearchooser({
					data : {
						label : "Cluster by:",
						list : [{label: "front", list: commonL},{label:"back",list:commonR}],
	                    resp : [_.range(commonL.length),_.range(-commonR.length,0)]
					}
				});
				
			lc.find(".chooserSubElement").click(function(e){
			    var num = parseInt($(e.target)[0].id);
			    var parentLabel = $(e.target).parent().parent()[0].id;

				collapse = utils.count(key,'|') + num + 1;
				collapsedict.items[key] = collapse;
				collapseddata = collapseData(data,metadata,collapse);
		
				var subkey;
				$.each(self.element.find(":ui-clusterelement, :ui-clusterview"),function(ind,item){
				   subkey = item.id.replace(/__/g,' ');
				   delete collapsedict[subkey]
				   $(item).remove();
				   
				});
				
		        var res = renderclusters(collapseddata,collapsedict,metadata,utils.count(key,'|'),collapse,self); 		
				keydiv.click(function(){
		          res.toggle();
		          if (keydiv.hasClass("hide")){
 			        keydiv.removeClass("hide");
		          } else {
			        keydiv.addClass("hide");
		          }
		        });
		
			});
			            
	      collapse = collapsedict.items[key] || 0
          collapseddata = collapseData(data,metadata,collapse);
          var res = renderclusters(collapseddata,collapsedict,metadata, utils.count(key,'|'),collapse,self);
		  keydiv.click(function(){
		     res.toggle();
		     if (keydiv.hasClass("hide")){
 			   keydiv.removeClass("hide");
		     } else {
			   keydiv.addClass("hide");
		     }
		  });
				         
     
		},
		destroy : function() {
			this.element.empty();
		}
	});
	
});