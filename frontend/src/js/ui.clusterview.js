define(["utils","jquery","jquery-ui","ui.linearchooser","ui.clusterelement"], function(utils) {
    function collapseData(data,metadata,start,collapse){

    	var collapseddata = {};
    	var sourcename;
    	
    	
    	var end = start + collapse;
    
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
	  
	  $.each(collapsedata,function(key,data){
	    collapsedict[key] = collapsedict[key] || 0;
		subcollapse = collapsedict[key];      
		colnames = _.uniq(_.map(data,function(val){return val["collectionName"][0]; }));
		newmetadata = utils.subdict(metadata,colnames);
	
		if (subcollapse === 0) {
		    if (colnames.length > 1){
		       newcommon = utils.computeCommon(newmetadata,start + collapse);	
		    } else {
		       newcommon = null;
		    }
		       
			elt = $("<div class='clusterElement'></div>").appendTo(context.widget()).
			clusterelement({
					 key : key,
					 results : data,
					 common : newcommon,
					 collapse : collapse,
					 renderer: context.options.resultsRenderer
			});
			
   		    elt.find(".topBar .chooserSubElement").click(function(e){
   		
		    var num = parseInt($(e.target)[0].id);
		   
		    console.log(num)
		    if (num + 1 !== 0){
		      var subcollapse = num + 1;
		      collapsedict[key] = subcollapse;
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
					start : start + collapse,
					collapsedict : collapsedict,
					resultsRenderer : context.options.resultsRenderer  
			  });
		      
		      newelt.attr("id",key.replace(/ /g,'__'));
		       
		      
		    }
		    
		   
		  });			
											
		} else {
			elt = $("<div class='clusterView'></div>").
				appendTo(context.widget()).
				clusterview({
				    key : key,
					data : data,
					metadata : newmetadata,
					start : start + collapse,
					collapsedict : collapsedict,
					resultsRenderer : context.options.resultsRenderer
				});
			
		};
		keyID = key.replace(/ /g,'__');
		elt.attr("id",keyID);
	  });

            
	  
			
	 };
	  

   	$.widget( "ui.clusterview", {
		options : {
		    start : 0,
		    collapsedict : {" ":2},
		    key : " "
		},
		_create : function() {
			var self = this,
			    key = this.options.key,
			    data = this.options.data,
				metadata = this.options.metadata,
			    start = this.options.start,
				collapsedict = this.options.collapsedict;
					
					
		
		    var collapse, collapseddata;

		    var common = utils.computeCommon(metadata,start);
	        var commonL = common[0], commonR = common[1];
			
			var lc = $("<div class='linearChooser'></div>").
				appendTo(this.widget()).
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

				collapse = num + 1;
				collapseddata = collapseData(data,metadata,start,collapse);
		
				var subkey;
				$.each(self.element.find(":ui-clusterelement, :ui-clusterview"),function(ind,item){
				   subkey = item.id.replace(/__/g,' ');
				   delete collapsedict[subkey]
				   $(item).remove();
				   
				});
				
				
		        renderclusters(collapseddata,collapsedict,metadata,start,collapse,self); 		
		
			});
			            
	      collapse = collapsedict[key]
          collapseddata = collapseData(data,metadata,start,collapse);
          renderclusters(collapseddata,collapsedict,metadata,start,collapse,self);
          
     
		},
		destroy : function() {
			this.element.empty();
		}
	});
	
});