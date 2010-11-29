define(["utils","jquery","jquery-ui","ui.linearchooser","ui.clusterelement"], function(utils) {
    function collapseData(data,metadata,start,collapse){

    	var collapseddata = {};
		_.each(data, function(datum) {
		var sourcename = _.pluck(
			_.values(metadata[datum.collectionName].source).slice(start,start+collapse),
				"name").join('|');
	    collapseddata[sourcename] = (collapseddata[sourcename] || []);
		collapseddata[sourcename].push(datum);
		});
	    return collapseddata;		
    };
    
    
    function jqueryescape(str) {
      return str.replace(/([ #;&,.+*~\':"!^$[\]()=>|\/])/g,'\\$1') 
    };
      
    
    function replacecluster(key,data,metadata,start,collapse,subcollapse,context){

		var subcollapseVal = subcollapse[key]; 

		var colnames = _.uniq(_.map(data,function(val){return val["collectionName"]; }));
		
		var newmetadata = utils.subdict(metadata,colnames);
		var newelt ;
		
		var keyID = key.replace(/ /g,'__');
		
 
		if (subcollapseVal === 0) {				    
			newcommon = utils.computeCommon(newmetadata,start + collapse);
			
			newelt = $("<div class='clusterElement'></div>");
			
			$(context.widget()).find("#" + keyID).replaceWith(newelt);
			
			newelt.clusterelement({
					 key : key,
					 results : collapseddata,
					 common : newcommon,
					 collapse : collapse,
					 renderer: context.options.resultsRenderer
			});
			
											
		} else {
		    newelt = $("<div class='clusterView'></div>");
		    
		    var oldelt = $(context.widget()).find('#' + jqueryescape(keyID));
		    oldelt.replaceWith(newelt);
    		    
			newelt.clusterview({
					data : data,
					metadata : newmetadata,
					start : start,
					collapse : collapse + subcollapseVal,
					subcollapse : subcollapse,
					resultsRenderer : context.options.resultsRenderer
				});



			
		};
		newelt.attr("id",keyID);    
    
    };
    
    function renderclusters(collapseddata,metadata,start,collapse,subcollapse,context){
            var key, subcollapseVal, colnames, newmetadata, elt,newcommon, keyID;

            
			for (key in collapseddata) {

			    keyID = key.replace(/ /g,'__');	
			   			
                subcollapseVal = subcollapse[key];      
                colnames = _.uniq(_.map(collapseddata[key],function(val){return val["collectionName"]; }));
				newmetadata = utils.subdict(metadata,colnames);
				if (subcollapseVal === 0) {				    
					newcommon = utils.computeCommon(newmetadata,start + collapse);
					
                    elt = $("<div class='clusterElement'></div>").appendTo(context.widget()).
                    clusterelement({
                             key : key,
                             results : collapseddata[key],
                             common : newcommon,
                             collapse : collapse,
                             renderer: context.options.resultsRenderer
                    });
                    
                               						
				} else {
					elt = $("<div class='clusterView'></div>").
						appendTo(context.widget()).
						clusterview({
							data : collapseddata[key],
							metadata : newmetadata,
							start : start + collapse,
							collapse : subcollapseVal,
							subcollapse : subcollapse,
							resultsRenderer : context.options.resultsRenderer
						});
					
				};
				elt.attr("id",keyID);
			};
	 };

	$.widget( "ui.clusterview", {
		options : {
		},
		_create : function() {
			var self = this,
			    data = this.options.data,
					metadata = this.options.metadata,
					start = this.options.start,
					collapse = this.options.collapse,
					subcollapse = this.options.subcollapse;

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

				collapse=num+1;
				self.element.find('.clusterElement, .clusterView').remove();

				var collapseddata = collapseData(data,metadata,start,collapse);
				renderclusters(collapseddata,metadata,start,collapse,subcollapse,self);			
	
			});
			            
            var collapseddata = collapseData(data,metadata,start,collapse);
                    
            $.each(collapseddata,function(key){
               subcollapse[key] = 0;
            });
                     
            renderclusters(collapseddata,metadata,start,collapse,subcollapse,self);
            
            $(this.element).find(".clusterElement .chooserSubElement").click(function(e){
               var clusterElement = $(e.target).closest(".clusterElement");
			   var num = parseInt($(e.target)[0].id);
			   var key = clusterElement.attr("id").replace(/__/g,' ');
			   subcollapse[key] = num+1;
			   	   
			   replacecluster(key,collapseddata[key],metadata,start,collapse,subcollapse,self)
			   
            });
            

  
		},
		destroy : function() {
			this.element.empty();
		}
	});
	
});