define(["jquery","jquery-ui","jquery-ui.extensions","ui.linearchooser","ui.clusterview"], function() {
 
    facet_computer = function(facet_counts){
        var facet_fields = facet_counts['facet_fields'];
        
        var facet_dict = {};
        var source_facets = facet_fields['sourceSpec'];
        var sourceName; 
        
        var L = source_facets.length / 2;
        var key, num, subsource;
        $.each(_.range(L),function(i){
            key = source_facets[2*i];
            num = source_facets[2*i + 1];
            sourceName = JSON.parse(key);
            var i, subsource;

  
            $.each(_.keys(sourceName),function(i,x){
               subsource = _.values(sourceName).slice(0,i+1).join('|');
               facet_dict[subsource] = (facet_dict[subsource] || 0) + num;
            })
        });
        
        var date_facets = facet_fields["dateDivisionsTight"];
        var L = date_facets.length / 2;
        var dateDivisions = new Array();
        $.each(_.range(L),function(i){
            key = date_facets[2*i];
            num = date_facets[2*i + 1];
            dateNames = key.split(' ')
            $.each(dateNames,function(j,val){
               if (num > 0){
                  facet_dict[val] = (facet_dict[val] || 0) + num;
                  dateDivisions.push(val);
               }
            })
        });       
        dateDivisions = _.uniq(dateDivisions);
        
        
        var space_facets = facet_fields["spatialDivisionsTight"];
        var L = space_facets.length / 2;
        var spatialDivisions = new Array();
        $.each(_.range(L),function(i){
            key = space_facets[2*i];
            num = space_facets[2*i + 1];
            if ((num > 0) & (key !== 'Undefined')) {
               facet_dict[key] =  (facet_dict[key] || 0) + num; 
               spatialDivisions.push(key);
            }
        });
        var spatialDivisions = _.uniq(spatialDivisions);
        
        return {'facet_dict' : facet_dict, 'spatialDivisions': spatialDivisions, 'dateDivisions': dateDivisions}
    };
    
	$.widget( "ui.resultsview", {
		options : {
			dataHandler : undefined,
			resultsRenderer : undefined,
			key : ' '
		},
		_init : function() {
			this.listenTo(this.options.dataHandler, "newResults", this.newResults);
		},
		newResults : function() {
			var self = this;
			var docs = self.options.dataHandler.docs;
			var facet_counts = self.options.dataHandler.facet_counts;
			var metadata = self.options.dataHandler.metadata;
			var query = self.options.query;
			
			//compute facets

			var facet_info = facet_computer(facet_counts);
			var facet_dict = facet_info['facet_dict'];
			var spatialDivisions = facet_info['spatialDivisions'];
			var dateDivisions = facet_info['dateDivisions'];
			
			var dateDivisionsWithFacets = _.map(dateDivisions,function(x){
			    return '<div class="innerDateChooser"><span class="value">' + x + '</span><span class="facet">' + facet_dict[x] + "</span></div>";
			
			});
			
			var spatialDivisionsWithFacets = _.map(spatialDivisions,function(x){
			    return '<div class="innerSpaceChooser"><span class="value">' + x + '</span><span class="facet">' + facet_dict[x] + "</span></div>";
			
			});
			
			
			
			var numFound = self.options.dataHandler.numFound;
			
		
			$("#subHeader").find('#filterBar').remove();

			var filterBar = $("<div id='filterBar'></div>").appendTo($("#subHeader"));
			
	
			self.element.find(".numFound").remove();
			$("<div class='numFound' id='totalNumFound'><div id='innerNumFound' style='font-size:30px'>" + numFound + "</div><div>Total Slices</div></div>").
				appendTo(filterBar)
			$('<div class="filterSeparator"></div>').appendTo(filterBar);	
				
			
			self.element.find(".linearChooser").remove();
			
			if (dateDivisionsWithFacets.length > 0){
				var dateChooser = $("<div class='linearChooser' id='dateChooser'></div>").
					appendTo(filterBar).
					linearchooser({
						data : {
							label : "<div style='width:120px'>Filter date by:</div>",
							list : [{label: "date", list: dateDivisionsWithFacets}],
						}
					});	
					
				dateChooser.find('.chooserSubElement').click(function(e){
					var num = parseInt($(e.currentTarget)[0].id);
					var filterval = dateDivisions[num];
					var filteritems = query.filteritems;
					var items = query.items;
					filteritems.push('dateDivisionsTight:"' + filterval + '"');
					query.update({'qval' : items, 'fqval' :filteritems});
					$(root).data()['statehandler'].changestate();			    
				
				});
				$('<div class="filterSeparator"></div>').appendTo(filterBar);
			}
			

			if (spatialDivisionsWithFacets.length > 0){
				var spaceChooser = $("<div class='linearChooser' id='spaceChooser'></div>").
					appendTo(filterBar).
					linearchooser({
						data : {
							label : "<div style='width:120px'>Filter space by:</div>",
							list : [{label: "space", list: spatialDivisionsWithFacets}],
						}
					});					
				
				spaceChooser.find('.chooserSubElement').click(function(e){
					var num = parseInt($(e.currentTarget)[0].id);
					var filterval = spatialDivisions[num];
					var filteritems = query.filteritems;
					var items = query.items;
					filteritems.push('spatialDivisionsTight:"' + filterval + '"');
					query.update({'qval' : items, 'fqval' :filteritems});
					$(root).data()['statehandler'].changestate();			    
				
				});		
				$('<div class="filterSeparator"></div>').appendTo(filterBar);
			}
			
			filterBar.find(".clusterTarget").remove();
			$("<div id='clusterTarget'></div>").appendTo(filterBar);
			
			self.element.find("hr").remove();
			$("<hr/>").appendTo(filterBar);
			
			self.element.find(".clusterView").remove();
			var view = $("<div class='clusterView' id='__'></div>").
									appendTo(this.element).
									clusterview({
											data : docs,
											metadata : metadata,
											resultsRenderer : self.options.resultsRenderer,
											collapsedict : self.options.collapsedict,
											key : self.options.key,
											hidedict : self.options.hidedict,
											facet_dict : facet_dict
											
									}).
									data("clusterview");
		    
		}
	});
	

});