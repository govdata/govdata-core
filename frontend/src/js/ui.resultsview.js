define(["jquery","jquery-ui","jquery-ui.extensions","ui.clusterview"], function() {
 
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

            $.each(_.keys(sourceName),function(i){
               subsource = _.values(sourceName).slice(0,i).join('__');
               facet_dict[subsource] = (facet_dict[subsource] || 0) + num;
            })
        });
        
        var date_facets = facet_fields["dateDivisionsTight"];
        var L = date_facets.length / 2;
        $.each(_.range(L),function(i){
            key = date_facets[2*i];
            num = date_facets[2*i + 1];
            dateNames = key.split(' ')
            $.each(dateNames,function(j,val){
               facet_dict[val] = (facet_dict[val] || 0) + num;
            })
        });       
        
        var space_facets = facet_fields["spatialDivisionsTight"];
        var L = space_facets.length / 2;
        $.each(_.range(L),function(i){
            key = space_facets[2*i];
            num = space_facets[2*i + 1];
            facet_dict[key] =  (facet_dict[key] || 0) + num;
        });   
        
        return facet_dict
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
			var docs = this.options.dataHandler.docs;
			var facet_counts = this.options.dataHandler.facet_counts;
			var metadata = this.options.dataHandler.metadata;
			
			//compute facets
			facet_dict = facet_computer(facet_counts);
			console.log(facet_dict)
			
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
											
									}).
									data("clusterview");
		    
		}
	});
	

});