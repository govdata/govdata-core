define(["jquery","jquery-ui"], function() {

	$.widget( "ui.findresults", {
		options : {
			metadataFn : function() {}
		},
		newResults : function(data) {
			var self = this;
			this.results = data;
			this.docs = this.results.response.docs;
			this.facet_counts = this.results.facet_counts;
			this.numFound = this.results.response.numFound;
			this.options.metadataFn(this.docs, function(metadata) {
				self.metadata = metadata;
				
				self._trigger("newResults",null);
			});
		}
	});

});
