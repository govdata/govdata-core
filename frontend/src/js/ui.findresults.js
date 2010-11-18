define(["jquery","jquery-ui"], function() {

	$.widget( "ui.findresults", {
		options : {
			metadataFn : function() {}
		},
		newResults : function(data) {
			console.log("new results");
			console.log(data);
			var self = this;
			this.results = data;
			this.docs = this.results.response.docs;
			this.options.metadataFn(this.docs, function(metadata) {
				self.metadata = metadata;
				self._trigger("newResults",null);
			});
		}
	});

});