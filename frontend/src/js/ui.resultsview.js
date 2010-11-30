define(["jquery","jquery-ui","jquery-ui.extensions","ui.clusterview"], function() {

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
			var metadata = this.options.dataHandler.metadata;
			self.element.find(".clusterView").remove();
			var view = $("<div class='clusterView' id='__'></div>").
									appendTo(this.element).
									clusterview({
											data : docs,
											metadata : metadata,
											resultsRenderer : self.options.resultsRenderer,
											collapsedict : self.options.collapsedict,
											key : self.options.key
									}).
									data("clusterview");
		}
	});
	

});