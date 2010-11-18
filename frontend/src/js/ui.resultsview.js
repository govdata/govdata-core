define(["jquery","jquery-ui","jquery-ui.extensions","ui.clusterview"], function() {

	$.widget( "ui.resultsview", {
		options : {
			dataHandler : undefined,
			resultsRenderer : undefined,
			start : 0,
			collapse : 2
		},
		_init : function() {
			this.listenTo(this.options.dataHandler, "newResults", this.newResults);
		},
		newResults : function() {
			var self = this;
			var docs = this.options.dataHandler.docs;
			var metadata = this.options.dataHandler.metadata;
			var view = $("<div class='clusterview'></div>").
									appendTo(this.element).
									clusterview({
											data : docs,
											metadata : metadata,
											start : self.options.start,
											collapse : self.options.collapse,
											resultsRenderer : self.options.resultsRenderer
									}).
									data("clusterview");
		}
	});

});