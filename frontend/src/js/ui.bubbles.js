define(["jquery","jquery-ui"], function() {

	$.widget( "ui.bubbles", {
		add : function(val) {
			$("<div class='bubble'>"+val+"</div>").
				appendTo(this.element);
		},
		value : function() {
			return _.map(this.element.children(), function(b) {
				return $(b).html();
			});
		},
		destroy : function() {
			this.element.find(".bubble").remove();
		}
	});

});