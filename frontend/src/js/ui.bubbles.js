define(["jquery","jquery-ui"], function() {

	$.widget( "ui.bubbles", {
		add : function(val) {
		    var self = this;
			var bubbl = $("<div class='bubble'><span class='bubbletext'>"+val+"</span><span class='remover'>(x)</span></div>").
				appendTo(this.element);
			bubbl.find('.remover').click(function(){
			   bubbl.remove();   
			});
		},
		value : function() {
			return _.map(this.element.find(".bubbletext"), function(b) {
				return $(b).html();
			});
		},
		destroy : function() {
			this.element.find(".bubble").remove();
		}
	});

});