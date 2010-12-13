define(["jquery","jquery-ui"], function() {

	$.widget( "ui.bubbles", {
		add : function(val,type) {
		    var self = this;
		    var classtext;
		    if (type === 'fq'){
		        classtext = 'bubbletextfq'
		    } else {
		        classtext = 'bubbletext'
		    }
			var bubbl = $("<div class='bubble'><span class='" + classtext + "'>"+val+"</span><span class='remover'>(x)</span></div>").
				appendTo(this.element);
			bubbl.find('.remover').click(function(){
			   bubbl.remove();   
			});
		},
		value : function() {
			var qval = _.map(this.element.find(".bubbletext"), function(b) {
				return $(b).html();
			});
			var fqval = _.map(this.element.find(".bubbletextfq"), function(b) {
				return $(b).html();
			});
			return {'qval' : qval, 'fqval' : fqval}
		},
		destroy : function() {
			this.element.find(".bubble").remove();
		}
	});

});