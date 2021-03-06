define(["jquery","jquery-ui"], function() {

	$.widget( "ui.bubbles", {
		add : function(val,type) {
			var self = this;
			var classtext;
			var classb;
			if (type === 'fq'){
				classtext = 'bubbletextfq';
				classb = 'fqbubble';
			} else {
				classtext = 'bubbletext';
				classb = 'qbubble';
			}
			var bubbl = $("<li class='bubble " + classb +"'><span class='" + classtext + "'>"+val+"</span><span class='remover'>&otimes;</span></li>").
				appendTo(this.element);
			bubbl.find('.remover').click(function(){
			   bubbl.remove();   
			   
			});
			
			return bubbl
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
