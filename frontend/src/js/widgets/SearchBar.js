(function( $, undefined ) {

$.widget( "ui.searchbar", {
	options : {
	},
	_create : function() {
		var self = this,
		o = self.options,
		el = self.element,
		bubbles = $("<div class='bubbles'></div>").
									bubbles().
									appendTo(el),
		input = $("<input type='text' />").
							appendTo(el).
							bind('keydown', 'return', function() {
								var val = _.trim(input.val());
								if(val === "") {
									console.log("make query");
									console.log(self._query());
									var q = self._query().join(" ");
									self._trigger("query",null,{q : q});
								} else {
									self.addBubble(val);
									input.val("");
								}
								return false;
							});
		self.bubbles = bubbles;
	},
	addBubble : function(val) {
		this.bubbles.bubbles("add",val);
	},
	_query : function() {
		return this.bubbles.bubbles("value");
	},
	destroy : function() {
		this.element.empty();
	}
});

$.widget( "ui.bubbles", {
	options : {
	},
	_create : function() {
	},
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
		this.element.empty();
	}
});

})(jQuery);
