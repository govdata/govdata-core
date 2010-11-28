define(["jquery","jquery-ui","ui.bubbles"], function() {

	$.widget( "ui.searchbar", {
		_create : function() {
			var self = this,
			o = self.options,
			el = self.element,
			bubbles = $("<div class='bubbles'></div>").
										bubbles().
										appendTo(el).
										data("bubbles"),
			input = $("<input type='text' />").
								appendTo(el).
								bind('keydown', 'return', function() {
									var val = _.trim(input.val());
									if(val === "") {
										self.submit();
									} else {
										self.addBubble(val);
										input.val("");
									}
									return false;
								});
			this.bubbles = bubbles;
			this.q = this.options.query;
			this.listenTo(this.q, "update", this.update)
			//$(document).bind("queryupdate", this.update);
			//_.listenTo(this.q, "update", this.update, this);
			//self.q.bind("queryupdate", this.update);
		},
		update : function() {

			var items = this.q.items;
			var self = this;
			this.element.find(".bubble").remove();
			$.each(items,function(ind,item){
			   self.addBubble(item);
			
			});

			
		},
		submit : function() {
			var value = this.bubbles.value();
			this.q.update(value);
			
		},
		addBubble : function(val) {
			this.bubbles.add(val);
		},
		destroy : function() {
			this.element.empty();
		}
	});
	
});

