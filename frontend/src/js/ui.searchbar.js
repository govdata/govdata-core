define(["jquery","jquery-ui","ui.bubbles"], function() {

	$.widget( "ui.searchbar", {
		_create : function() {
			var self = this,
			o = self.options,
			autocomplete = o.autocomplete,
			el = self.element;
			
			var elt_container = $("<div id='searchEltContainer'></div>").appendTo(el);
			
			bubbles = $("<ul class='bubbles'></ul>").
										bubbles().
										appendTo(elt_container).
										data("bubbles"),
			input = $("<input type='text' />").
								appendTo(elt_container);
			input.unbind('keydown');
			input.autocomplete(autocomplete)
			input.bind('keydown', 'return', function() {
									var val = _.trim(input.val());

									if(val === "") {
										self.submit();
									} else {
										self.addBubble(val);
										input.val("");
										self.submit();
									}
									return false;
								});
		
		    
			this.bubbles = bubbles;
			this.q = this.options.query;
			this.listenTo(this.q, "update", this.update)
	
		},
		update : function() {
            var self = this;
			var items = self.q.items;


			self.element.find(".bubble").remove();
			$.each(items,function(ind,item){
			   self.addBubble(item);
			
			});	
			var filteritems = self.q.filteritems;
			$.each(filteritems,function(ind,item){
			   self.addBubble(item,'fq');
			
			});
			
		},
		submit : function() {
			var value = this.bubbles.value();
			this.q.update(value);
			$(root).data()['statehandler'].changestate();
			
		},
		addBubble : function(val,type) {
		    var self = this;
			var bubble = this.bubbles.add(val,type);
			bubble.find('.remover').click(function(){
			    self.submit();
			});

									         
		},
		destroy : function() {
			this.element.empty();
		}
	});
	
});

