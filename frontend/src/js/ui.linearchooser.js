
define(["text!templates/linearchooser.html","jquery",
	"jquery-ui","underscore"], function(tmpl) {

	$.widget( "ui.linearchooser", {
			options: {
			},
			_create : function() {
				var html = "";
				var self = this;
				if (this.options.data.resp === undefined) {
						this.options.data.resp = _.map(this.options.data.list,
																						function(elt){
																							return _.range(elt.list.length); 
																						});			
				}
				$(_.template(tmpl, this.options.data)).appendTo(this.element);

			},
			destroy : function() {
				this.element.empty();
			}
	});

});
