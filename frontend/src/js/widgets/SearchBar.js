(function( $, undefined ) {

$.widget( "ui.query", {
	update : function(value) {
		this.items = value;
		this._trigger("update",null);
		this.submit();
	},
	submit: function() {
		var params = { q : this.items.join(" ") };
		var self = this;
		this.options.submitFn(params, function(data) {
			self._trigger("newData",null,data);
		});
	},
	_create : function() {
		this.items = [];
	},
	destroy : function() {}
});

$.widget( "ui.findResults", {
	options : {
		metadataFn : function() {}
	},
	newResults : function(data) {
		console.log("new results");
		console.log(data);
		var self = this;
		this.results = data;
		this.docs = this.results.response.docs;
		this.options.metadataFn(this.docs, function(metadata) {
			self.metadata = metadata;
			self._trigger("newResults",null);
		});
	}
});

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
		console.log(this);
		console.log(this.options.query);
		var items = this.q.items;
		// update the bubbles
		//var query = q.q.split(" ");
		console.log("update the bubbles");
	},
	submit : function() {
		console.log("make query");
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


$.widget( "ui.resultsView", {
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
		var view = $("<div class='clusterView'></div>").
								appendTo(this.element).
								clusterView({
										data : docs,
										metadata : metadata,
										start : self.options.start,
										collapse : self.options.collapse,
										resultsRenderer : self.options.resultsRenderer
								}).
								data("clusterView");
	}
});

$.widget( "ui.clusterView", {
	options : {
	},
	_create : function() {
		var self = this,
		    data = this.options.data,
				metadata = this.options.metadata,
				start = this.options.start,
				collapse = this.options.collapse;

	  var common = computeCommon(metadata,start);
		var commonL = common[0];
		var commonR = common[1];
		var collapseddata = {};
		_.each(data, function(datum) {
			var sourcename = _.pluck(
								_.values(metadata[datum.collectionName].source).slice(start,collapse),
								"name").join('__');
			 collapseddata[sourcename] = (collapseddata[sourcename] || []);
			 collapseddata[sourcename].push(datum);
		});

		var lc = $("<div class='linearChooser'></div>").
			appendTo(this.widget()).
			linearChooser({
				data : {
					label : "Cluster by:",
					list : [{label: "front", list: commonL},{label:"back",list:commonR}]
				}
			});
		lc.find(".chooserSubElement").click(function(e){
			var parentId = $(e.target).parent().parent().attr("id");
			var id = e.target.id;
			console.log(id, parentId);
		});

		var html = ""
		for (key in collapseddata) {
			subcollapse = 0;
			var colnames = _.uniq(_.map(collapseddata[key],function(val){return val["collectionName"]; }));
			var newmetadata = subdict(metadata,colnames);

			if (subcollapse === 0) {
				var newcommon = computeCommon(newmetadata,start + collapse);
				html += "<br/><br/>" + key.split('__').join(' >> ') + ", Collapse by:" + newcommon[0].join(' ') + ' ... ' + newcommon[1].join(' ') + "<br/><br/>"
				html += this.options.resultsRenderer(collapseddata[key],collapse);
				this.element.append(html);
			} else {
				$("<div class='clusterView'></div>").
					appendTo(this.widget()).
					clusterView({
						data : collapseddata[key],
						metadata : newmetadata,
						start : start + collapse,
						collapse : subcollapse,
						resultsRenderer : self.options.resultsRenderer
					});
			}
		}
	},
	destroy : function() {
		this.element.empty();
	}
});


$.widget( "ui.linearChooser", {
		options: {
		},
		_create : function() {
			var html = "";
			var self = this;
			var tmpl = "\
<div>\
	<%= label %>\
	<ul>\
	<% _.each(list, function(data) { %>\
		<li class='chooserElement' id='<%= data.label %>'>\
			<ul>\
			<% _.each(data.list, function(item, idx) { %>\
				<li class='chooserSubElement' id='<%= idx %>'>\
					<%= item %>\
				</li>\
			<% }); %>\
			</ul>\
		</li>\
	<% }); %>\
	</ul>\
</div>";
			console.log(tmpl);
			$(_.template(tmpl, this.options.data)).appendTo(this.element);
			console.log(this.options.data);
		},
		destroy : function() {
			this.element.empty();
		}
});


})(jQuery);

