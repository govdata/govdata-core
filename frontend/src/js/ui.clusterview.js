define(["utils","jquery","jquery-ui","ui.linearchooser"], function(utils) {

	$.widget( "ui.clusterview", {
		options : {
		},
		_create : function() {
			var self = this,
			    data = this.options.data,
					metadata = this.options.metadata,
					start = this.options.start,
					collapse = this.options.collapse;

		  var common = utils.computeCommon(metadata,start);
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

			var lc = $("<div class='linearchooser'></div>").
				appendTo(this.widget()).
				linearchooser({
					data : {
						label : "Cluster by:",
						list : [{label: "front", list: commonL},{label:"back",list:commonR}],
	                    resp : [_.range(commonL.length),_.range(-1,-commonR.length-1,-1)]
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
				var newmetadata = utils.subdict(metadata,colnames);

				if (subcollapse === 0) {
					var newcommon = utils.computeCommon(newmetadata,start + collapse);
                
	                this.element.append("<div class='clusterElement'><br/><br/>" + key.split('__').join(' >> ') + "<br/><br/>")
                
	                var innerchooser = $("<div></div>").appendTo(this.widget()).linearchooser({
	                    data : {
	                        label : "Cluster by:",
	                        list : [{label:"front",list:newcommon[0]},{label:"back",list:newcommon[1]}]
	                    }
	                });
                
	                innerchooser.find(".chooserSubElement").click(function(e){
				      var parentId = $(e.target).parent().parent().attr("id");
				      var id = e.target.id;
				      console.log(id, parentId);
	                  var thing = $(e.target).parent().parent().parent().parent().parent().parent();
	                  console.log(thing)
	                  var options = thing.data().clusterView.options;
	                  options.collapse += 1;
	                  //thing.find(".resultsTable").remove()
	                  console.log(thing.parent())
                  
                                    
                  
                  
			        });
                
					this.element.append(this.options.resultsRenderer(collapseddata[key],collapse) + '</div>');
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
	
});