var root = this;

define(["gov","jquery","underscore","underscore.strings",
	"jquery.hotkeys","ui.searchbar","ui.query","ui.findresults",
	"ui.resultsview"], function(gov) {
	
	var find = {};

	find.autocompleteCache = {};

	find.submit = function(options, callback) {
		var params = {
			q : '',
			start : 0,
			rows : 20,
			'facet.field' : ['agency','subagency','datasetTight','dateDivisionsTight','spatialDivisionsTight'],
			facet : 'true'
		};
		$.extend(true,params,options);
		$.ajax({
			url: gov.API_URL + '/find',
			dataType: 'jsonp',
			data: params,
			success: function(data) {
				console.log("successfully got data");
				console.log(data);
				callback(data);
			}
		});
	}

	find.getMetadata = function(doclist,callback,options){
		var colls = _.uniq(_.map(doclist,function(doc){return doc["collectionName"][0] ;}));
		var qSeq = [["find",[[{"name":{"$in":colls}}],{"fields":["name","metadata.source"]}]]];
		var qSeqStr = JSON.stringify(qSeq);

		var params = {querySequence: qSeqStr};
		$.extend(true,params,options);

		$.ajax({
			url: gov.API_URL + '/sources',
			dataType: 'jsonp',
			data: params,
			success : function(metadata){
				var metadict = new Object();
				for (i in metadata){
					var val = metadata[i];
					metadict[val["name"]] = val["metadata"];
				}
				callback(metadict);
			}
		});
	};

	find.resultsRenderer = function(resultlist,collapse){
		var html = "<table class='resultsTable'>";
		_.each(resultlist, function(d) {
			html += "<tr>";
			html += "<td>"+d.mongoID[0]+"</td>";
			html += "<td><a href='#/show?q=\""+d.mongoID[0]+"\"' >clickhere</a></td>";
			html += "</tr>";
		});
		html += "</table>";
		return html
	}

	find.addSearchBar = function() {
		var params = {
			query: find.query,
			autocomplete: {
				minLength : 2,
				source : function(request, response) {
						var term = request.term;
						if (term in find.autocompleteCache) {
								response(find.autocompleteCache[term]);
								return;
						}
						lastXhr = $.ajax({
								url : gov.API_URL + "/terms",
								data : {
										"terms.fl" : "autocomplete",
										"terms.sort" : "index",
										"terms.prefix" : request.term,
										"omitHeader" : true
								},
								dataType : 'jsonp',
								success : function( data, status, xhr ) {
										data = _.select(data.terms[1],function(val,i) { if(i % 2 == 0) return val; });
										find.autocompleteCache[ term ] = data;
										if ( xhr === lastXhr ) {
												response( data );
										}
								}
						});
				}
			}
		};
		$("<div id='searchbar'></div>").
			appendTo("#content").
			searchbar({
				query : find.query,
			});
	};

	find.load = function(params, state) {
		find.query = $(root).query({
				submitFn: find.submit,
				newData: function(e,d) {
					console.log("NEW DAATAAA");
					find.results.newResults(d);
				}
		}).data("query");
		find.addSearchBar();
		find.results = $(root).
													findresults({
														metadataFn : find.getMetadata
													}).
													data("findresults");
		find.resultsView = $("<div id='resultsview'></div>").
													appendTo("#content").
													resultsview({
														dataHandler: find.results,
														resultsRenderer: find.resultsRenderer
													}).
													data("resultsview");
	};

	find.keypress = function() {
		console.log('keypress');
	};

	return find;

});
