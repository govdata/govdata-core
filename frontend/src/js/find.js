var root = this;

define(["underscore","widgets/SearchBar"], function() {
	
	var find = {};

	var subdict = function(olddict,subkeys){
		return _.reduce(subkeys,function(a,b){
			a[b] = olddict[b];
			return a;
			},{});
	};

	var commonFinder = function(listoflists) {
		var commonList = [];
		_.each(_.first(listoflists), function(obj, i) {
			var rowi = _.uniq(_.map(listoflists, function(x) { return x[i]; }));
			if (rowi.length > 1 || rowi[0] === undefined) {
				_.breakLoop();
			} else {
				commonList.push(obj);
			}
		});
		return commonList;
	};


	var computeCommon = function(metadict,start){
		var sourceDict = {};
		for (name in metadict) {
			 var entry = metadict[name];
			 var goodkeys = _.keys(entry["source"]).slice(start);
			 sourceDict[name] = {};
			 for (k in goodkeys) {
				 var key = goodkeys[k];
				 sourceDict[name][key] = entry["source"][key];
			 }
		}
		var sourcekeys = _.map(_.values(sourceDict), function(entry){return _.keys(entry) ; });
		var sourcekeysRev = _.map(sourcekeys,function(n){n = n.slice(); n.reverse() ; return n});
		var commonL = commonFinder(sourcekeys);
		var commonR = commonFinder(sourcekeysRev);
		commonR.reverse();
		if (_.isEqual(commonL,commonR)) {
			commonR = [];
		}
		return [commonL,commonR];
	};


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

	find.onLoad = function() {
		find.query = $(root).query({
				submitFn: find.submit,
				newData: function(e,d) {
					console.log("NEW DAATAAA");
					find.results.newResults(d);
				}
		}).data("query");
		find.addSearchBar();
		find.results = $(root).
													findResults({
														metadataFn : find.getMetadata
													}).
													data("findResults");
		find.resultsView = $("<div id='results'></div>").
													appendTo("#content").
													resultsView({
														dataHandler: find.results,
														resultsRenderer: find.resultsRenderer
													}).
													data("resultsView");
	};

	find.keypress = function() {
		console.log('keypress');
	};

	return find;

});
