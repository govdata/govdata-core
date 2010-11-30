var root = this;

define(["gov","jquery","underscore","underscore.strings",
	"jquery.hotkeys","ui.searchbar","ui.query","ui.findresults",
	"ui.resultsview","jquery.masonry"], function(gov) {
	
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
			traditional:true,
			success: function(data) {
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
	
	
    find.resultRenderer = function(item,collapse){
  
     var title = item["title"][0];
     var source = JSON.parse(item["sourceSpec"][0]);
     var sourceKeys;
     if (collapse === 0){
       sourceKeys = [];
     } else {
       sourceKeys = _(source).keys().slice(collapse);
     }
     var sourceStr = sourceKeys.map(function(key){return "<div class='sourceElement'><span class='sourceKey'>" + key + "</span>: <span class='sourceVal'>" + source[key] + "</span></div>";}).join('')
     var query = JSON.parse(item["query"][0]);
     var queryStr = _(query).keys().map(function(key){return "<div class='queryElement'><span class='queryKey'>" + key + "</span>: <span class='queryVal'>" + query[key] + '</span></div>';}).join('')
 
     var sourceBox = '<div class="sourceBox">' + sourceStr + '</div>';
     var queryBox = '<div class="queryBox">' + queryStr + '</div>';
     var numResults = '<div class="numResults">Records: ' + item["volume"][0] + '</div>';
     return '<div class="resultBox">' + sourceBox + queryBox  + numResults +'</div>'
  
    };	

	find.resultsRenderer = function(parent,resultlist,collapse){
	
	    var result_container = $("<div class='resultMason'></div>").appendTo(parent);
	    
	    $.each(resultlist,function(ind,result){
	        result_container.append(find.resultRenderer(result,collapse));
	    });
	    
	    result_container.masonry({
	        columnWidth:200,
	        itemSelector : '.resultBox',
	        resizeable:true,
	    });
	    
	   $('.sourceElement').unbind('click'); 
	   $('.sourceElement').click(function(e){
	      var res = $(e.currentTarget);
	      var newItems = find.query.items.slice(0);
	      var skey = res.find(".sourceKey").text();
	      var sval = res.find(".sourceVal").text();
          var filteritems = find.query.filteritems;
	      filteritems.push(skey + ':"' + sval + '"');
          find.query.update(newItems,filteritems);

	    });
	
	   $('.queryElement').unbind('click'); 
	   $('.queryElement').click(function(e){
	      var res = $(e.currentTarget);
	      var newItems = find.query.items.slice(0);
	      var qkey = res.find(".queryKey").text();
	      var qval = res.find(".queryVal").text();
	      var newclause = '"' + qkey + '=' + qval + '"';
	      newItems.push(newclause);
          find.query.update(newItems);

	    });	
	  
	  return result_container;
	  
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
					find.results.newResults(d);
				}
		
		}).data("query");
		
		
		find.addSearchBar();
		find.results = $(root).
													findresults({
														metadataFn : find.getMetadata
													}).
													data("findresults");
		find.resultsView = $("<div id='resultsView'></div>").
													appendTo("#content").
													resultsview({
														dataHandler: find.results,
														resultsRenderer: find.resultsRenderer
													}).
													data("resultsView");
	    
        
	};


     
	return find;

});
