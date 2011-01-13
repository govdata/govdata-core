var root = this;

define(["gov","common/location","common/timedate", "jquery","underscore","underscore.strings",
	"jquery.hotkeys","ui.searchbar","ui.query","ui.findresults",
	"ui.resultsview","jquery.masonry","ui.statehandler","ui.dict"], function(gov,loc,td) {

	var find = {};

	find.autocompleteCache = {};

	find.submit = function(options, callback) {
		var params = {
			q : '',
			rows : 100,
			'facet.field' : ['sourceSpec','datasetTight','dateDivisionsTight','spatialDivisionsTight'],
			facet : 'true',
			fl : ['mongoID','mongoText','sourceSpec','query','volume','topic','collectionName'].join(',')
		};
		$.extend(true,params,options);
		$.ajax({

			url: gov.API_URL + '/search',
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
		var qSeq = [{'action':"find",'args':[{"name":{"$in":colls}}] , 'kargs':{"fields":["name","metadata.source"]}}];
		var qSeqStr = JSON.stringify(qSeq);

		var params = {querySequence: qSeqStr};
		$.extend(true,params,options);

		$.ajax({
			url: gov.API_URL + '/metadata',
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


	same_as = function(item1,item2,key){
	    if ((key in item1) & (key in item2) & (item1[key] == item2[key])) {
	        return 'sameVal'
	    } else {
	        return 'diffVal'
	    }

	}

	make_value = function(v,k){

	    if (k == 'Location'){
	        return loc.phrase(v)
	    } else {
	        return v
	    }
	}

	upperFirst = function(x){
	    return x[0].toUpperCase() + x.slice(1);
	};

    find.resultRenderer = function(item,collapse,prev_item){

     var source = item["sourceSpecParsed"]
     var prev_source;
     if (prev_item === undefined){
         prev_item = {}
     }
     if (prev_item["sourceSpecParsed"]) {
         prev_source = prev_item["sourceSpecParsed"]
         prev_query =  prev_item["queryParsed"]
     } else {
         prev_source = {};
         prev_query = {};
     }
     var sourceKeys;

     if (collapse === 0){
       sourceKeys = [];
     } else if (collapse === 'QUERY') {
       sourceKeys = _(source).keys();
     } else {
       sourceKeys = _(source).keys().slice(collapse);
     }
     var sourceStr = sourceKeys.map(function(key){return "<div class='sourceElement " + same_as(source,prev_source,key) + "'><span class='sourceKey' id='" + key + "'>" + upperFirst(key) + "</span>: <span class='sourceVal'>" + source[key] + "</span></div>";}).join('')
     var query = item["queryParsed"];
     var queryStr = _(query).keys().map(function(key){return "<div class='queryElement " + same_as(query,prev_query,key) + "'><span class='queryKey' id='" + key + "'>" + upperFirst(key) + "</span>: <span class='queryVal'>" + make_value(query[key],key) + '</span></div>';}).join('')

     var sourceBox = '<div class="sourceBox">' + sourceStr + '</div>';
     var queryBox = '<div class="queryBox">' + queryStr + '</div>';
     var numResults;
     if (item["volume"] !== undefined){
         numResults = '<div class="numResults">' + item["volume"][0] + '</div>';
     } else {
         numResults = '';
     }

	 var collection = item.collection || "";
	 var query = item.queryParsed || {};
	 query = JSON.stringify(query);
	 var jsonp = "<div><a href=\"" + gov.API_URL + "/data?collection=" + collection + "&query=" + encodeURI(query) + "\">JSONP</a></div>";

	 return '<div class="resultBox"><div class="innerResultBox">' + sourceBox + queryBox + jsonp + numResults +'</div></div>'

    };

    dictIntersect = function(dict1,dict2){
        var k,v;
        $.each(dict2, function(k,v){
            if ((k in dict1) && (! (_.isEqual(dict1[k], v)))) {
                    delete dict1[k];
            }
        });
        $.each(dict1, function(k,v){
            if ( ! (k in dict2) ) {
                 delete dict1[k];
            }
        });

    };

    dictDiff = function(dict1,dict2){
        var k,v;
        $.each(dict2, function(k,v){
            if (_.isEqual(dict1[k], v)) {
               delete dict1[k];
            }
        });
    };

    computeCommons = function(resultlist){
        var commons = {'sourceSpecParsed': _.clone(resultlist[0]['sourceSpecParsed']),
                       'queryParsed': _.clone(resultlist[0]['queryParsed'])
                      } ;

        var i,r;
        $.each(resultlist.slice(1),function(i,r){
            dictIntersect(commons['sourceSpecParsed'],_.clone(r['sourceSpecParsed']))
            dictIntersect(commons['queryParsed'],_.clone(r['queryParsed']))

        });

        $.each(resultlist,function(i,r){
             dictDiff(r['sourceSpecParsed'],commons['sourceSpecParsed'])
             dictDiff(r['queryParsed'],commons['queryParsed'])
        });

        return commons

    };

    reduceDuplicates = function(Rlist){
        var retains = [];
        var retain, superset;
        $.each(Rlist,function(i1,elt1){
            retain = true
            $.each(Rlist,function(i2,elt2){
               if (retain) {

				   if ((elt1['volume'][0] == elt2['volume'][0]) && (_.isEqual(elt1['sourceSpecParsed'],elt2['sourceSpecParsed'])) && (i1 != i2)){
					  superset = true;
					  $.each(elt2['queryParsed'],function(k,v){
						  if (!((k in elt1['queryParsed']) && (_.isEqual(elt1['queryParsed'][k] , v)))){
							  superset = false;
						  }
					  });


					  if (superset){
					      retain = false;

					  }

				   }
			   }
            });
            if (retain){
                retains.push(i1);
            }
        });


       return _.map(retains,function(elt){return Rlist[elt];})

    };

	find.resultsRenderer = function(parent,resultlist,collapse,facet,key){


		var keyKeys = _.keys(JSON.parse(resultlist[0]['sourceSpec'][0]));

	    parent.find('.keyLabel .chooserSubElement').unbind('click');
		parent.find('.keyLabel .chooserSubElement').click(function(e){
		   var id = parseInt(e.currentTarget.id);

	      var newItems = find.query.items.slice(0);
	      var skey = keyKeys[id]
	      var sval = $(e.currentTarget).find(".value").text();
          var filteritems = find.query.filteritems;
	      filteritems.push(skey + ':"' + sval + '"');
          find.query.update({'qval' : newItems, 'fqval' :filteritems});
          $(root).data()['statehandler'].changestate();

		});


	    var Rcopy = [];

        var ival = {}
	    $.each(resultlist,function(ind,val){
	        ival = {"sourceSpecParsed" : JSON.parse(val["sourceSpec"][0]),
	                "queryParsed" : JSON.parse(val["query"][0]),
	                "volume" : val['volume'],
	                "title": val["title"],
					"collection" : val["collectionName"][0]
	               };
	        Rcopy.push(ival);
	    });

	    Rcopy = reduceDuplicates(Rcopy);


	    var commons = computeCommons(Rcopy);

 	    var all_volume,cidx,rval;
 	    var retains = [], removes = [];
	    for (cidx in Rcopy){
		    rval = Rcopy[cidx];
		    if ((_.isEqual(rval['sourceSpecParsed'],{})) && (_.isEqual(rval['queryParsed'],{}))){
		 	   if (rval['volume'] !== undefined){
  		 	     all_volume = rval['volume'];
		 	     removes.push(cidx);
		 	   }
		    } else {
			   retains.push(cidx);
		    }
	    }

	    if (!_.isEqual(removes,[])){

	       var common_col = collapse;
	       if  ((_.keys(commons['sourceSpecParsed']).len == key.split('|').len) && (_.isEqual(commons['queryParsed'],{}))){
	           common_col = collapse - 1;
	       }

           var commonBox = $("<div class='commonBox'><div class='commonText'>All Data for: </div></div>").appendTo(parent);
           Rcopy = _.map(retains,function (elt) {return Rcopy[elt];});
           commons['volume'] = [all_volume + ' records'];
           var commonResult= $(find.resultRenderer(commons,common_col)).appendTo(commonBox)
           commonResult.addClass("commonResult");
        }


	    var result_container = $("<div class='resultMason'></div>").appendTo(parent);

	    $.each(Rcopy,function(ind,result){
	        var prev_result;
	        if (ind > 0){
	            prev_result = resultlist[ind-1];
	        } else {
	            prev_result = {}
	        }
	        result_container.append(find.resultRenderer(result,-1,prev_result));
	    });

	    result_container.masonry({
	        columnWidth:230,
	        singleMode : true,
	        itemSelector : '.resultBox',
	        resizeable:true
	    });

	   $('.sourceElement').unbind('click');
	   $('.sourceElement').click(function(e){
	      var res = $(e.currentTarget);
	      var newItems = find.query.items.slice(0);
	      var skey = res.find(".sourceKey")[0].id;
	      var sval = res.find(".sourceVal").text();
          var filteritems = find.query.filteritems;
	      filteritems.push(skey + ':"' + sval + '"');
          find.query.update({'qval' : newItems, 'fqval' :filteritems});
          $(root).data()['statehandler'].changestate();

	    });

	   $('.queryElement').unbind('click');
	   $('.queryElement').click(function(e){
	      var res = $(e.currentTarget);
	      var newItems = find.query.items.slice(0);
	      var qkey = res.find(".queryKey")[0].id;
	      var qval = res.find(".queryVal").text();
	      var newclause = '"' + qkey + '=' + qval + '"';
	      newItems.push(newclause);
          find.query.update({'qval': newItems});
          $(root).data()['statehandler'].changestate();

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
		var sb = $("<div id='searchbar'></div>").
			appendTo("#subHeader").
			searchbar({
				query : find.query,
				autocomplete : params.autocomplete
			});

		return sb;
	};


    var specialdict = function(items){
        self = this;
        self.items = items ;

        self.value = function (){
		    var self = this;
		    return this.items
		};

		self.update = function(val){
		  self = this;
		  self.items = val;
		};

		remove = function(val){
		  self = this;
		  delete self.items[val];

		}

		add = function(key,val){
		  self = this;
		  self.items[key] = val;

		}

		return self
    }

    specialdict.prototype = new Object;


	find.load = function(params, state) {


		find.query = $(root).query({
				submitFn: find.submit,
				newData: function(e,d) {
					find.results.newResults(d);
				}

		}).data("query");

        find.collapsedict = $(root).dict({
          items : {" " :0},
          prefix : 'collapsedict'
        }).data("dict");

 /*       find.hidedict = $(self.element).dict({
          items : {" " :false},
        }).data("dict");      */

/*        find.collapsedict = specialdict({" " :0}); */
        find.hidedict = specialdict({" " :false});

		find.statehandler = $(root).statehandler({
			objects : {
				query : find.query,
				collapsedict : find.collapsedict,
				hidedict : find.hidedict
			}

		}).data("statehandler");

	    $("#subHeader").remove();
	    var subheader = $("<div id='subHeader'></div>").appendTo('#content');

        $("#searchbar").remove();
        find.sb = find.addSearchBar();


        find.statehandler.setstate(state);

   		find.results = $(root).findresults({
														metadataFn : find.getMetadata
													}).
													data("findresults");

	    $("#resultsView").remove();
		find.resultsView = $("<div id='resultsView'></div>").
													appendTo("#content").
													resultsview({
														dataHandler: find.results,
														resultsRenderer: find.resultsRenderer,
														collapsedict : find.collapsedict,
														hidedict : find.hidedict,
														query : find.query
													});



	};


	return find;

});
