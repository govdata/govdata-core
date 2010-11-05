goog.provide('gov.find');

gov.find.autocompleteCache = {};

gov.find.submit = function(options) {
  var params = {
    q : '',
    start : 0,
    rows : 20,
    'facet.field' : ['agency','subagency','datasetTight','dateDivisionsTight','spatialDivisionsTight'],
    facet : 'true'    
  }
  $.extend(true,params,options);
  $.ajax({
    url: gov.API_URL + '/find',
    dataType: 'jsonp',
    data: params,
    success: function(data) {
      gov.find.results.newResults(data);
    }
  });
}

gov.find.addSearchBar = function() {
  gov.find.searchbar = new gov.SearchBar("#content", {
    query: gov.find.query,
    autocomplete: {
      minLength : 2,
      source : function(request, response) {
          var term = request.term;
          if (term in gov.find.autocompleteCache) {
              response(gov.find.autocompleteCache[term]);
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
                  gov.find.autocompleteCache[ term ] = data;
                  if ( xhr === lastXhr ) {
                      response( data );
                  }
              }
          });
      }
    }
  });
  
  goog.events.listen(gov.find.searchbar,
                    "keypress",
                    gov.find.keypress,
                    false,
                    gov.find);
};

gov.find.onLoad = function() {
  gov.find.query = new gov.Query(gov.find.submit);
  console.log(gov.find.query);
  gov.find.addSearchBar();
  gov.find.results = new gov.FindResults();
  gov.find.resultsView = new gov.ResultsView("#content",gov.find.results);
};

gov.find.keypress = function() {
  console.log('keypress');
};

