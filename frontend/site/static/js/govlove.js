var GovLove = {};
(function($) {
    var result_template = "";
    var base_url = "http://ec2-184-73-89-111.compute-1.amazonaws.com"
    var cache = {};
    var state = {};
    
    var fetchWithCache = function(path, callback, modifierFn) {
        var cachedresult = cache[path];
        if(cachedresult === undefined) {
            $.get(path, function(data) {
                if(modifierFn === undefined) {
                    cache[path] = data;
                } else {
                    cache[path] = modifierFn(data);
                }
                callback(cache[path]);
            });
        } else {
            callback(cachedresult);
        }
    }
    
    GovLove.loadState = function(s) {
        //TODO
        return s;
    }
    GovLove.getState = function() {
        return state;
    }
    GovLove.find = function(q,callback,options) {
        var params = {
            "q" : q,
            "facet" : true,
            "facet.field" : ["agency","subagency","source","spatialDivisions","spatialPhrases","dateDivisions","datePhrases","datasetTight"]
        };
        $.extend(params,options);
        $.getJSON(
            base_url+"/find?"+$.param(params,true),
            {},
            callback);
    }
    GovLove.table = function(q,callback) {
        $.ajax({
            url: base_url+"/table",
            type: "GET",
            data: q,
            dataType: "json",
            success: callback,
            complete: function(d) {
                console.log("is complete");
                console.log(d);
            }
        });
    }
    
    GovLove.get = function(q,callback) {
        $.ajax({
            url: base_url+"/get",
            type: "POST",
            data: (q),
            dataType: "jsonp",
            success: callback,
            complete: function(d) {
                console.log("is complete");
                console.log(d);
            }
        });
        // $.getJSON(
        //     base_url+"/get?callback=?",
        //     q,
        //     callback);
    }
    
    GovLove.convertFindDocToGetQuery = function(findDoc) {
        var mongoID = findDoc.mongoID;
        var mongoQuery = eval('('+findDoc.query[0]+')');
        var collectionName = findDoc.collectionName[0];
        var query = {"collectionName": collectionName,
                    "querySequence" : [["find", mongoQuery]]};
        return query;
    }
    
    GovLove.templates = {
        find_result: function(view, callback) {
            fetchWithCache("/static/jstemplates/find_result.erb", function(template) {
                // callback(Mustache.to_html(template,view));
                callback(template(view))
            }, _.template );
        },
    }
})(jQuery);
