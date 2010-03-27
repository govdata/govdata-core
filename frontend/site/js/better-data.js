var BetterData = {};
(function($) {
    var result_template = "";
    var api_url = "http://0.0.0.0:4000"
    var solrbase = "http://entabular.com:8983/solr";
    var cache = {};
    var state = {};
    
    var fetch = function(path, callback, modifierFn) {
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
    
    BetterData.loadState = function(s) {
        //TODO
        return s;
    }
    BetterData.getState = function() {
        return state;
    }
    BetterData.find = function(q,callback,options) {
        var settings = {"type":"select"}
        $.extend(settings,options)
        $.ajax({ url: solrbase+"/"+settings["type"], type: "GET",
            dataType: "json", 
            traditional: true,
            data: {"q":q,"wt":"json","hl":"on",
                    "hl.fl":"keywords,title,description",
                    "facet":true,
                    "facet.field":["Agency","Subagency","Source","Geography","TimePeriod"],
                    "facet.query":"ReleaseDate:[1907-07-18T23:59:59Z TO 2006-07-18T23:59:59Z]"},
            success: callback
        });
    }
    BetterData.get = function(q,callback,options) {
    
    }
    BetterData.templates = {
        result: function(view, callback) {
            fetch("templates/result.erb", function(template) {
                // callback(Mustache.to_html(template,view));
                callback(template(view))
            }, _.template );
        },
    }
})(jQuery);
