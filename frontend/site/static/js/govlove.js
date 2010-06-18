var GovLove = {};
(function($) {
    GovLove.api_url = "http://ec2-184-73-70-72.compute-1.amazonaws.com"
    var result_template = "";
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
        return s;
    }
    
    GovLove.getState = function() {
        return state;
    }

    var getData = function(url,q,callback) {            
      // Send the query with a callback function
      var query = new google.visualization.Query(url);
      query.setQuery(q);
      query.send(callback);
      console.log(query);
    }
    
    GovLove.timeline = function(query, callback) {
        var timelineurl = GovLove.api_url+'/timeline?'
        getData(timelineurl,query,function(response) {
            var data = response.getDataTable();
            window.dd = data;
            console.log(data);
            // var t = $('<div class="timeline" style="height: 200px; width: 200px; margin-top: 50px;"></div>');
            // t.appendTo($("body"));
            // var timelineviz = new google.visualization.AnnotatedTimeLine(t[0]);
            // timelineviz.draw(data, {displayAnnotations: true});
            // t.show().dialog({ 
            //                 autoOpen: true,
            //                 modal: false,
            //                 option: "stack",
            //                 width: 800,
            //                 height: 500 });
        });
    }
    
    GovLove.find = function(q,callback,options) {
        var params = {
            "q" : q,
            "facet" : true,
            "facet.field" : ["agency","subagency","source","spatialDivisions","spatialPhrases","dateDivisions","datePhrases","datasetTight"]
        };
        $.extend(params,options);
        $.ajax({
            url: GovLove.api_url+"/find",
            data: params,
            dataType: "jsonp",
            success: callback,
            complete: function(d) {
                // do error checking
                console.log("is complete");
                console.log(d);
            }
        });
        // Example
        // $.getJSON("/static/exampledata.json",callback);
    }
    
    GovLove.get = function(query,callback,options) {
        $.extend(query,options);
        $.ajax({
            url: GovLove.api_url+"/get",
            data: $.param(query,true),
            dataType: "jsonp",
            processData: false,
            success: callback,
            complete: function(d) {
                // do error checking
                console.log("is complete");
                console.log(d);
            }
        });
    }
    
    GovLove.cleanQuery = function(query) {
        _.each(query,function(v,k) {
            query[k] = encodeURI(JSON.stringify(v));
        });
        return query;
    }

    GovLove.getQueryForDoc = function(doc, options) {
        var params = {
            limit : 10
        }
        $.extend(params,options)
        var mongoID = doc.mongoID;
        var mongoQuery = eval('('+doc.query[0]+')');
        var collectionName = doc.collectionName[0];
        var query = GovLove.Query(collectionName).find(mongoQuery).limit(params.limit).toString()
        return GovLove.cleanQuery(query);
    }
    
    GovLove.formatDate = function(dateStr) {
        // TODO: converts date value into english readable text
        return dateStr;
    }
    
    GovLove.locationToText = function(loc) {
        // TODO: converts location value from a query object into english readable text
        return loc;
    }
    
    GovLove.cluster = function(docs) {
        // cluster the docs by agency and subagency
        return docs;
    }
    
    GovLove.collapse = function(docs) {
        // collapse the docs into items with the same SourceSpec
        return docs;
    }
    
    GovLove.Query = function(collectionName) {
        /*
        collectionName : String => collection name e.g. BEA_NIPA
        query : List[Pair[action,args]] => mongo db action read pymongo docs e.g. 
            case args switch {
                tuple => (pymongoargs,) args is a positional args to be sent to action e.g. single element tuple
                dict => args is the dictionary of keyword arguments
                two element list => [tuple,dict] first position element Tuple and second is keyword dictionary 
            }
            e.g.
                tuple -> [("find",({"Topic":"Employment"},))]
        */
        function createSimpleAction(name,arg) {
            return {
                action : name,
                args : [arg]
            }
        }
        return {
            val: {
                    collection : collectionName,
                    query : [],
                    timeQuery : {"format":"Y"},
                    spaceQuery: {}
                 },
            find: function(q) {
                this.val.query.push(createSimpleAction("find",q))
                return this;
            },
            limit: function(size) {
                this.val.query.push(createSimpleAction("limit",size))
                return this;
            },
            action: function(act) {
                this.val.query.push(act);
                return this;
            },
            time: function(options) {
                // timeQuery : Dict => {"format": ?, "begin": ?, "end": ?, "on": ?} begin, end, on are dates in "fomat" format
                $.extend(this.val.timeQuery,options);
                return this;
            },
            space: function(options) {
                // spaceQuery : Dict => {"s": ?, "c": ?, "f": {"s", "c"}}
                //            : List => ["s", "c", "f.s"]
                $.extend(this.val.spaceQuery,options);
                return this;   
            },
            toString: function() {
                return JSON.stringify(this.val);
            }
        };
    }
    
    GovLove.templates = {
        find_result: function(view, callback) {
            fetchWithCache("/static/jstemplates/find_result.erb", function(template) {
                // callback(Mustache.to_html(template,view));
                console.log(template);
                callback(template(view));
                $("html").css("height","100%");
            }, _.template );
        },
    }
})(jQuery);
