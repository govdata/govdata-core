var GovLove = {};
(function($) {
    GovLove.api_url = "http://ec2-72-44-53-142.compute-1.amazonaws.com"
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
        //TODO
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
        // var params = {
        //     "q" : q,
        //     "facet" : true,
        //     "facet.field" : ["agency","subagency","source","spatialDivisions","spatialPhrases","dateDivisions","datePhrases","datasetTight"]
        // };
        // console.log("finding");
        // $.extend(params,options);
        // $.ajax({
        //     url: GovLove.api_url+"/find",
        //     data: params,
        //     dataType: "jsonp",
        //     success: callback,
        //     complete: function(d) {
        //         console.log("is complete");
        //         console.log(d);
        //     }
        // });
        $.getJSON("/static/exampledata.json",callback);
    }

    GovLove.getQueryForDoc = function(findDoc, limit) {
        if (limit === undefined) {
            limit = 10;
        }        
        var mongoID = findDoc.mongoID;
        var mongoQuery = eval('('+findDoc.query[0]+')');
        var collectionName = findDoc.collectionName[0];
        var query = GovLove.Query(collectionName).find(mongoQuery).limit(limit).toString()
        // var query = JSON.stringify({"collectionName": collectionName,
        //             "querySequence" : JSON.stringify([["find", [[mongoQuery],{}]],["limit", [[limit],{}]]]).replace(/"/g,'\"')});
        return query;
    }
    
    GovLove.formatDate = function(dateStr) {
        return dateStr;
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
