var Gov = {};
(function($) {
    Gov.api_url = "http://ec2-67-202-31-123.compute-1.amazonaws.com";
    var state = {};    
    
    Gov.levels = ['agency','subagency','topic','subtopic','program','dataset'];
    
    Gov.loadState = function(s) {
        return s;
    };
    
    Gov.getState = function() {
        return state;
    };

    var getData = function(url,q,callback) {            
      // Send the query with a callback function
      var query = new google.visualization.Query(url);
      query.setQuery(q);
      query.send(callback);
      C.println(query);
    };
    
    Gov.sources = function() {
        C.fetchWithCache(Gov.api_url+"/sources?");
    };
    
    Gov.timeline = function(query, callback) {
        var timelineurl = Gov.api_url+'/timeline?';
        getData(timelineurl,query,function(response) {
            var data = response.getDataTable();
            window.dd = data;
            C.println(data);
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
    };
    
    Gov.find = function(q,callback,options) {
        var params = {
            "q" : q
        };
        $.extend(params,options);
        $.ajax({
            url: Gov.api_url+"/find",
            data: params,
            dataType: "jsonp",
            success: callback
        });
    };
    
    Gov.get = function(q,callback,options) {
        var params = {
            "q" : q
        };
        $.extend(params,options);
        $.ajax({
            url: Gov.api_url+"/get",
            data: params,
            dataType: "jsonp",
            jsonpCallbackString: "C.load",
            success: callback,
            complete: function(d) {
                // do error checking
                C.println("is complete");
                C.println(d);
            }
        });
    };
    
    Gov.getQueryForDoc = function(doc, options) {
        var params = {
            limit : 10
        };
        $.extend(params,options);
        var mongoID = doc.mongoID;
        var mongoQuery = eval('('+doc.query[0]+')');
        var collectionName = doc.collectionName[0];
        var query = Gov.Query(collectionName).find(mongoQuery).limit(params.limit).toString();
        return query;
    };
    
    Gov.formatDate = function(dateStr) {
        // TODO: converts date value into english readable text
        return dateStr;
    };
    
    Gov.locationToText = function() {
        // TODO: converts location value from a query object into english readable text
    };
    
    Gov.hierarchy = function(docs) {
        // hierarcy the docs by agency and subagency
        var hierarchy = {};
        var lowest_level = _.last(Gov.levels);
        _.each(docs, function(doc) {
            var source = C.load(doc.sourceSpec);
            var hierarchy_position = hierarchy;
            _.each(Gov.levels, function(level) {
                var level_value = source[level];
                if (level_value === undefined || level_value === "") {
                    level_value = undefined;
                }
                if (hierarchy_position[level_value] === undefined) {
                    if(level === lowest_level) {
                        hierarchy_position[level_value] = [];
                    } else {
                        hierarchy_position[level_value] = {};
                    }
                }
                if (level === lowest_level) {
                    hierarchy_position[level_value].push(doc)
                }
                hierarchy_position = hierarchy_position[level_value];
            });
        });
        return hierarchy;
    };
    
    Gov.renderHierarchy = function(hierarchy) {
        function render_helper(hierarchy) {
            var rendered = ""
            if (_.isArray(hierarchy)) {
                rendered += "<ul>";
                _.each(hierarchy, function(doc) {
                    // render as item
                    rendered += "<li>";
                    rendered += doc.title;
                    rendered += doc.query;
                    rendered += "</li>";
                });
                rendered += "</ul>";
            } else {
                // render as hierarchy
                rendered += "<ul>";
                _.each(_.keys(hierarchy), function(key) {
                    if (!(key === undefined || key === 'undefined')) {
                        rendered += "<li>";
                        rendered += key;
                        rendered += "</li>";
                    }
                   rendered += render_helper(hierarchy[key]);
                });
                rendered += "</ul>";
            }
            return rendered;
        }
        return "<div id='hierarchy_result'>"+render_helper(hierarchy)+"</div>";
    }
        
    Gov.renderCollapsed = function(docs) {
        var rendered = "";
        var toLoad = ['sourceSpec','query'];
        var renderKeys = ['dataset','sourceSpec','query']
        var last = {};
        rendered += "<div class='header'><div>Dataset</div><div>Source</div><div>Query</div></div>";
        _.each(docs, function(doc) {
            rendered += "<div class='collapsed_result'>";
            var current = {};
            var sourceSpec = C.load(doc['sourceSpec']);
            current['dataset'] = sourceSpec.dataset;
            current['sourceSpec'] = {agency: sourceSpec.agency, subagency: sourceSpec.subagency};
            current['query'] = C.load(doc['query']);
            var diff = _.difference(last,current);
            if (diff === null) {
                rendered += "WTF this was the exact same as the last one";
            } else {
                _.each(renderKeys, function(key) {
                    rendered += "<table class='"+key+"'>";
                    var item = diff[key];
                    if (item !== undefined) {
                        if(_.isArray(item)) {
                            _.each(item, function(value,index) {
                                rendered += "<tr><td>"+value+"</td></tr>";
                            });
                        } else if (_.isString(item)) {
                            rendered += "<tr><td class='value'>"+item+"</td></tr>";
                        } else {
                            _.each(item, function(value,key) {
                                rendered += "<tr><td class='key'>"+key+"</td><td class='value'>"+value+"</td></tr>";
                            });
                        }
                    }
                    rendered += "</table>";
                });
            }
            rendered += "</div>";
            last = current;
        });
        return "<div id='collapsed_results'>"+rendered+"</div>";
    }
    
    Gov.Query = function(collectionName) {
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
                    spaceQuery: {},
                    returnMetadata: true,
                    returnObj: true
                 },
            find: function(q) {
                this.val.query.push(createSimpleAction("find",q));
                return this;
            },
            limit: function(size) {
                this.val.query.push(createSimpleAction("limit",size));
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
    };
})(jQuery);
