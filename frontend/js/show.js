var Show = {};

(function($) {

    var SPECIAL_KEYS =  ['__versionNumber__','__retained__','__addedKeys__','__originalVersion__'];

    Show.updatePositions = function() {
        $('#floatedArea').masonry({
            columnWidth: 150,
            resizeable: false, // handle resizing ourselves
            itemSelector: '.module' });
    };

    // Show.transformTimeCols = function(row) {
    //     var timeCols = this.metadata.columnGroups.timeColNames;
    //     if(timeCols) {
    //         var dateFormat = this.metadata.dateFormat;
    //         var dateDivisions = this.metadata.dateDivisions;
    //         var dateFormatTesters = _.map(dateDivisions, function(dateCode) {
    //             var idx = dateFormat.indexOf(dateCode);
    //             return (function(dateObj) {
    //                 return dateObj[idx] !== 'X';
    //             });
    //         });
    //         var toDateStr = this.metadata.nameProcessors.timeColNames;
    //         var toDate = function(d) { return new Date(toDateStr(d));};
    //         var dateGroups = {};
    //         _.each(this.metadata.columns, function(col,i) {
    //             if(_.include(timeCols, col)) {
    //                 _.each(dateFormatTesters, function(dateTest,j) {
    //                     if(dateTest(col)) {
    //                         var dd = dateDivisions[j];
    //                         if(dateGroups[dd] === undefined) dateGroups[dd] = [];
    //                         var d = toDate(col);
    //                         var v = row[i];
    //                         if(_.isNumber(v)) {
    //                             dateGroups[dd].push({x: d, y: v});
    //                         }
    //                         _.breakLoop();
    //                     }
    //                 });
    //             }
    //         });
    //         return dateGroups;
    //     } else {
    //         return null;
    //     }
    // }

    Show.metadata = {};

    Show.run = function(query, collectionName) {

        Show.apiUrl = "http://ec2-67-202-31-123.compute-1.amazonaws.com";
        Show.baseQuery = JSON.parse(query);
        Show.collectionName = collectionName;
        var querySequence = [["find",[[{"name":collectionName}],{"fields":[
        "metadata.valueProcessors","metadata.nameProcessors","metadata.columnGroups",
        "metadata.source","metadata.columns","metadata.startDate","metadata.endDate",
        "metadata.contactInfo","metadata.keywords","metadata.dateFormat",
        "metadata.spatialDivisions","metadata.dateDivisions","metadata.description",
        "metadata.volume","metadata.shortTitle","metadata.title","metadata.sliceCols"]}]]];

        var serverData = function(opts,callback) {
            var q = [{ action: 'find', args: [Show.baseQuery]}];
            var skip = opts.skip || 0;
            q.push({ action : 'skip', args : [skip]});
            var limit = opts.limit || 30;
            q.push({ action : 'limit', args : [limit]});
            var timeQuery = {format : Show.metadata.timeSelector};
            var queryString = JSON.stringify({"timeQuery": timeQuery,
                                    "query":q,"collection":collectionName});
            // var queryString = JSON.stringify({"query":q,"collection":collectionName});
            countRows({}, function(count) {
                Show.metadata.count = count;
                $.ajax({
                    url : Show.apiUrl+'/get',
                    data : { q : queryString },
                    dataType : 'jsonp',
                    success : function(data) {
                        callback(data.data);
                    }
                });
            });
        };

        var countRows = function(opts, callback) {
            //TODO: why is volume wrong
            var q = [{ action: 'find', args: [Show.baseQuery]},{action:"count"}];
            var timeQuery = {format : Show.metadata.timeSelector};
            var queryString = JSON.stringify({"timeQuery": timeQuery,
                                    "query":q,"collection":collectionName});
            // var queryString = JSON.stringify({"query":q,"collection":collectionName});
            var cached = Show.metadata.countCache[queryString];
            if(cached) {
                callback(cached);
            } else {
                $.ajax({
                    url : Show.apiUrl+'/get',
                    data : { q : queryString },
                    dataType : 'jsonp',
                    success : function(data) {
                        Show.metadata.countCache[queryString] = data.data;
                        callback(data.data);
                    }
                });
            }
        }

        var dataToRows = function(data,metadata) {
            // store the data in raw form for later
            metadata.data = data;
            var result = new Array();
            _.each(data, function(rowObj) {
                var row = new Array(metadata.showColsLength);
                _.each(rowObj, function(v,k) {
                    row[metadata.absoluteToRelative[k]] = v;
                });
                result.push(row);
            });
            return result;
        };

        var extractTimelineData = function(row) {
            var data = [];
            var metadata = Show.metadata;
            var dt = require("timedate");
            var toDate = function(d) {
                return dt.convertToDT(dt.stringtomongo(d,metadata.dateFormat));
            }
            _.each(row, function(val,key) {
                var columnName = metadata.showCols[key];
                if(metadata.isTimeColumn(columnName)) {
                    var d = toDate(columnName);
                    data.push({x: d, y: val});
                }
            });
            return data;
        };

        var extractMapData = function(transdata) {
        };

        var onRowClick = function(row) {
            //console.log(row);
            console.log("CLICKED A ROW MOFO!!!");
            var timedata = extractTimelineData(row);
            Show.timeline.add(timedata);
            //Show.metadata.transpose(row,"Location.s",["1"],function(transdata) {
                //var mapdata = extractMapData(transdata);
                //console.log(transdata);
                ////Show.map.add(transdata);
                //Show.updatePositions();
            //});
            Show.updatePositions();
        };

        var renderSource = function(source) {
            return _.template("<div class='sourceHierarchy'>\
                    <% _.each(source, function(v,k) { %>\
                        <div><span class='label'><%= k %>: </span><%= v.name %></div>\
                    <% }); %>\
                    </div>", {source : source});
        };

        // Initial load with metadata
        $.ajax({
            url : Show.apiUrl+'/sources',
            data : { querySequence : JSON.stringify(querySequence) },
            dataType : 'jsonp',
            success : function(data) {
                Show.metadata = new Metadata(data[0].metadata);
                var metadata = Show.metadata;

                $("#floatedArea").append('<div class="module col3"><div class="inner"><div class="title">'+
                    metadata.title+" ("+metadata.shortTitle+')</div><div class="keywords"><span class="label">keywords: </span>'+
                    metadata.keywords+'</div></div></div>');
                if(metadata.contactInfo) {
                    $("#floatedArea").append('<div class="module col3"><div class="inner"><span class="label">Contact Info: </span>'+metadata.contactInfo+'</div></div>');
                }
                $("#floatedArea").append('<div class="module col3"><div class="description inner"><span class="label">Description: </span>'+
                    metadata.description+'</div></div>');
                $("#floatedArea").append('<div class="module col1"><div class="query inner"><span class="label">Query: </span>'+
                    query+'</div></div>');
                $("#floatedArea").append('<div class="module col2"><div class="source inner"><span class="label">Source: </span>'+
                    renderSource(metadata.source)+'</div></div>');

                $("#floatedArea").append('<div class="module col3"><div class="inner"><div id="timeline" ></div></div></div>')
                Show.timeline = new iv.Timeline({
                  container : document.getElementById('timeline')
                });

                $("#floatedArea").append('<div class="module col6"><div id="table" class="inner"></div></div>');
                Show.table = new iv.Table({
                  container : document.getElementById('table'),
                  metadata : metadata,
                  serverData : serverData,
                  transformer : dataToRows,
                  onRowClick : onRowClick
                });

                if(metadata.isSpatial()) {
                    onRowClick(0);
                }

                // $("#floatedArea").append('<div class="module col4"><div id="rotatedTable"></div></div>')
                // Show.rotatedTable = new iv.Table({
                //   container : document.getElementById('rotatedTable'),
                //   metadata : metadata,
                //   serverData : serverData,
                //   transformer : toData,
                //   onRowClick : onRowClick
                // });

                // Show.timeline.render();
                // Show.table.render();
                // Show.rotatedTable.render();

                _.defer(Show.updatePositions);
            }
        });

        $(window).resize(function() {
            // Use timer method so this event doesn't fire All the time
            clearTimeout(Show.resizeTimer);
            Show.resizeTimer = setTimeout(Show.updatePositions, 500);
        });

        $("table tr").live("click", function() {
            var idx = $(this).data("idx");
            idx = idx || 0;
            var row = Show.metadata.data[idx];
            onRowClick(row);
        });
    };
})(jQuery);


var Metadata = function(metadata) {
    // extend yourself with metadata
    // needs to have a timeSelector and a spaceSelector
    this.data = [];
    this.showCols = {};
    _.extend(this,metadata);
    //FORCE SORT DATEDIVISIONS
    this.dateDivisions = _.sortBy(metadata.dateDivisions,function(i){ return metadata.dateFormat.indexOf(i);});
    this.countCache = {};
    this.transposeCache = {};
    this.evalProcessors();
    this.calcColumnGroups();
    var groupTypes = _.keys(this.columnGroups);
    if(_.include(groupTypes, "timeColNames")) {
        if(_.include(groupTypes, "spaceColNames")) {
            //has time and space data
            this.type = "spacetime";
        } else {
            // just has time data
            this.type = "time";
        }
    } else if (_.include(groupTypes, "spaceColNames")) {
        // just has space data
        this.type = "space";

    } else {
        // no time or space
        this.type = "other";
    }
    var esta = this;
    _.each(["labelColumns","Topics","Info"], function(v) {
        if(esta.groups[v]) {
            _.extend(esta.showCols, esta.groups[v]);
        }
    });
    if (this.isTemporal()) {
        this.timeSelector = _.first(this.dateDivisions);
        _.extend(this.showCols, this.groups.timeColNames[this.timeSelector]);
    }
    //if (this.isSpatial()) {
        //this.spaceSelector = "s"; //TODO: fix this hardcoding
        //_.extend(this.showCols, this.groups.spaceColNames[this.spaceSelector]);
    //}
    this.calcColumnNumbers();
};

Metadata.prototype.isSpatial = function() {
    return _.include(["space","spacetime"], this.type);
};

Metadata.prototype.isTemporal = function() {
    return _.include(["time","spacetime"], this.type);
};

Metadata.prototype.evalProcessors = function() {
    var metadata = this;
    // functionize name and value processors
    var nameProcessors = {};
    _.each(metadata.nameProcessors, function(v,k) {
        nameProcessors[k] = eval("(function(value){ " + v + " })");
    });
    this.nameProcessors = nameProcessors;

    var valueProcessors = {};
    _.each(metadata.valueProcessors, function(v,k) {
        valueProcessors[k] = eval("(function(value){ " + v + " })");
    });
    this.valueProcessors = valueProcessors;
};

Metadata.prototype.calcColumnGroups = function() {
    var metadata = this;
    metadata.groups = {};
    metadata.groups.Base = {};
    var pre = {};
    // create pre processors and group storage objects
    _.each(metadata.columnGroups, function(group,key) {
        if (metadata.groups[key] === undefined) metadata.groups[key] = {};
        switch(key) {
            case "timeColNames":
                pre.dateDivisions = metadata.dateDivisions;
                console.log(pre.dateDivisions);
                pre.dateFormatTesters = _.map(pre.dateDivisions, function(dateCode) {
                    var idx = metadata.dateFormat.indexOf(dateCode);
                    return (function(dateObj) {
                        return dateObj[idx] !== 'X';
                    });
                });
                break;
            default:
                break;
        }
    });
    //transform columns while removing all the datecolumns
    _.each(metadata.columns, function(col,i) {
        var inGroup = false;
        _.each(metadata.columnGroups, function(names,key) {
            if(_.include(names, col)) {
                switch(key) {
                    case "timeColNames":
                        var dd;
                        _.each(pre.dateFormatTesters, function(dateTest,j) {
                            if(dateTest(col)) {
                                dd = pre.dateDivisions[j];
                            }
                        });
                        if(metadata.groups[key][dd] === undefined) metadata.groups[key][dd] = {};
                        var v = col;
                        metadata.groups[key][dd][i] = col;
                        break;
                    default: // includes "labelColumns" and "spaceColNames"
                        metadata.groups[key][i] = col;
                        break;
                }
                inGroup = true;
                //_.breakLoop();
            }
        });
        if(!inGroup) {
            metadata.groups.Base[i] = col;
        }
    });
};


Metadata.prototype.calcColumnNumbers = function() {
    var metadata = this;
    metadata.absoluteToRelative = {};
    metadata.relativeToAbsolute = {};
    var j = 0;
    _.each(this.showCols, function(col,i) {
        metadata.absoluteToRelative[i] = j;
        metadata.relativeToAbsolute[j] = i;
        j += 1;
    });
    this.showColsLength = j;
};

Metadata.prototype.isSpaceColumn = function(columnName) {
    var spaceCols = this.columnGroups.spaceColumns;
    if(spaceCols) {
        return _.include(spaceCols, columnName);
    } else {
        return false;
    }
};

Metadata.prototype.isTimeColumn = function(columnName) {
    var timeCols = this.columnGroups.timeColNames;
    if(timeCols) {
        return _.include(timeCols, columnName);
    } else {
        return false;
    }
};

Metadata.prototype.transpose = function(row,toTransposeOn,keys,callback) {
    //q={"collection":colection, "query" : {"action":"find", "args":[{"label1":label1_val, ...., "Location.s":{"$exists":True}, sliderKey:{"$exists"True}}], "kargs":{"fields":["Location.s",sliderKey]}}
    //construct the transpose object
    var locConstraint = {"Location.s" : {"$exists" : true}};
    //var labelConstraint  = {"
    var args = [Show.baseQuery];
    args.push({toTransposeOn : {"$exists" : true}});
    var kargs = [toTransposeOn];
    _.each(keys, function(k) {
        args.push({k : {"$exists" : true}});
        kargs.push(k);
    });
    kargs = {"fields":kargs};
    var q = [{ action: 'find', args: args, kargs: kargs}];
    var queryString = JSON.stringify({"query":q,"collection":Show.collectionName});
    $.ajax({
        url : Show.apiUrl+'/get',
        data : {q : queryString},
        dataType : 'jsonp',
        success : function(data) {
            console.log(data);
            callback(data);
        }
    });
};
