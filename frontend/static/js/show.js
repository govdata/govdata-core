var Show = {};

(function($) {
    
    var SPECIAL_KEYS =  ['__versionNumber__','__retained__','__addedKeys__','__originalVersion__']
    
    Show.updatePositions = function() {
        $('#floatedArea').masonry({ 
            columnWidth: 200,
            resizeable: false, // handle resizing ourselves
            itemSelector: '.module' });                
    }
    
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
    
    Show.run = function(query, collectionName) {
                
        var apiUrl = "http://ec2-67-202-31-123.compute-1.amazonaws.com";
        var baseQuery = JSON.parse(query);
        var querySequence = [["find",[[{"name":collectionName}],{"fields":[
        "metadata.valueProcessors","metadata.nameProcessors","metadata.columnGroups",
        "metadata.source","metadata.columns","metadata.startDate","metadata.endDate",
        "metadata.contactInfo","metadata.keywords","metadata.dateFormat",
        "metadata.spatialDivisions","metadata.dateDivisions","metadata.description",
        "metadata.volume","metadata.shortTitle","metadata.title","metadata.sliceCols"]}]]];
                
        var serverData = function(opts,callback) {
            var q = [{ action: 'find', args: [baseQuery]}];
            var start = opts.start || 0;
            q.push({ action : 'skip', args : [start]});
            var limit = opts.limit || 30;
            q.push({ action : 'limit', args : [limit]});
            var timeQuery = {format : "Y"};
            var queryString = JSON.stringify({"timeQuery": timeQuery, 
                                    "query":q,"collection":collectionName});
            // var queryString = JSON.stringify({"query":q,"collection":collectionName});
            $.ajax({
                url : apiUrl+'/get',
                data : { q : queryString },
                dataType : 'jsonp',
                success : function(data) {
                    callback(data.data);
                }
            });
        };
        
        var toData = function(data,metadata) {
            var result = new Array();
            _.each(data, function(rowObj) {
                var row = new Array(metadata.showCols.length);
                _.each(rowObj, function(v,k) {
                    row[metadata.columnNumbers[k]] = v
                });
                result.push(row);
            });
            return result;
        }
        
        var onRowClick = function(row) {
            console.log(row);
        }
        
        // var toRotatedData = function(data,metdata) {
        //     var result = [];
        //     _.each(data, function(rowObj) {
        //         var row = [];
        //         _.each(metadata.showCols, function(col,i) {
        //             row[i] = rowObj[i+""] || 0;
        //         });
        //         result.push(row);
        //     });
        //     return result;
        // }
        
        // Initial load with metadata
        $.ajax({
            url : apiUrl+'/sources',
            data : { querySequence : JSON.stringify(querySequence) },
            dataType : 'jsonp',
            success : function(data) {
                var metadata = new Metadata(data[0].metadata);
                                                                
                $("#floatedArea").append('<div class="module col2"><div class="title">'+
                    metadata.title+" ("+metadata.shortTitle+')</div><div class="keywords">'+
                    metadata.keywords+'</div></div>');
                $("#floatedArea").append('<div class="module col2"><div class="description">'+
                    metadata.description+'</div></div>');
                if(metadata.contactInfo) {
                    $("#floatedArea").append('<div class="module col2">'+metadata.contactInfo+'</div>');
                }
                
                // $("#floatedArea").append('<div class="module col3"><div id="timeline" ></div></div>')
                // Show.timeline = new iv.Timeline({
                //   container : document.getElementById('timeline')
                // });
                
                $("#floatedArea").append('<div class="module col4"><div id="table"></div></div>')
                Show.table = new iv.Table({
                  container : document.getElementById('table'), 
                  metadata : metadata,
                  serverData : serverData,
                  transformer : toData,
                  onRowClick : onRowClick
                });

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
        
    };
})(jQuery);


var Metadata = function(metadata) {
    // extend yourself with metadata
    _.extend(this,metadata);
    this.evalProcessors();
    this.calcBaseCols();
    this.calcDateGroups();
    this.calcShowCols("Y");
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
}

Metadata.prototype.calcBaseCols = function() {
    var metadata = this;
    var baseCols = {};
    //transform columns while removing all the datecolumns
    _.each(metadata.columns, function(col,i) {
        var include = true;
        _.each(metadata.columnGroups, function(names) {
            if(_.include(names, col)) {
                include = false;
                _.breakLoop();
            }
        });
        if(include) {
            baseCols[i+""] = col;
        }
    });
    this.baseCols = baseCols;
};

Metadata.prototype.calcDateGroups = function() {
    var metadata = this;
    var timeCols = metadata.columnGroups.timeColNames;
    if(timeCols) {
        var dateFormat = metadata.dateFormat;
        var dateDivisions = metadata.dateDivisions;
        var dateFormatTesters = _.map(dateDivisions, function(dateCode) {
            var idx = dateFormat.indexOf(dateCode);
            return (function(dateObj) {
                return dateObj[idx] !== 'X';
            });
        });
        var dateGroups = {};
        var showCols = {};
        _.each(metadata.columns, function(col,i) {
            if(_.include(timeCols, col)) {
                _.each(dateFormatTesters, function(dateTest,j) {
                    if(dateTest(col)) {
                        var dd = dateDivisions[j];
                        if(dateGroups[dd] === undefined) dateGroups[dd] = {};
                        var v = col;
                        dateGroups[dd][i+""] = col;
                        _.breakLoop();
                    }
                });
            }
        });
        // select which column groups to use
        this.dateGroups = dateGroups;
    } else {
        this.dateGroups = {};
    }
};
    
Metadata.prototype.calcShowCols = function(selector) {
    var metadata = this;
    var niceDateCols = {};
    console.log(metadata.nameProcessors);
    _.map(metadata.dateGroups[selector], function(v,k) {
        niceDateCols[k] = metadata.nameProcessors.timeColNames(v);
    });
    this.showCols = _({}).extend(metadata.baseCols,niceDateCols)
    this.calcColumnNumbers();
};

Metadata.prototype.calcColumnNumbers = function() {
    var metadata = this;
    metadata.columnNumbers = {};
    var j = 0;
    _.each(this.showCols, function(col,i) {
        metadata.columnNumbers[i+""] = j;
        j += 1;
    });
}