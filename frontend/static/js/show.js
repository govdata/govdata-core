var Show = {};

(function($) {
    
    Show.updateSizes = function() {
        var w = $(window);
        var head = $("#header");
        var width = w.width();
        var height = w.height()-head.height();
        if(Show.table && Show.table.container) {
            $(Show.table.container).css({width: width*1, height: height*0.5},100);
        }
        Show.table.render();
        Show.timeline.render();
        $('#floatedArea').masonry({ 
            columnWidth: 200,
            resizeable: false, // handle resizing ourselves
            itemSelector: '.module' });
                
        // var floatAreaWidth = _.reduce($("#floatedArea .module"), function(memo,module){ return memo + $(module).outerWidth() + 10;}, 0)
        // $("#floatedArea").animate({width: Math.min(floatAreaWidth,width)},100)
        // Double the resize to go full while avoiding the window scrollbar only need when absolute positioning
        // if(Show.doubleResizeAllTheWay === undefined) {
        //     Show.resizeTimer = setTimeout(function() {
        //         Show.doubleResizeAllTheWay = true;
        //         Show.updateSizes();
        //         Show.doubleResizeAllTheWay = undefined;
        //     }, 500);
        // }
    }
    
    Show.run = function(query, collectionName) {

        Show.example = [];
        _.each(_.range(100), function(x) {
            var row = {};
            _.each(_.range(100), function(y) {
                row[x+' '+y] = x*y;
            });
            Show.example.push(row);
        });
        
        var apiUrl = "http://ec2-67-202-31-123.compute-1.amazonaws.com";
        var baseQuery = JSON.parse(query);
        var querySequence = [["find",[[{"name":collectionName}],{"fields":["metadata.valueProcessors","metadata.nameProcessors","name","metadata.columnGroups","metadata.source","metadata.columns"]}]]];

        var queryTranslator = function(opts, callback) {
            var q = [{ action: 'find', args: [baseQuery]}];
            var start = opts.start || 0;
            q.push({ action : 'skip', args : [start]});
            var limit = opts.limit || 30;
            q.push({ action : 'limit', args : [limit]});
            var queryString = JSON.stringify({"query":q,"collection":collectionName});
            console.log(queryString);
            $.ajax({
                url : apiUrl+'/get',
                data : { q : queryString },
                dataType : 'jsonp',
                success : function(data) {
                    callback(data.data);
                }
            });
          return apiUrl;
        };
        
        var countCalculator = function(opts, callback) {
            var countQuery = [{"action":"find", "args":[baseQuery]},{"action":"count"}];
            var queryCountString = JSON.stringify({"query":countQuery,"collection":collectionName});
            console.log(queryCountString);
            $.ajax({
                url : apiUrl+'/get',
                data : { q : queryCountString },
                dataType : 'jsonp',
                success : function(data) {
                    callback(data.data);
                }
            });
            // count_query = [{"action":"find", "args":[base_query]},{"action":"count"}]
            // base_query_count_string = json.dumps({"query":count_query,"collection":collection})
            // request1 = options.api_url + '/get?q=' + quote(base_query_count_string)
            // iTotalRecords = json.loads(urllib2.urlopen(request1).read())['data']
            // self.COUNT_CACHE[(query_string,collection)] = iTotalRecords
            return apiUrl;
        };
        
        $('#floatedArea').masonry({ 
            columnWidth: 200,
            resizeable: false, // handle resizing ourselves
            itemSelector: '.module' });
        
        // Initial load with metadata
        countCalculator({},function(numRows){
            console.log(numRows);
            $.ajax({
                url : apiUrl+'/sources',
                data : { querySequence : JSON.stringify(querySequence) },
                dataType : 'jsonp',
                success : function(data) {
                    var m = data[0].metadata;
                    var metadata = _({}).extend( m, {
                                numCols : m.columns.length,
                                numRows : numRows
                    });

                    Show.collection = new iv.Collection({
                      metadata : metadata,
                      queryTranslator : queryTranslator,
                      countCalculator : countCalculator
                    });

                    Show.table = new iv.Table({
                      container : document.getElementById('table'), 
                      collection : Show.collection 
                    });

                    Show.timeline = new iv.Timeline({
                      container : document.getElementById('timeline'), 
                      collection : Show.collection 
                    });

                    Show.updateSizes();
                }
            });
        });
        
        
        $(window).resize(function() {
            // Use timer method so this event doesn't fire All the time
            clearTimeout(Show.resizeTimer); 
            Show.resizeTimer = setTimeout(Show.updateSizes, 500);
        });
        
    };
})(jQuery);
