var Show = {};

(function($) {
    
    Show.updateSizes = function() {
        var w = $(window);
        var head = $("#header");
        var width = w.width();
        var height = w.height()-head.height();
        if(Show.table.container) {
            $(Show.table.container).css({width: width*1, height: height*0.5},100);
        }
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
    
    Show.run = function(query, collectionName, volume) {

        Show.example = [];
        _.each(_.range(100), function(x) {
            var row = {};
            _.each(_.range(100), function(y) {
                row[x+' '+y] = x*y;
            });
            Show.example.push(row);
        });
        
        var apiUrl = "http://ec2-67-202-31-123.compute-1.amazonaws.com";
        var querySequence = [["find",[[{"name":collectionName}],{"fields":["metadata.valueProcessors","metadata.nameProcessors","name","metadata.columnGroups","metadata.source","metadata.columns"]}]]];

        var queryTranslator = function(query, callback) {
          return apiUrl;
        };
        
        var countCalculator = function(query, callback) {
            return apiUrl;
        };
        
        $.ajax({
            url : apiUrl+'/sources',
            data : { querySequence : JSON.stringify(querySequence) },
            dataType : 'jsonp',
            success : function(data) {
                var m = data[0].metadata;
                var metadata = _({}).extend( m, {
                            numCols : m.columns.length,
                            numRows : volume
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
                
                Show.collection.metadata.numRows = 100;
                Show.collection.metadata.numCols = 100;
                Show.table.render();
                Show.updateSizes();
            }
        });
        
        $(window).resize(function() {
            // Use timer method so this event doesn't fire All the time
            clearTimeout(Show.resizeTimer); 
            Show.resizeTimer = setTimeout(Show.updateSizes, 500);
        });
        
    };
})(jQuery);
