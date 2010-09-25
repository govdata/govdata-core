var Show = {};

(function($) {
    
    Show.updatePositions = function() {
        $('#floatedArea').masonry({ 
            columnWidth: 200,
            resizeable: false, // handle resizing ourselves
            itemSelector: '.module' });                
    }
    
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
            var queryString = JSON.stringify({"query":q,"collection":collectionName});
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
            var result = [];
            _.each(data, function(rowObj) {
                var row = [];
                _.each(metadata.columns, function(col,i) {
                    row[i] = rowObj[i+""] || 0;
                });
                result.push(row);
            });
            return result;
        }
        
        var onRowClick = function(row) {
            console.log(row);
        }
        
        var toRotatedData = function(data,metdata) {
            var result = [];
            _.each(data, function(rowObj) {
                var row = [];
                _.each(metadata.columns, function(col,i) {
                    row[i] = rowObj[i+""] || 0;
                });
                result.push(row);
            });
            return result;
        }
        
        // Initial load with metadata
        $.ajax({
            url : apiUrl+'/sources',
            data : { querySequence : JSON.stringify(querySequence) },
            dataType : 'jsonp',
            success : function(data) {
                var m = data[0].metadata;
                // remove unwanted columns
                m.columns = _.reject(m.columns,function(col){return _.startsWith(col,"__")});
                
                // functionize name and value processors
                var nameProcessors = {};
                _.each(m.nameProcessors, function(v,k) {
                    nameProcessors[k] = eval("(function(value){ " + v + " })");
                });
                m.nameProcessors = nameProcessors;

                var valueProcessors = {};
                _.each(m.valueProcessors, function(v,k) {
                    valueProcessors[k] = eval("(function(value){ " + v + " })");
                });
                m.valueProcessors = valueProcessors;
                
                var metadata = _({}).extend( m, {
                            numCols : m.columns.length,
                            numRows : m.volume
                });
                $("#floatedArea").append('<div class="module col2"><div class="title">'+
                    metadata.title+" ("+metadata.shortTitle+')</div><div class="keywords">'+
                    metadata.keywords+'</div></div>');
                $("#floatedArea").append('<div class="module col1"><div class="description">'+
                    metadata.description+'</div></div>');
                if(metadata.contactInfo) {
                    $("#floatedArea").append('<div class="module col3">'+metadata.contactInfo+'</div>');
                }
                $("#floatedArea").append('<div class="module col3"><div id="timeline" ></div></div>')
                Show.timeline = new iv.Timeline({
                  container : document.getElementById('timeline')
                });
                
                $("#floatedArea").append('<div class="module col4"><div id="table"></div></div>')
                Show.table = new iv.Table({
                  container : document.getElementById('table'), 
                  metadata : metadata,
                  serverData : serverData,
                  transformer : toData,
                  onRowClick : onRowClick
                });

                $("#floatedArea").append('<div class="module col4"><div id="rotatedTable"></div></div>')
                Show.rotatedTable = new iv.Table({
                  container : document.getElementById('rotatedTable'), 
                  metadata : metadata,
                  serverData : serverData,
                  transformer : toData,
                  onRowClick : onRowClick
                });

                Show.timeline.render();
                Show.table.render();
                Show.rotatedTable.render();
                
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
