var Find = {};
(function($) {
    
    Find.offset = 0;
    Find.buffer = 200;
    Find.max_offset = 1000000;
    Find.q = "";
    Find.filters = [];
    
    Find.addFilter = function(filter) {
        console.log(filter);
        Find.filters.push(filter);
        var results = $('#results tbody');
        results.html("");
        getItems(function(data) {
            results.append(data);
        });
    }
    
    var registerAutoComplete = function() {        
    }
    
    var registerLiveEvents = function() {
        // $('#results tbody tr').live('mouseover mouseout', function(event) {
        //     if (event.type === 'mouseover') {
        //         $(this).find('.hidden').removeClass('hidden').addClass('showing');
        //     } else {
        //         $(this).find('.showing').removeClass('showing').addClass('hidden');
        //     }
        //     return true;
        // });
    }
    
    var getItems = function( callback ) {
        $.get('/', 
            $.param({
                q : Find.q,
                partial : true,
                page : Find.offset,
                filter : Find.filters
            },true),
            function(data) {
                callback(data);
            });
    }
    
    var getMoreItems = function( results ) {
        Find.offset += 1;
        if(Find.offset < Find.max_offset) {
            getItems(function(data) {
                results.append(data);
            });
        }
    }
    
    var moreItemsNeeded = function( results ) {
        var w = $(window);
        var wheight = w.height();
        var woffset = w.scrollTop();
        var rheight = results.height();
        var roffset = results.offset().top;
        if ((rheight - roffset) < (woffset + wheight + Find.buffer)) {
            return true;
        } else {
            return false;
        }
    }
    
    var checkContents = function( results ) {
        if ( results.length !== 0 && moreItemsNeeded(results) ) {
            getMoreItems(results);
        }
    }
        
    Find.run = function() {
        registerAutoComplete();
        registerLiveEvents();
    }
    
    
    Find.registerAutoPaginate = function() {
        var results = $('#results tbody');
        $( window ).bind("scroll resize", function( event ){
            checkContents( results );
        });
        checkContents( results );
    }

})(jQuery);