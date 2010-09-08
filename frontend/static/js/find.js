var Find = {};
(function($) {
    
    Find.offset = 0;
    Find.buffer = 200;
    Find.max_offset = 1000000;
    Find.q = "";
    
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
    
    var getMoreItems = function( results ) {
        Find.offset += 1;
        if(Find.offset < Find.max_offset) {
            $.get('/', 
                {
                    q : Find.q,
                    partial : 'true',
                    page : Find.offset
                },
                function(data) {
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
        if ( moreItemsNeeded(results) ) {
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