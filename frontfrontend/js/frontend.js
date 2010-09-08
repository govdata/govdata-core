var Frontend = {};
(function($) {
    var registerSearchBar = function() {
        $("#search").submit(function() {
            var q = $("#search input").val();
            Gov.find(q, function(data) {
                window.d = data;
                Gov.docs = data.response.docs;
                if( C.option('hierarchy') ) {
                    $("#results").html(
                        Gov.renderHierarchy(
                            Gov.hierarchy(Gov.docs)));
                } else {
                    $("#results").html(
                            Gov.renderCollapsed(Gov.docs));
                }
                // TODO: hack to make background gradient continue to work when document height changes
                $("html").css("height",$(document).height());
            });
            return false;
        });
    }
    
    var registerAutoComplete = function() {        
    }
    
    var registerLiveEvents = function() {
        $('#hierarchy_result ul').live('mouseover mouseout', function(event) {
          if (event.type === 'mouseover') {
              $(this).css('background-color','#fff').addClass('shadow');
              return false;
          } else {
            $(this).css('background-color','#f6f6e6').removeClass('shadow');
            return false;
          }
        });
    }
    
    var addOptionsPanel = function() {
        C.apply_template('_options', C._persistent, function(html) {
            $('html').prepend('<div id="options">'+html+'</div>').find('#options')
                .dropDownMenu('&lsaquo;options&rsaquo;', 100);
            $('#options_form').change(function() {
                Frontend.updateOptions();
            });
        });
    }
    
    Frontend.updateOptions = function() {
        $("#options input").each(function(index, item) {
            C._persistent.options[item.name] = item.checked;
        });
        C.savePersistentData();
    }
    
    Frontend.openDoc = function(doc) {
        var query = Gov.getQueryForDoc(doc);
        Gov.get(query, function(d) {
            C.println(d);
        });
    }
    
    var defaults = {
        levels: 2
    }
    
    Frontend.run = function() {
        C._persistent_name = 'GovData';
        C.loadCookie({options: defaults});
        C.savePersistentData();
        // addOptionsPanel();
        registerSearchBar();
        registerLiveEvents();
    }

})(jQuery);