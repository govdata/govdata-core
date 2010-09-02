var Frontend = {};
(function($) {
    var registerSearchBar = function() {
        $("#search").submit(function() {
            var q = $("#search input").val();
            GovLove.find(q, function(data) {
                window.d = data;
                GovLove.docs = data.response.docs;
                if( C.option('hierarchy') ) {
                    $("#results").html(
                        GovLove.renderHierarchy(
                            GovLove.hierarchy(GovLove.docs)));
                } else {
                    $("#results").html(
                            GovLove.renderCollapsed(GovLove.docs));
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
            $('#options').html(html).dropDownMenu('&lsaquo;options&rsaquo;', 100);
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
        var query = GovLove.getQueryForDoc(doc);
        GovLove.get(query, function(d) {
            C.println(d);
        });
    }
    
    var defaults = {
        hierarchy: true
    }
    
    Frontend.run = function() {
        C._persistent_name = 'govlove_data';
        C.loadCookie({options: defaults});
        C.savePersistentData();
        registerSearchBar();
        addOptionsPanel();
        registerLiveEvents();
    }

})(jQuery);