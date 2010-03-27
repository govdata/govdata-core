var Frontend = {};
(function($) {
    var boxify = function() {
        console.log("boxify");
        var data = $("#q").val()
        if(data === "") { return; }
        $("#terms").append("<button class='term'>"+data+"</button>");
        $("#q").val("");
        $("#q").css({width:($("#content").width()-$("#terms").width()-10)},function() {
            $("#q").val("").focus();
        });
        return false;
    }
    
    var autocomplete = function() {
        console.log("autocomplete");
    }
    
    var registerSearchBar = function() {
        $("#search_form").submit(function() {
            var q = $("#q").val()
            boxify();
            BetterData.find(q, function(data) {
                BetterData.templates.result(data, function(html) {
                    $("#results").html(html);
                })
                console.log(data);
            });
            return false;
        });
        $("#q").bind('keydown','tab',$("#search_form").submit);
        $("#q").bind('keydown','ctrl+space',autocomplete);
    }
    
    var registerAutoComplete = function() {
        
    }
    
    Frontend.run = function() {
        registerSearchBar();
        registerAutoComplete();
    }

})(jQuery);