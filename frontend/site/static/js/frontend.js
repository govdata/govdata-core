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
            // boxify();
            GovLove.find(q, function(data) {
                console.log(data);
                window.d = data;
                var doc = data.response.docs[0];
                var getQuery = GovLove.convertFindDocToGetQuery(doc);
                window.getQuery = getQuery;
                console.log(getQuery);
                GovLove.get(getQuery,function(getResult) {
                    console.log("result");
                    console.log(getResult);
                });
                GovLove.templates.find_result(data, function(html) {
                    $("#results").html(html);
                });
            });
            return false;
        });
        $("#q").bind('keydown','tab',$("#search_form").submit);
        $("#q").bind('keydown','ctrl+space',autocomplete);
    }
    
    var registerAutoComplete = function() {
        
    }
    
    Frontend.run = function() {
        // registerSearchBar();
        registerAutoComplete();
    }

})(jQuery);