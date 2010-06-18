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
            var q = $("#q").val();
            GovLove.find(q, function(data) {
                window.d = data;
                GovLove.docs = data["response"]["docs"];
                GovLove.templates.find_result(data, function(html) {
                    $("#results").html(html);
                });
            });
            return false;
        });
    }
    
    var registerAutoComplete = function() {        
    }
    
    Frontend.openDoc = function(doc) {
        var query = GovLove.getQueryForDoc(doc);
        console.log(query);
        GovLove.get(query, function(d) {
            console.log(d);
        });
    }
    
    Frontend.run = function() {
        registerSearchBar();
    }

})(jQuery);