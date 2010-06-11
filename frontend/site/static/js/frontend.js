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
        GovLove.timeline(query);
    }
    
    Frontend.table = function(doc_id) {
        var doc = GovLove.docs[doc_id];
        console.log(doc);
        var t = $('<div class="table" style="height: 200px; width: 200px; margin-top: 50px;"></div>');
        t.appendTo($("body"));
        t.show().dialog({ 
                        autoOpen: true,
                        modal: false,
                        option: "stack",
                        width: 800,
                        height: 500 });        
    }
    
    Frontend.run = function() {
        registerSearchBar();
    }

})(jQuery);