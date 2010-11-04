var gov = gov || {};

(function($) {
    gov.Find = {};
    var Find = gov.Find;

    Find.Query = function() {
        this.items = [];
    };

    Find.Query.prototype.value = function() {
        return this.items.join(" ");
    };

    Find.addSearchBar = function() {
        var s = new gov.SearchBar("#content");
        s.bind('submit', function(e) {
            console.log('submit happened');
        });
    };

    $(document).ready(function() {
        console.log("ready");
        Find.addSearchBar();
        //match("/find", function() {
            //Find.addSearchBar();
            //Find.addWelcomeMsg();
        //});
        //match("/find?q=", function() {
            //Find.addSearchBar();
            //Find.addResults();
        //});
    });

})(jQuery);
