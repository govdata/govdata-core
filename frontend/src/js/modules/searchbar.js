var gov = gov || {};

(function($) {
    var html = "<div id='searchbar'><input>search</input></div>";
    gov.SearchBar = function(node) {
        var searchbar = $(node).append(html);
        var input = searchbar.find('input');
        input.keypress(function(e) {
            var code = (e.keyCode ? e.keyCode : e.which);
            switch(code) {
                case 13: //return
                    console.log('return');
                    searchbar.trigger('submit');
                    e.preventDefault();
                    break;
                default:
                    searchbar.trigger('queryChanged', input.val());
                    console.log(e.keyCode);
                    break;
            }
        });
        searchbar.bind('queryChanged', function(e) {

        });
        return searchbar;
    };

})(jQuery);
