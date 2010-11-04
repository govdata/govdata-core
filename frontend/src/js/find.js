goog.provide('gov.find');

gov.find.addSearchBar = function() {
  gov.find.searchbar = new gov.SearchBar("#content", gov.find.query);
  goog.events.listen(gov.find.searchbar,
                    "keypress",
                    gov.find.keypress,
                    false,
                    gov.find);
};

gov.find.onLoad = function() {
  gov.find.query = new gov.Query();
  gov.find.addSearchBar();
};

gov.find.keypress = function() {
  console.log('keypress');
};

