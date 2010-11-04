goog.provide('gov.SearchBar');

goog.require('goog.events');
goog.require('goog.events.EventTarget');

(function() {

gov.SearchBar = function(node,query) {
  goog.events.EventTarget.call(this);
  this.escaped = false;
  this.state = {};
  //var html = this.html;
  this.element = $(node).append(this.html);
  bindKeys(this);
  if(query !== undefined) {
    this.query = query;
    goog.events.listen(query,"update",this.update,false,this);
  }
};
goog.inherits(gov.SearchBar, goog.events.EventTarget);

gov.SearchBar.prototype.update = function() {
  console.log("updated query");
};

gov.SearchBar.prototype.html = "<div id='searchbar'><input>search</input></div>";

gov.SearchBar.prototype.addParam = function(param) {

}

// constructor helper function to bind keys
var bindKeys = function(searchbar) {
  var input = searchbar.element.find('input');
  input.bind('keydown', 'return', function(e) {
    var val = _.trim(input.val());
    if(val === "") {
      console.log("query");
    } else {
      console.log("add word");
      input.val("");
    }
    return false;
  });
  input.bind('keydown','tab space', function(e) {
    if(searchbar.escaped === true) {
        return true;
    }
    var val = _.trim(input.val());
    if(val === "") {
      console.log("do nothing");
    } else {
      console.log("add word");
      input.val("");
    }
    return false;
  });
  input.bind('keydown','(', function(e) {
    searchbar.escaped = true;
    return true;
  });
  input.bind('keydown',')', function(e) {
    searchbar.escaped = false;
    return true;
  });
  input.bind('keydown',"' shift+'", function(e) {
    console.log("quote");
    searchbar.escaped = !searchbar.escaped;
    return true;
  });
  input.keydown(function(e) {
    console.log(e.which);
  });

}

})();
