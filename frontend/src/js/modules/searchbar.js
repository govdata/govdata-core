goog.provide('gov.SearchBar');

goog.require('goog.events');
goog.require('goog.events.EventTarget');

(function() {

gov.SearchBar = function(elem,opts) {
  goog.events.EventTarget.call(this);
  this.escaped = false;
  this.state = {};
  this.bubbles = [];
  //var html = this.html;
  this.element = $(this.html).appendTo(elem);
  if(opts.query !== undefined) {
    this.query = opts.query;
    goog.events.listen(this.query,"update",this.update,false,this);
  }
  if(opts.autocomplete !== undefined) {
    this.element.find('input').autocomplete(opts.autocomplete);
  }
  bindKeys(this);
};
goog.inherits(gov.SearchBar, goog.events.EventTarget);

gov.SearchBar.prototype.update = function() {
  console.log("updated query");
};

gov.SearchBar.prototype.html = "<div id='searchbar'><span class='bubbles'></span><input>search</input></div>";

gov.SearchBar.prototype.addBubble = function(value) {
  this.bubbles.push(new gov.SearchBubble(this,value));
}

// constructor helper function to bind keys
var bindKeys = function(searchbar) {
  var input = searchbar.element.find('input');
  input.bind('keydown', 'return', function(e) {
    var val = _.trim(input.val());
    if(val === "") {
      console.log("query");
      console.log(searchbar.query);
      searchbar.query.items = [];
      _.each(searchbar.bubbles, function(b) {
        searchbar.query.items.push(b.value);
      });
      searchbar.query.submit();
    } else {
      searchbar.addBubble(val);
      _.defer(function(){input.val("");});
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
      searchbar.addBubble(val);
      _.defer(function(){input.val("");});
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
