goog.provide('gov.SearchBubble');

goog.require('goog.events');
goog.require('goog.events.EventTarget');

(function() {

gov.SearchBubble = function(searchbox) {
  goog.events.EventTarget.call(this);
}
goog.inherits(gov.SearchBar, goog.events.EventTarget);


gov.SearchBubble.prototype.html = "<div class='bubble'></div>";

})();
