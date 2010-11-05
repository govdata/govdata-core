goog.provide('gov.SearchBubble');

goog.require('goog.events');
goog.require('goog.events.EventTarget');

gov.SearchBubble = function(searchbar,value) {
  goog.events.EventTarget.call(this);
  this.value = value;
  searchbar.element.find('.bubbles').append(this.html(value));
}
goog.inherits(gov.SearchBubble, goog.events.EventTarget);

gov.SearchBubble.prototype.html = function() {
  return "<span class='bubble'>"+this.value+"</span>"; 
};
