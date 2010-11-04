goog.provide('gov.Query');

goog.require('goog.events');
goog.require('goog.events.EventTarget');

gov.Query = function() {
  goog.events.EventTarget.call(this);
  this.items = [];
};
goog.inherits(gov.Query, goog.events.EventTarget);

gov.Query.prototype.value = function() {
  return this.items.join(" ");
};

gov.Query.prototype.update = function() {
  this.dispatchEvent("update");
}
