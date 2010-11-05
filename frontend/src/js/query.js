goog.provide('gov.Query');

goog.require('goog.events');
goog.require('goog.events.EventTarget');

gov.Query = function(submitFn) {
  goog.events.EventTarget.call(this);
  this.items = [];
  this.submitFn = submitFn || _.identity;
};
goog.inherits(gov.Query, goog.events.EventTarget);

gov.Query.prototype.value = function() {
  return this.items.join(' ');
};

gov.Query.prototype.update = function() {
  this.dispatchEvent('update');
};

gov.Query.prototype.submit = function() {
  this.dispatchEvent('submit');
  var params = {
    q : this.value(),
  }
  this.submitFn(params);  
};
