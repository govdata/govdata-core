goog.provide('gov.FindResults');

goog.require('goog.events');
goog.require('goog.events.EventTarget');

gov.FindResults = function() {
  goog.events.EventTarget.call(this);
};
goog.inherits(gov.FindResults, goog.events.EventTarget);

gov.FindResults.prototype.docs = function() {
  if(this.results === undefined) {
    return undefined;
  }
  return this.results.response.docs;
}

gov.FindResults.prototype.newResults = function(results) {
  this.results = results;
  console.log("newResults");
  this.dispatchEvent('newResults');
};
