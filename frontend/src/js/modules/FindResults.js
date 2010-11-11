goog.provide('gov.FindResults');

goog.require('goog.events');
goog.require('goog.events.EventTarget');

gov.FindResults = function(opts) {
  goog.events.EventTarget.call(this);
  var defaults = {metadatafn : _.identity}
  $.extend(this,defaults,opts);
};
goog.inherits(gov.FindResults, goog.events.EventTarget);

gov.FindResults.prototype.newResults = function(results) {
  this.results = results;
  this.docs = this.results.response.docs;
  var esta = this;
  this.metadatafn(this.docs, function(metadata) {
    esta.metadata = metadata;
    esta.dispatchEvent('newResults');
  });
};

