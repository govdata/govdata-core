goog.provide('gov.FindResults');

goog.require('goog.events');
goog.require('goog.events.EventTarget');

gov.FindResults = function(getmetadatafn) {
  goog.events.EventTarget.call(this);
  this.getmetadata = getmetadatafn
};
goog.inherits(gov.FindResults, goog.events.EventTarget);

gov.FindResults.prototype.docs = function() {
  if(this.results === undefined) {
    return undefined;
  }
  return this.results.response.docs;
}

gov.FindResults.prototype.newResults = function(results) {
  console.log("newResults");
  this.results = results;
  this.getmetadata(this.docs())  
};

gov.FindResults.prototype.metadataDone = function(metadata){
  this.metadata = metadata
  this.dispatchEvent('newResults');
};