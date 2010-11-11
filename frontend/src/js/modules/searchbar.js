goog.provide('gov.SearchBar');
goog.provide('gov.SearchBarRenderer');

goog.require('goog.ui.Container');
goog.require('goog.ui.Component');
goog.require('goog.ui.Control');
goog.require('goog.ui.ContainerRenderer');

gov.SearchBar = function(opt_domHelper) {
  goog.ui.Component.call(this, opt_domHelper);
};
goog.inherits(gov.SearchBar, goog.ui.Component);

gov.SearchBar.prototype.createDom = function() {
  this.setElementInternal(
      this.getDomHelper().createDom('div'));
  this.input = new gov.SearchBarInput();
  this.input.render();
  this.addChild(this.input);
  this.init_();
};

gov.SearchBar.prototype.init_ = function() {
  console.log("bind events");
};

gov.SearchBarInput = function(opt_domHelper) {
  goog.ui.Component.call(this, opt_domHelper);
};
goog.inherits(gov.SearchBarInput, goog.ui.Component);

gov.SearchBarInput.prototype.createDom = function() {
  console.log(this.parent);
  $(this.parent.getElement()).append("<input type='text'>");
  //this.setElementInternal(
      //this.getDomHelper().createDom('input', {'type': 'text'}));
  this.init_();
};

gov.SearchBarInput.prototype.init_ = function() {
};
