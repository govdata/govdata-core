goog.provide('gov.ResultsView');

goog.require('goog.events');
goog.require('goog.events.EventTarget');

//given object and level creates a view
gov.ResultsView = function(elem,dataHandler,viewlevel,opts) {
  this.data = dataHandler;
  this.element = $(this.html).appendTo(elem);
  goog.events.listen(this.data,
                    "newResults",
                    this.updateData,
                    false,
                    this);
  

};

gov.ResultsView.prototype.updateData = function() {
  console.log("updatedata");
  var html = "<table>";
  _.each(this.data.docs(), function(d) {
    html += "<tr>";
    html += "<td>"+d.mongoID[0]+"</td>";
    html += "<td><a href='#/show?q=\""+d.mongoID[0]+"\"' >clickhere</a></td>";
    html += "</tr>";
  });
  html += "</table>";
  this.element.html(html);
}

gov.ResultsView.prototype.html = "<div id='results'></div>";

