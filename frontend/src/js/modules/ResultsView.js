goog.provide('gov.ResultsView');

goog.require('goog.events');
goog.require('goog.dom')
goog.require('goog.events.EventTarget');

(function(){
//given object and level creates a view
gov.ResultsView = function(container,dataHandler,resultRenderer,start,collapse) {
  this.dataHandler = dataHandler;
  this.resultRenderer = resultRenderer;
  this.container = container;
  this.start = start;
  this.collapse = collapse;
  goog.events.listen(this.dataHandler,
                    "newResults",
                    this.newResults,
                    false,
                    this);
};

gov.ResultsView.prototype.newResults = function() {
  var docs = this.dataHandler.docs;
  var metadata = this.dataHandler.metadata;
  var view = new gov.ClusterView(this.container,docs,metadata,this.start,this.collapse,this);
  view.render();
  return view;
};
  
gov.ClusterView = function(container,data,metadata,start,collapse,context) {
  this.data = data;
  this.metadata = metadata;
  this.container = container;
  this.element = $(this.html).appendTo(this.container);
  this.start = start;
  this.collapse = collapse;
  this.context = context;
};


var subdict = function(olddict,subkeys){ 
  return _.reduce(subkeys,function(a,b){
    a[b] = olddict[b];
    return a;
    },{});
};
  
var commonFinder = function(listoflists) {
  var commonList = [];
  _.each(_.first(listoflists), function(obj, i) {
    var rowi = _.uniq(_.map(listoflists, function(x) { return x[i]; }));
    if (rowi.length > 1 || rowi[0] === undefined) {
      _.breakLoop();
    } else {
      commonList.push(obj);
    }
  });
  return commonList;
};


var computeCommon = function(metadict,start){
  var sourceDict = {};
  for (name in metadict) {
     var entry = metadict[name];
     var goodkeys = _.keys(entry["source"]).slice(start);
     sourceDict[name] = {}; 
     for (k in goodkeys) {
       var key = goodkeys[k];
       sourceDict[name][key] = entry["source"][key];
     }
  }
  var sourcekeys = _.map(_.values(sourceDict), function(entry){return _.keys(entry) ; });
  var sourcekeysRev = _.map(sourcekeys,function(n){n = n.slice(); n.reverse() ; return n});
  var commonL = commonFinder(sourcekeys);
  var commonR = commonFinder(sourcekeysRev);
  commonR.reverse();
  if (_.isEqual(commonL,commonR)) {
    commonR = [];
  }
  return [commonL,commonR];
};
  
  
gov.ClusterView.prototype.render = function() {  
  var data = this.data;
  var metadict = this.metadata;
  var start = this.start;
  var collapse = this.collapse;
  var common = computeCommon(metadict,start);
  var commonL = common[0];
  var commonR = common[1];
  var collapseddata = {};
  _.each(data, function(datum) {
    var sourcename = _.pluck(
              _.values(metadict[datum.collectionName].source).slice(start,collapse),
              "name").join('__');          
     collapseddata[sourcename] = (collapseddata[sourcename] || []);
     collapseddata[sourcename].push(datum);
  });
  
  var lc = new gov.LinearChooser(this.element,[["front",commonL],["back",commonR]],"Cluster by: "," | ");
  lc.element.find(".ChooserElement span").click(function(e){
     console.log(e.target.id,$(e.target.parentNode).attr("name"))
  });

  var html = ""
  for (key in collapseddata) {
    subcollapse = 0; 
    var colnames = _.uniq(_.map(collapseddata[key],function(val){return val["collectionName"]; }));
    var newmetadict = subdict(metadict,colnames);

    if (subcollapse === 0) {
      var newcommon = computeCommon(newmetadict,start + collapse);
      html += "<br/><br/>" + key.split('__').join(' >> ') + ", Collapse by:" + newcommon[0].join(' ') + ' ... ' + newcommon[1].join(' ') + "<br/><br/>"
      html += this.context.resultRenderer(collapseddata[key],collapse);
      this.element.append(html);       
    } else {
      var subview = new gov.ClusterView(this.element,collapseddata[key],newmetadict,this.resultRendererFn,start + collapse,subcollapse);
      subview.render();
    }
  } 
}
gov.ClusterView.prototype.html = "<div id='results'></div>";

gov.LinearChooser  = function(elem,dataArray,startHtml,sepHtml) {
  this.element = $(this.html).appendTo(elem);
  this.element.html(startHtml)
  var i;
  for (i in dataArray){
    new gov.ChooserElement(this.element,i,dataArray[i]);
    this.element.append(sepHtml);
  }     
}
goog.inherits(gov.LinearChooser, goog.events.EventTarget);
gov.LinearChooser.prototype.html = "<div id='LinearChooser'></div>";

gov.ChooserElement = function(elem, ind, data) {
  this.element = $(this.html).appendTo(elem);
  this.data = data[1];
  var i;
  for (i in this.data){
    new gov.ChooserSubElement(this.element,i,this.data[i])
  }
  this.element.attr("id",ind)
  this.element.attr("name",data[0])
}
goog.inherits(gov.ChooserElement, goog.events.EventTarget);
gov.ChooserElement.prototype.html = "<div class='ChooserElement'></div>";


gov.ChooserSubElement = function(elem,i,text){
   this.element = $(this.html).appendTo(elem);
   this.element.attr("id",i)
   this.element.html(text)
}

gov.ChooserSubElement.prototype.html = "<span class='ChooserSubElement'></span>";

})();