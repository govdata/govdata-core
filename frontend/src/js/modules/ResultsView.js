goog.provide('gov.ResultsView');

goog.require('goog.events');
goog.require('goog.events.EventTarget');

//given object and level creates a view
gov.ResultsView = function(elem,dataHandler,resultRenderer) {
  this.data = dataHandler;
  this.resultRenderer = resultRenderer
  this.element = $(this.html).appendTo(elem);
  goog.events.listen(this.data,
                    "newResults",
                    this.updateData,
                    false,
                    this);
  

};

var commonFinder = function(common,next){
    if (common === undefined){
      return next;
    } else {
      
      var z = _.zip(_.range(Math.max(common.length,next.length)),common,next)
      var upto = _.detect(z,function(val){return val[1] !== val[2]; } ) ; 
      if (upto !== undefined){
        return common.slice(0,upto[0])
      } else {
        return common
      }
    }
  
  }


gov.ResultsView.prototype.clusterView = function(docs,start,collapse){

  var sourceDict = new Object();
  
  for (i in this.data.metadata){
     var entry = this.data.metadata[i]
     
     var goodkeys = _.keys(entry["metadata"]["source"]).slice(start)
     
     sourceDict[entry["name"]] = new Object() ; 
     
     for (k in goodkeys){
       key = goodkeys[k]
       sourceDict[entry["name"]][key] = entry["metadata"]["source"][key]
     }
     
  }

  var sourcekeys = _.map(_.values(sourceDict), function(entry){return _.keys(entry) ; })
  var sourcekeysRev = _.map(sourcekeys,function(n){n = n.slice(); n.reverse() ; return n})
    
  var commonL = _.reduce(sourcekeys,commonFinder,undefined)
  var commonR = _.reduce(sourcekeysRev,commonFinder,undefined)  
  commonR.reverse()
  
  
  collapsedDocs = new Object();
  
  for (i in docs){
  
    var doc = docs[i]
    sourcename = _.map(_.values(sourceDict[doc["collectionName"]]).slice(0,collapse),function(obj){return obj["name"]}).join('__')
    if (sourcename in collapsedDocs){
       collapsedDocs[sourcename].push(doc)
       
    } else {
       collapsedDocs[sourcename] = [doc]
    }
      
  }
    
  var html = "Cluster by: " + commonL.join(' ') + ' ... ' + commonR.join(' ') 
  for (key in collapsedDocs){
    subcollapse = 0
    
    if (subcollapse === 0){
      html += "<br/><br/>" + key.split('__').join(' >> ') + "<br/><br/>"
      html += this.resultRenderer(collapsedDocs[key],collapse)
    } else {

      html += this.clusterView(collapsedDocs[key],start + collapse,subcollapse)
    
    }
  }

  return html

}

gov.ResultsView.prototype.updateData = function() {

  var docs = this.data.docs()
  
  var html = this.clusterView(docs,0,2)
  this.element.html(html);
  

}

gov.ResultsView.prototype.html = "<div id='results'></div>";

