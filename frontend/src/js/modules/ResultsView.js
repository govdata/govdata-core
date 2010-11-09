goog.provide('gov.ResultsView');

goog.require('goog.events');
goog.require('goog.events.EventTarget');

//given object and level creates a view
gov.ResultsView = function(elem,dataHandler,resultRenderer,start,collapse) {
  this.data = dataHandler;
  this.resultRenderer = resultRenderer
  this.elem = elem
  this.start = start
  this.collapse = collapse
  goog.events.listen(this.data,
                    "newResults",
                    this.updateData,
                    false,
                    this);
  

};

gov.ClusterView = function(elem,data,metadata,start,collapse,context) {
  this.data = data;
  this.metadata = metadata
  this.element = $(this.html).appendTo(elem);
  this.start = start
  this.collapse = collapse
  this.context = context



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


var computeCommon = function(metadict,start){

  var sourceDict = new Object();
  
  for (name in metadict){

     var entry = metadict[name]
     
     var goodkeys = _.keys(entry["source"]).slice(start)
     
     sourceDict[name] = new Object() ; 
     
     for (k in goodkeys){
       var key = goodkeys[k]
       sourceDict[name][key] = entry["source"][key]
     }
     
  }

  var sourcekeys = _.map(_.values(sourceDict), function(entry){return _.keys(entry) ; })
  var sourcekeysRev = _.map(sourcekeys,function(n){n = n.slice(); n.reverse() ; return n})
    
  var commonL = _.reduce(sourcekeys,commonFinder,undefined)
  var commonR = _.reduce(sourcekeysRev,commonFinder,undefined)  
  commonR.reverse()
  
  if (_.isEqual(commonL,commonR)){
    commonR = []
  }
  return [commonL,commonR]
  
}
  

gov.ResultsView.prototype.updateData = function() {

  var docs = this.data.docs()
  var metadata = this.data.metadata
  
  var view = new gov.ClusterView(this.elem,docs,metadata,this.start,this.collapse,this)
  view.render()
  return view
  
  }
  
  
gov.ClusterView.prototype.render = function(){  

  var docs = this.data
  var metadict = this.metadata
  
  var start = this.start
  var collapse = this.collapse
  
  common = computeCommon(metadict,start)
  commonL = common[0]
  commonR = common[1]
  
  var collapseddocs = new Object();
  
  for (i in docs){
  
    var doc = docs[i]
    sourcename = _.map(_.values(metadict[doc["collectionName"]]["source"]).slice(start,collapse),function(obj){return obj["name"]}).join('__')
    if (sourcename in collapseddocs){
       collapseddocs[sourcename].push(doc)
       
    } else {
       collapseddocs[sourcename] = [doc]
    }
      
  }

  var html = "Cluster by: " + commonL.join(' ') + ' ... ' + commonR.join(' ') 
  for (key in collapseddocs){
    subcollapse = 0

    var colnames = _.uniq(_.map(collapseddocs[key],function(val){return val["collectionName"]; }))
    var newmetadict = new Object()
    
    for (k in colnames){
      var innerkey = colnames[k]
      newmetadict[innerkey] = metadict[innerkey]
    }

    if (subcollapse === 0){
      newcommon = computeCommon(newmetadict,start + collapse)
      html += "<br/><br/>" + key.split('__').join(' >> ') + ", Collapse by:" + newcommon[0].join(' ') + ' ... ' + newcommon[1].join(' ') + "<br/><br/>"
      html += this.context.resultRenderer(collapseddocs[key],collapse)
   
      this.element.html(html); 
      
    } else {

      new gov.ClusterView(key,collapseddocs[key],newmetadict,this.resultRendererFn,start + collapse,subcollapse)
    
    }
    
  }

   

}

gov.ClusterView.prototype.html = "<div id='results'></div>";

