define(["utils","jquery","jquery-ui","ui.linearchooser","ui.clusterelement"], function(utils) {
	function collapseData(data,metadata,collapse){

		var collapseddata = {};
		var sourcename;
		var sourcenamebasedict = {};
		var sourcenamebase;
		var prev_sourcename;
		
		var end = collapse;
	
		_.each(data, function(datum) {

		if (end === 0){
		  sourcename = _.pluck(
			_.values(metadata[datum.collectionName].source),
				"name").join('|');
		} else if (end == 'QUERY') {

		  sourcenamebase = _.pluck(_.values(metadata[datum.collectionName].source),
				"name").join('|');

		  if ((prev_sourcename !== undefined) && (prev_sourcename.split('|').slice(0,-1).join('|') == sourcenamebase)) {
			  sourcename = prev_sourcename;
		 
		  } else {
		  
			  if (sourcenamebase in sourcenamebasedict){
				  sourcenamebasedict[sourcenamebase] += 1;
			  } else {
				  sourcenamebasedict[sourcenamebase] = 0;
			  }	
			 			  
			  sourcename = sourcenamebase + '|' + sourcenamebasedict[sourcenamebase];

		  }
		  
		  prev_sourcename = sourcename;
		  
		} else{
 		  sourcename = _.pluck(
			_.values(metadata[datum.collectionName].source).slice(0,end),
				"name").join('|');
		} 
				
				
		collapseddata[sourcename] = (collapseddata[sourcename] || []);
		collapseddata[sourcename].push(datum);
		});
		return collapseddata;		
	};
	
	
	function jqueryescape(str) {
	  return str.replace(/([ #;&,.+*~\':"!^$[\]()=>|\/])/g,'\\$1') 
	};
	  
	
	function renderclusters(collapsedata,collapsedict,hidedict,facet_dict,metadata,start,collapse,context){
	
	  var key, data, colnames, subcollapse, newmetadata, elt, newcommon, keyID;
	  
	  var container = $("<div class='elementContainer'></div>").appendTo(context.widget());
	  
	  $.each(collapsedata,function(key,data){
		subcollapse = collapsedict.items[key] || 0; 
		colnames = _.uniq(_.map(data,function(val){return val["collectionName"][0]; }));
		newmetadata = utils.subdict(metadata,colnames);
		
		
		if (subcollapse === 0) {
			if (colnames.length > 1){
			   newcommon = utils.computeCommon(newmetadata,collapse);	
			} else {
			   newcommon = null;
			}
			   
			elt = $("<div class='clusterElement'></div>").appendTo(container).
			clusterelement({
					 key : key,
					 results : data,
					 common : newcommon,
					 start : start,
					 collapse : collapse,
					 hidedict : hidedict,
					 renderer: context.options.resultsRenderer,
					 facet_dict : facet_dict
			});
			
/*   			elt.find(".topBar .chooserSubElement").click(function(e){
   		
			var num = parseInt($(e.target)[0].id);

			if (num + 1 !== 0){

			  var subcollapse = collapse + num + 1;
			  console.log(key,subcollapse)
			  collapsedict.items[key] = subcollapse;
			  var newelt = $("<div class='clusterView'></div>");
			  var clusterElement = $(e.target).closest(".clusterElement")
			  clusterElement.replaceWith(newelt);
			  var olddata = collapsedata[key];
			  var colnames = _.uniq(_.map(olddata,function(val){return val["collectionName"][0]; }));
			  var oldmetadata = utils.subdict(metadata,colnames);
			  newelt.clusterview({					
					key : key,
					data : olddata,
					metadata : oldmetadata,
					start : start,
					collapsedict : collapsedict,
					hidedict : hidedict,
					resultsRenderer : context.options.resultsRenderer,
					facet_dict : facet_dict
			  });
			  
			  newelt.attr("id",key.replace(/ /g,'__'));
			  $(root).data()['statehandler'].changestate();

			}
			
		   
		  });			*/
											
		} else {
			elt = $("<div class='clusterView'></div>").
				appendTo(container).
				clusterview({
					key : key,
					data : data,
					metadata : newmetadata,
					start : start,
					collapsedict : collapsedict,
					hidedict : hidedict,
					resultsRenderer : context.options.resultsRenderer,
					facet_dict : facet_dict
				});
			
		};
		keyID = key.replace(/ /g,'__');
		elt.attr("id",keyID);
	  });

	  
	  return container	  
			
	 };
	  

   	$.widget( "ui.clusterview", {
		options : {
			start : 0
		},
		_create : function() {
			var self = this,
				key = this.options.key,
				data = this.options.data,
				metadata = this.options.metadata,
				collapsedict = this.options.collapsedict,
				hidedict = this.options.hidedict,
				start = this.options.start,
				facet_dict = this.options.facet_dict;
					
		
			var collapse, collapseddata;

			var common = utils.computeCommon(metadata,utils.count(key,'|'));
			var commonL = common[0], commonR = common[1];
			$.each(commonL,function(i,x){
				commonL[i] = '<div class="innerClusterChooser value">' + x[0].toUpperCase() + x.slice(1) + '</div>';
			});
			$.each(commonR,function(i,x){
				commonR[i] = '<div class="innerClusterChooser value">' + x[0].toUpperCase() + x.slice(1) + '</div>';
			});			
			
			var top = $("<div class='topBar'></div>").appendTo(this.element);
				
			var hide = hidedict.items[key] || false		
			var classtext;		
			if (hide === true){
				classtext = "keyLabel hide";
			 
			} else {
				classtext = "keyLabel";
			}
			
			var facet_text;
			if (key in facet_dict){
				facet_text = " <span class='facet'>(" + facet_dict[key] + ")</span>";
			} else {
				facet_text = '';
			}
			var keydiv = $("<div class='" + classtext + "'>" + key.split('|').slice(start).join(' >> ') + facet_text + "</div>").appendTo(top);
			
			var clusterTarget = $('#clusterTarget');
			
			clusterTarget.find('#clusterChooser').remove()
			lc = $("<div id='clusterChooser' class='linearChooser'></div>").
			appendTo(clusterTarget).
			linearchooser({
				data : {
					label : "<div>Cluster by:</div>",
					list : [{label: "front", list: commonL},{label:"back",list:commonR},{label:'query',list:['<div class="innerClusterChooser">Query</div>']}],
					resp : [_.range(commonL.length),_.range(-commonR.length,0),['QUERY']]
				}
			});
			
			var lList =  _.range(commonL.length).concat(_.range(-commonR.length,0));
			var chId;
			if (collapsedict.items[key] !== 'QUERY') { 
				chId = lList.slice(collapsedict.items[key] - 1)[0];
			} else {
				chId = 'QUERY'
			}
				

			lc.find('.chooserSubElement').removeClass('shown');
			lc.find('#' + chId).addClass('shown');
			
			lc.find(".chooserSubElement").click(function(e){
				var num = $(e.currentTarget)[0].id;
				if (num !== 'QUERY'){
					num = parseInt(num);
				}
				
				
				var parentLabel = $(e.currentTarget).parent().parent()[0].id;

				if (num !== 'QUERY'){
					collapse = utils.count(key,'|') + num + 1;
				} else {
					collapse = 'QUERY';
				}
				
								
				collapsedict.items[key] = collapse;
				collapseddata = collapseData(data,metadata,collapse);
					
				var subkey;
				$.each(self.element.find(":ui-clusterelement, :ui-clusterview"),function(ind,item){
				   subkey = item.id.replace(/__/g,' ');
				   delete collapsedict[subkey]
				   $(item).remove();
				   
				});
		
				
				var res = renderclusters(collapseddata,collapsedict,hidedict,facet_dict,metadata,utils.count(key,'|'),collapse,self);

				if (collapsedict.items[key] !== 'QUERY') {
				   chId =lList.slice(collapsedict.items[key] - 1)[0];
				} else {
				   chId = 'QUERY';
				}			
				lc.find('.chooserSubElement').removeClass('shown');
				lc.find('#' + chId).addClass('shown');
				
				$(root).data()['statehandler'].changestate();
				keydiv.click(function(){
				  res.toggle();
				  if (keydiv.hasClass("hide")){
 					keydiv.removeClass("hide");
 					hidedict.remove(key);
				  } else {
					keydiv.addClass("hide");
					hidedict.add(key,true);
				  }
				  $(root).data()['statehandler'].changestate();
				  
				});
		
			});
				 
		  collapse = collapsedict.items[key] || 0
		  var hide = hidedict.items[key] || false
		  collapseddata = collapseData(data,metadata,collapse);
		  var res = renderclusters(collapseddata,collapsedict,hidedict,facet_dict,metadata, utils.count(key,'|'),collapse,self);
		  if (hide === true){
			  res.hide();
		  }
		  keydiv.click(function(){
			 res.toggle();
			 if (keydiv.hasClass("hide")){
 			   keydiv.removeClass("hide");
 			   hidedict.remove(key);
			 } else {
			   keydiv.addClass("hide");
			   hidedict.add(key,true);
			 }
			 $(root).data()['statehandler'].changestate();
		  });
						 
	 
		},
		destroy : function() {
			this.element.empty();
		}
	});
	
});
