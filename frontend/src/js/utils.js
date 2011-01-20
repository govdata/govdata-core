define(["underscore"],function() {
	return {
		subdict : function(olddict,subkeys){
			return _.reduce(subkeys,function(a,b){
				a[b] = olddict[b];
				return a;
				},{});
		},
		commonFinder : function(listoflists) {
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
		},
		computeCommon : function(metadict,start){
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
			var commonL = this.commonFinder(sourcekeys);
			var commonR = this.commonFinder(sourcekeysRev);
			commonR.reverse();
			if (_.isEqual(commonL,commonR)) {
				commonR = [];
			}
			return [commonL,commonR];
		},
		count : function(str,splitchar){
		   if (str === " "){
			  return 0;
		   } else {
			  return str.split(splitchar).length;
		   }
		}
	}
});

