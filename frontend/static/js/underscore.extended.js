// underscore extensions
_.mixin({
    union : function(array) {
      return array.concat.apply(array,_.tail(arguments));
    },
    isNotEqual : function() {
        return !_.isEqual.apply(this,arguments);
    },
    isNotEmpty : function() {
        return !_.isEmpty.apply(this,arguments);
    },
    allAre : function(array, testFn) {
        var istype = true;
        _.each(array, function(item) {
            if (!testFn(item)) {
                istype = false;
                _.breakLoop();
            }
        });
        return istype;
    },
    areArray : function() {
        return _.allAre(arguments,_.isArray);
    },
    areObject : function() {
        return _.allAre(arguments,function(o) {return typeof o === 'object'});
    },
    difference : function(obj1, obj2) {
        // Return an obj with values different in obj2 vs obj1
        var obj_difference = function(a,b,tail) {
            var keys = _.uniq(_.union(_.keys(a),_.keys(b)));
            _.each(keys, function(key) {
                subA = a[key];
                subB = b[key];
                tail[key] = _.difference(subA,subB);
            });
            return tail;
        }
        var array_difference = function(a,b,tail) {
            // TODO: this should be a recursive function to handle objs
            return _.reject(b,function(d){return _.include(_.intersect(a,b),d);});
        }
        var other_difference = function(a,b) {
            if (_.isEqual(a,b)) {
                return null;
            } else {
                return b;
            }
        }
        var diffObj = {};
        var cleaned = {};
        if (_.areArray(obj1,obj2)) {
            diffObj = array_difference(obj1,obj2,[]);
            cleaned = diffObj;
        } else if (!_.areObject(obj1,obj2)) {
            return other_difference(obj1,obj2);
        } else {
            diffObj = obj_difference(obj1,obj2,{});
            _.each(diffObj, function(v,k) {
                if(_.isNotEmpty(v)) {
                    cleaned[k] = v;
                }
            });
        }
        if(_.isEmpty(cleaned)) {
            return null;
        } else {
            return cleaned;
        }
    }
});
