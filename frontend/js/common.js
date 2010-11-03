/*
 * An implementation of the CommonJS Modules 1.0
 * Copyright (c) 2009 by David Flanagan
 */


var registered = {};
var require = function require(module) {
     return registered[module];
};

(function($) {
var exports = {};
var SPACE_CODE_DIVISIONS = {'A': 'Area Code', 'B': 'Metropolitan Division', 'C': 'country', 'D': 'Census Division', 'I': 'Incoporated Place', 'L': 'State Legislative District -- Upper', 'O': 'Continent', 'Q': 'School District -- Secondary', 'S': 'State Abbreviation', 'T': 'Census Tract', 'V': 'Village', 'W': 'City', 'X': 'Undefined', 'Z': 'ZCTA5', 'a': 'Address', 'b': 'Combined Statistical Area', 'c': 'County', 'd': 'Public Use Microdata Area -- 1%', 'e': 'Public Use Microdata Area -- 5%', 'f': 'FIPS', 'g': 'Congressional District', 'i': 'Island', 'j': 'County Subdivision', 'k': 'School District -- Unified', 'l': 'State Legislative District -- Lower', 'm': 'Metropolitan/Micropolitan Statistical Area', 'n': 'New England City and Town Area', 'p': 'Postal Code', 'q': 'School District -- Elementary', 'r': 'Census Region', 's': 'State', 't': 'Town', 'u': 'Urban Area', 'v': 'Voting District', 'z': 'ZCTA3'}

var phrase = function (l){
    var rendered = "";
    _.each(l, function(v,k) {
        if (k === 'f') {
            // render fips code
        } else {
            rendered += v+", ";
        }
    });
    return rendered.slice(0,-2);
 }
exports.phrase = phrase
registered.location = exports;
})(jQuery);

(function($) {
    var exports = {};
     var DEFAULT_T =  [['Y',0],['m',1],['d',1],['H',0],['M',0],['S',0],['MS',0]];

     var tObjFlatten = function(tObj){
         var S = {};
         for (i in _.keys(tObj)){
     	var l = _.keys(tObj)[i] ;

     	if (tObj[l].hasOwnProperty('')){

     	    S[l] = tObj[l][''];

     	    _.extend(S,tObjFlatten(tObj[l]))
     	}

         }
         return S

     }

     var convertToDT = function(tObj){

         var ftObj = tObjFlatten(tObj);

         var tlist = new Array();

         for (i in DEFAULT_T){
           var K = DEFAULT_T[i];
           if (_.include(_.keys(ftObj),K[0])){
           	tlist.push(ftObj[K[0]])
           } else {
          	tlist.push(K[1])
           }
         }
         if (_.include(_.keys(ftObj),'q') && !(_.include(_.keys(ftObj),'m'))){
             tlist[1] = 3*(ftObj['q']-1)+1
         }

     	var date = new Date(tlist[0],tlist[1],tlist[2],tlist[3],tlist[4],tlist[5],tlist[6]) ;

     	return  date
         }


     var month=new Array(12);
     month[0]="January";
     month[1]="February";
     month[2]="March";
     month[3]="April";
     month[4]="May";
     month[5]="June";
     month[6]="July";
     month[7]="August";
     month[8]="September";
     month[9]="October";
     month[10]="November";
     month[11]="December";

     var weekday=new Array(7);
     weekday[0]="Sunday";
     weekday[1]="Monday";
     weekday[2]="Tuesday";
     weekday[3]="Wednesday";
     weekday[4]="Thursday";
     weekday[5]="Friday";
     weekday[6]="Saturday";

     var phrase = function(tObj){
         var dateObj = convertToDT(tObj);

          var ftObj = tObjFlatten(tObj);

          var s = '';

          if ('q' in ftObj){
            s += 'Q' + ftObj['q']
          }

          if (('d' in ftObj && 'm' && ftObj && 'Y' in ftObj) || 'w' in ftObj){
              s += weekday[dateObj.getDay()] + ' '
          }

          if ('m' in ftObj){
              s += month[dateObj.getMonth()] + ' '
          }
          if ('d' in ftObj){
              s += dateObj.getDate()
          }

          if (s !== ''){
      	s += ', ' ;
          }
          if ('Y' in ftObj){
              s += dateObj.getFullYear()
          }

          return s


      }

    var stringtomongo = function(tstring,dateformat){
    	var clist = new Array();
    	clist[0] = 0
    	var llist = new Array();
    	llist[0] = 1
        for (ind=1;ind<dateformat.length;ind++){

            if (dateformat[ind] != dateformat[ind-1]){
            	clist.push(ind)
    			llist.push(0)
            }
            llist[llist.length-1] += 1

    	}

    	var result = new Object();
    	for (ind in clist){
    	    i = clist[ind]
    	    if (tstring[i] !== 'X'){
    			result[dateformat[i]] = {'' : tstring.slice(i,i + llist[ind])}
    		}
    	}

    	return result

    };

    exports.stringtomongo = stringtomongo;
    exports.tObjFlatten = tObjFlatten;
    exports.convertToDT = convertToDT;
    exports.phrase=phrase;
    registered.timedate = exports;
})(jQuery);
