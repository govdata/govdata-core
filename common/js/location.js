var _ = require("underscore")._

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