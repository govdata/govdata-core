(function(){function k(b,a){for(var d=[];a>0;d[--a]=b);return d.join("")}function h(b){if(b)return e.escapeRegExp(b);return"\\s"}var i=String.prototype.trim,e;e=this._s={capitalize:function(b){return b.charAt(0).toUpperCase()+b.substring(1).toLowerCase()},join:function(b){b=String(b);for(var a="",d=1;d<arguments.length;d+=1){a+=String(arguments[d]);if(d!==arguments.length-1)a+=b}return a},escapeRegExp:function(b){return b.replace(/([-.*+?^${}()|[\]\/\\])/g,"\\$1")},reverse:function(b){return Array.prototype.reverse.apply(b.split("")).join("")},
contains:function(b,a){return b.indexOf(a)!==-1},clean:function(b){return e.strip(b.replace(/\s+/g," "))},trim:function(b,a){if(!a&&i)return i.call(b);a=h(a);return b.replace(new RegExp("^["+a+"]+|["+a+"]+$","g"),"")},ltrim:function(b,a){a=h(a);return b.replace(new RegExp("^["+a+"]+","g"),"")},rtrim:function(b,a){a=h(a);return b.replace(new RegExp("["+a+"]+$","g"),"")},startsWith:function(b,a){return b.length>=a.length&&b.substring(0,a.length)===a},endsWith:function(b,a){return b.length>=a.length&&
b.substring(b.length-a.length)===a},sprintf:function(){for(var b=0,a,d=arguments[b++],g=[],c,f,j;d;){if(c=/^[^\x25]+/.exec(d))g.push(c[0]);else if(c=/^\x25{2}/.exec(d))g.push("%");else if(c=/^\x25(?:(\d+)\$)?(\+)?(0|'[^$])?(-)?(\d+)?(?:\.(\d+))?([b-fosuxX])/.exec(d)){if((a=arguments[c[1]||b++])==null||a==undefined)throw"Too few arguments.";if(/[^s]/.test(c[7])&&typeof a!="number")throw"Expecting number but found "+typeof a;switch(c[7]){case "b":a=a.toString(2);break;case "c":a=String.fromCharCode(a);
break;case "d":a=parseInt(a);break;case "e":a=c[6]?a.toExponential(c[6]):a.toExponential();break;case "f":a=c[6]?parseFloat(a).toFixed(c[6]):parseFloat(a);break;case "o":a=a.toString(8);break;case "s":a=(a=String(a))&&c[6]?a.substring(0,c[6]):a;break;case "u":a=Math.abs(a);break;case "x":a=a.toString(16);break;case "X":a=a.toString(16).toUpperCase();break}a=/[def]/.test(c[7])&&c[2]&&a>=0?"+"+a:a;f=c[3]?c[3]=="0"?"0":c[3].charAt(1):" ";j=c[5]-String(a).length-0;f=c[5]?k(f,j):"";g.push(""+(c[4]?a+f:
f+a))}else throw"Huh ?!";d=d.substring(c[0].length)}return g.join("")}};this._s.strip=e.trim;this._s.lstrip=e.ltrim;this._s.rstrip=e.rtrim;this._&&this._.mixin(this._s)})();