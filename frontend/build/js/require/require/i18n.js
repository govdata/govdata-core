(function(){function r(b,a,i,e,c,d){if(a[b]){i.push(b);if(a[b]===true||a[b]===1)e.push(c+b+"/"+d)}}function s(b,a,i,e,c){a=e+a+"/"+c;require._fileExists(b.nameToUrl(a,null))&&i.push(a)}var u=/(^.*(^|\/)nls(\/|$))([^\/]*)\/?([^\/]*)/;define({load:function(b,a,i,e){e=e||{};var c=u.exec(b),d=c[1],o=c[4],f=c[5],p=o.split("-"),g=[],t={},j,h,k="";if(c[5]){d=c[1];b=d+f}else{b=b;f=c[4];o=e.locale||(e.locale=typeof navigator==="undefined"?"root":(navigator.language||navigator.userLanguage||"root").toLowerCase());
p=o.split("-")}if(require.isBuild){g.push(b);s(a,"root",g,d,f);for(j=0;h=p[j];j++){k+=(k?"-":"")+h;s(a,k,g,d,f)}a(g);i()}else a([b],function(q){var m=[];r("root",q,m,g,d,f);for(j=0;h=p[j];j++){k+=(k?"-":"")+h;r(k,q,m,g,d,f)}a(g,function(){var n,l;for(n=m.length-1;n>-1&&(h=m[n]);n--){l=q[h];if(l===true||l===1)l=a(d+h+"/"+f);require.mixin(t,l)}i(t)})})}})})();