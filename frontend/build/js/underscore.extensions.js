define(["underscore"],function(){_.mixin({provide:function(a){a=a.split(".");var b=root;_.each(a,function(c){b=b[c]=b[c]||{}})}})});