var C = {};


(function($) {
    
    C.api_url = "http://ec2-67-202-31-123.compute-1.amazonaws.com";
    
    C.load = function(d){
        return eval("("+d+")"); }
    
    C._cache = {};
    C.fetchWithCache = function(path, callback, modifierFn) {
        var cachedresult = C._cache[path];
        cachedresult = undefined;
        if(cachedresult === undefined) {
            $.get(path, function(data) {
                if(modifierFn === undefined) {
                    C._cache[path] = data;
                } else {
                    C._cache[path] = modifierFn(data);
                }
                callback(C._cache[path]);
            });
        } else {
            callback(cachedresult);
        }
    };
    
    C.get_template = function(name, callback) {
        C.fetchWithCache("/jstemplates/"+name+".erb?version="+_.uniqueId(), function(template) {
            callback(template);
        }, _.template );
    }
    
    C.apply_template = function(name, model, callback) {
        C.get_template(name, function(template) {
            callback(template(model));
        });
    }
    
    C.println = function(d) {
        console.log(d);
    }
        
})(jQuery);
