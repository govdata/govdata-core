var C = {};


(function($) {
    
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
    
    C._persistent = {};
    C._persistent_name = "persistent_cookie"
    
    C.createCookie = function(name,value,days) {
        if (days) {
            var date = new Date();
            date.setTime(date.getTime()+(days*24*60*60*1000));
            var expires = "; expires="+date.toGMTString();
        }
        else var expires = "";
        document.cookie = name+"="+value+expires+"; path=/";
    }

    C.readCookie = function(name) {
        var nameEQ = name + "=";
        var ca = document.cookie.split(';');
        for(var i=0;i<ca.length;i++) {
            var c = ca[i];
            while (c.charAt(0)==' ') c = c.substring(1,c.length);
            if (c.indexOf(nameEQ) == 0) return c.substring(nameEQ.length,c.length);
        }
        return null;
    }
    
    C.loadCookie = function(defaults) {
        C._persistent = new Object();
        try {
            var cookieData = C.readCookie(C._persistent_name);
            if(cookieData!=null)
            {
                C._persistent = JSON.parse(cookieData);
            }
            else
            {
                //if there is no cookie we initialize it
                C.initCookie(defaults);
            }
        }catch (err)
        {
            C.initCookie(defaults);
        }
        
    }
    
    C.initCookie = function(defaults) {
        if (defaults === undefined) {
            defaults = new Object();
        }
        C._persistent = new Object();
        C._persistent.options = new Object();
        _.extend(C._persistent, defaults);
        C.savePersistentData();
    }
    
    C.savePersistentData = function() {
        C.createCookie(C._persistent_name, JSON.stringify(C._persistent),1000000);
    }

    C.println = function(d) {
        console.log(d);
    }

    C.option = function(name,value) {
        if (value === undefined) {
            return C._persistent.options[name];
        } else {
            C._persistent.options[name] = value;
        }
    }
        
})(jQuery);
