import os
import re
import json

import pymongo.json_util as ju
import PyV8

COMMONJS_PATH = '../../common/js/'

COMMONJS="""
/*
 * An implementation of the CommonJS Modules 1.0
 * Copyright (c) 2009 by David Flanagan
 */
 

var require = function require(id) {
     
    var filename = """ + COMMONJS_PATH + """filename + ".js"; 
    // Only load the module if it is not already cached.
    if (!require._cache.hasOwnProperty(filename)) {
        
        try {
            // get the text of the module
            var modtext = read(filename);
            // Wrap it in a function
            var f = new Function("require", "exports", modtext);
            // Prepare function arguments
            var context = {};                            // Invoke on empty obj
            var exports = require._cache[filename] = {}; // API goes here
            f.call(context, require, exports);   // Execute the module
        }
        catch(x) {
            throw new Error("Can't load module " + origid + ": " + x);
        }

    }
    return require._cache[filename];  // Return the module API from the cache

};

// Set require.dir to point to the directory from which modules should be
// loaded.  It must be an empty string or a string that ends with "/".

require._cache = {};               // So we only load modules once
"""

def js_call(context,key,value):
    name = context.instructions[key]['name']
    return context.eval(name + '(' + json.dumps(value,default=ju.default) + ')')

import os
class JSReadEnv(PyV8.JSClass):
    def read(self,name):
            os.environ['PROTECTION'] = 'OFF'
            with open(name,'r') as f:
                s = f.read()
            os.environ['PROTECTION'] = 'ON'
            return s



class pyV8CommonJS(PyV8.JSContext):

    def __init__(self):
    
        PyV8.JSContext.__init__(self,JSReadEnv())
  
        self.enter()

        self.eval(COMMONJS)
    
                        
    def load(self,code=None,module=None):
    
        if not code:
            code = 'require("' + module + '")'
        
        self.eval(code)
        
            
class translatorContext(pyV8CommonJS):

    def __init__(self,instructions,*args,**kwargs):
        
        pyV8CommonJS.__init__(self,*args,**kwargs)
        
        self.instructions = {}
        for (name,body) in instructions.items():
            self.add_instruction(name,body)

                        
    def add_instruction(self,name,body):
    
        code = 'var processor_' + name +  '= function(value){' + body + '};'
        
        self.load(code=code)

        self.instructions[name] = {'name' : 'processor_' + name, 'body':body}