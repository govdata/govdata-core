/*
 * An implementation of the CommonJS Modules 1.0
 * Copyright (c) 2009 by David Flanagan
 */
 

var require = function require(filename) {
     
    // Only load the module if it is not already cached.
    if (!require._cache.hasOwnProperty(filename)) {
        
        try {
            // get the text of the module
            var modtext = require._mod_texts[filename];
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
require._mod_texts = {};