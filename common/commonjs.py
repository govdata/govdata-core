import os
import re
import json

import pymongo.json_util as ju
import PyV8


REQUIRE_EXP = re.compile('require[\s]*\([\s]*"([a-zA-Z0-9/._-]*)"[\s]*\)')

COMMONJS_LOCATION = '../../common/js/common.js'

def js_call(context,key,value):
    name = context.instructions[key]['name']
    return context.eval(name + '(' + json.dumps(value,default=ju.default) + ')')

class pyV8CommonJS(PyV8.JSContext):

    def __init__(self,*args,**kwargs):
    
        PyV8.JSContext.__init__(self,*args,**kwargs)
        
        self.commonJS_location = COMMONJS_LOCATION
        
        self.mod_texts = {}

        self.enter()
        self.eval(open(self.commonJS_location).read())
    
            
    def add_texts(self,module_path=None,module_text = None):
        
        if module_text or (module_path not in self.mod_texts.keys()):   
            text = module_text if module_text else open(module_path).read()
            requires = REQUIRE_EXP.findall(text)
            for requirepath in requires:
                if requirepath not in self.mod_texts.keys():
                    self.add_texts(module_path = requirepath)
            if module_path:
                self.mod_texts[module_path] = text
                self.eval('require._mod_texts["' + module_path + '"] = ' + json.dumps(text))
            
            
    def load(self,code=None,module=None):
    
        if not code:
            code = 'require("' + module + '")'
        
        self.add_texts(module_text=code)
        self.eval(code)
        

            
class translatorContext(pyV8CommonJS):

    def __init__(self,instructions,*args,**kwargs):
        
        pyV8CommonJS.__init__(self,*args,**kwargs)
        
        self.instructions = instructions
        for instruction in instructions.values():
            self.add_instruction(instruction)

                        
    def add_instruction(self,instruction):
    
        name = instruction['name']
        body = instruction['body']
        
        code = 'var ' + name + ' = function(value){' + body + '};'
        
        self.load(code=code)
