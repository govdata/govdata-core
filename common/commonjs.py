import os
import re
import json

import pymongo.json_util as ju
import PyV8


def js_call(context,key,value):
    name = context.instructions[key]['name']
    return context.eval(name + '(' + json.dumps(value,default=ju.default) + ')')

class JSReadEnv(PyV8.JSClass):
	def read(self,name):
		return open(name,'r').read()


class pyV8CommonJS(PyV8.JSContext):

    def __init__(self):
    
        PyV8.JSContext.__init__(self,JSReadEnv())
        
        self.commonJS_location = COMMONJS_LOCATION
  
        self.enter()
        self.eval(open(self.commonJS_location).read())
    
                        
    def load(self,code=None,module=None):
    
        if not code:
            code = 'require("' + module + '")'
        
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
