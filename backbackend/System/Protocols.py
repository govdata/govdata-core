'''
Functions for implementing data protocols.
'''

from System.Utils import *
import sys, os, inspect, time, shutil, types, pickle, numpy, re, System.MetaData


def ApplyOperations2(outfilename,OpThing,WriteMetaData =True,importsOnTop=True):

	'''
	Function which implements the basic protocol concept.  
	Given an 'OpThing', which is a set of descriptions of operations 
	and corresponding arguments to be passed to the operations, 
	this writes out a python module with functions making the calls.
	
	ARGUMENTS:
	outfilename = pathname for the output python module, must end with .py
	OpThing = dictionary or list.  
	1) if dictionary, then:
	-- the keys are strings representing names for the concrete operations in the
		written-out python file 
	-- for each key, say 'FName' the value is a 3-tuple: (function, internal arguments,  external arguments)
		where:
	--function is the function -- the actual python function object, NOT the just the
		function name -- to be called as the body of FName
					
	--internal arguments are values for the arguments to be pased to FName
	The internal arguments can be given in one of several forms:
		--a tuple of positional arguments
		--a dictionary of keyword arguments
		--a 2-element list [t,d] where t is a tuple 
		of positional arguments and d is a
		dictionary of keyword arguments
	
	--external arguments is a dictionary where:
		the keys are names of keyword arguments for FName
		the values are the default values for the keyword arguments
						
	2) if a list then:
		each element of the list is a 4-tuple
		
		(FName, function, internal arguments, external arguments)
		
		where all these are as described just above.  
		The only difference between the list-frmat input and 
		the dictionary format input is that the list format
		takes the keys of th dictionary and makes the
		first element of the tuples in the list 
		(which are now 4-tuples, as opposed to 3-tuples in 
		the dictionary case). 
		
		The reason for the list format is that it the operations 
		are written out to the file in the order specified in the list,
		whereas with a dictionary the order is  determined by the 
		order of writing out the keys of the dictionary, which is
		often not the "natural" ordering. 
	
	WriteMetaData: boolean, which if true has the operations write 
	metadata for th operations in the outputted module.
		
	RETURNS:
		NOTHING
	'''

	if isinstance(OpThing,dict):
		OpList = [[s] + list(OpThing[s]) for s in OpThing.keys()]
	else:
		OpList = [list(x) for x in OpThing]
		
	OpList = OpListUniqify(OpList)
	ModulesToImport = uniqify([op[1].__module__ for op in OpList])
	
	oplines = []

	TagDicts = {}
	X = os.environ
	D = X['DataEnvironmentDirectory']
	ProtocolName = callermodule()[len(D):].rstrip('.py').replace('/','.').lstrip('.') + '.' + caller()
	for i in range(len(OpList)):
		OpTagDict = {}
		
		assert len(OpList[i]) in [3,4], 'stepname and func and args must be specified'
		if len(OpList[i]) == 4:
			[stepname,func,args,deflinedict] = OpList[i]
		else:
			[stepname,func,args] = OpList[i]
			deflinedict = {}
			
		if isinstance(args,list):
			assert len(args) == 2 and isinstance(args[0],tuple) and isinstance(args[1],dict)
			posargs = args[0]
			kwargs = args[1]
		elif isinstance(args,tuple):
			posargs = args
			kwargs = {}
		else:
			assert isinstance(args,dict)
			kwargs = args
			posargs = ()

		Info = inspect.getargspec(func)
		Args = Info[0]
		Defaults = Info[3]
		NumPosArgs = len(Args) - (len(Defaults) if Defaults else 0)
		Kwargs = Args[NumPosArgs:]

		assert len(posargs) == NumPosArgs or (len(posargs) > NumPosArgs and Info[1])
		assert set(kwargs.keys()) <= set(Kwargs) or (Info[2] and all([isinstance(k,str) for k in kwargs.keys()]))

		argdict = dict(list(enumerate(posargs)))
		argdict.update(kwargs)

		if 'depends_on' not in deflinedict.keys() and '__dependor__' in func.func_dict.keys():
			deflinedict['depends_on'] = func.__dependor__(argdict)
		if 'creates' not in deflinedict.keys() and '__creator__' in func.func_dict.keys():
			deflinedict['creates'] = func.__creator__(argdict)
		
		
		LiveDict = {}
		if '__objector__' in func.func_dict.keys():
			LiveDict = func.__objector__(argdict)
			assert isinstance(LiveDict,dict) and LiveDict <= argdict, '__objector__ decoration must return subdictionary of input dictionary'		
		LiveDict.update(dict([(var,obj) for (var,obj) in argdict.items() if (isinstance(obj,types.FunctionType) or isinstance(obj,types.BuiltinFunctionType) or isinstance(obj,types.ClassType)) and var not in LiveDict.keys()]))
			
		livetoimport = [func.__module__]
		if len(LiveDict) > 0:
			for (var,obj) in LiveDict.items():
				if isinstance(obj,str):
					mod = '.'.join(obj.split('.')[:-1])
				else:
					mod = obj.__module__
					name = obj.__name__
					argdict[var] = (mod + '.' + name) if mod != '__builtin__' else name
				if mod not in livetoimport and mod != '__builtin__':
					livetoimport.append(mod)
					
		internalimports = list(set(livetoimport).difference(ModulesToImport))
		if internalimports:
 			if importsOnTop:
				op_importline = ''
				ModulesToImport += internalimports
			else:	
				op_importline = '\timport ' + ','.join(internalimports)
		else:
			op_importline = ''
			

		for (k,v) in argdict.items():
			if not isinstance(v,str) or k not in LiveDict.keys():
				argdict[k] = repr(v)
				
		intvals = [k for k in argdict.keys() if isinstance(k,int)]
		intvals.sort()
		posargs = tuple([argdict[k] for k in intvals])
		kwargs = dict([(k,v) for (k,v) in argdict.items() if not isinstance(k,int)])
		
		defline = 'def ' + stepname + '(' + ','.join([key + '=' + repr(deflinedict[key]) for key in deflinedict.keys()]) + '):'

		ArgList = list(posargs) + [k + '=' + v for (k,v) in kwargs.items()]
		ArgString = '(' + ','.join(ArgList) + ')'
		callline = '\tOpReturn = ' + func.__module__ + '.' + func.__name__ + ArgString
		returndefline = '\tReturnDict = {} ; ReturnDict["OpReturn"] = OpReturn\n\tif isinstance(OpReturn,dict) and "MetaData" in OpReturn.keys():\n\t\tReturnDict["MetaData"] = OpReturn["MetaData"]\n\tReturnDict["ProtocolMetaData"] = {}'
		metadatadeflines = []
		StepTag = deflinedict['StepTag'] if 'StepTag' in deflinedict.keys() else stepname
		if 'creates' in deflinedict.keys():
			createlist = MakeT(deflinedict['creates'])
			for j in range(len(createlist)):
				metadatadeflines += ['\tReturnDict["ProtocolMetaData"]["' + createlist[j] + '"] = "This file is an instance of the output of step ' + StepTag + ' in protocol ' + ProtocolName + '."']
		metadatalines = '\n'.join(metadatadeflines)
		returnline = '\treturn ReturnDict'
		oplines = oplines + [defline + '\n' + op_importline + '\n' + callline + '\n' + returndefline + '\n' + metadatalines + '\n' + returnline]
		
		Fullstepname = outfilename.lstrip('../').rstrip('.py').replace('/','.') + '.' + stepname
		OpTagDict[Fullstepname] = 'Protocol\ ' + ProtocolName + ',\ ' + StepTag + ':\\nApply ' + func.__name__
				
		if 'creates' in deflinedict.keys():
			createlist = MakeT(deflinedict['creates'])
			for j in range(len(createlist)):
				OpTagDict[createlist[j]] =  'Protocol\ ' + ProtocolName + ',\ ' + StepTag + '\\n Output\ ' + str(j)
							
		TagDicts[Fullstepname]  = OpTagDict

	importline = 'import ' + ','.join(uniqify(ModulesToImport))
	
	optext = importline + '\n\n\n' + '\n\n\n'.join(oplines)

	F = open(outfilename,'w')
	F.write(optext)
	F.close()
	
	AttachMetaData = System.MetaData.AttachMetaData
	
	OpFileMetaData = {}
	OpFileMetaData['description'] = 'This python module was created by applying ' + funcname() + ' on the protocol ' + ProtocolName + '.'
	AttachMetaData(OpFileMetaData,FileName = outfilename)
	
	if WriteMetaData:
		for Fullstepname in TagDicts.keys():
			OpMetaData = {}
			OpMetaData['ProtocolTags'] = TagDicts[Fullstepname]
			OpMetaData['description'] = 'This operation is an instance of protocol ' + ProtocolName + '.'
			AttachMetaData(OpMetaData,OperationName = Fullstepname)
								
def OpListUniqify(OpList):
	'''
		When the OpList in the ApplyOperations2 contains multiple functions
		that are the same, e.g. have the same contents, but perhaps not the 
		same FName,this optimizes and retains only one of each. 
	'''
	OList = numpy.array([';'.join([str(a[1]),str(a[2]),str(a[3])]) if len(a) == 4 else ';'.join([str(a[1]),str(a[2]),''])  for a in OpList])
	[D,s] = FastArrayUniqify(OList)
	si = PermInverse(s)
	R = [i for i in range(len(OList)) if D[si[i]]]
	return [OpList[r] for r in R]