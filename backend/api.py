#!/usr/bin/env python

import tornado.web
import os
import hashlib
import pymongo as pm
import json
import pymongo.json_util
import gridfs as gfs
import cPickle as pickle
from common.utils import IsFile, listdir, is_string_like, ListUnion
import common.timedate as td


EXPOSED_ACTIONS = ['find','find_one','group','skip','limit','sort','count','distinct']

class GetHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello, get")

class FindHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello, find")


def get(collectionName,querySequence,timeQuery=None, returnMetadata=False,fh = None,returnObj = True,processor = None):
	
	collection = Collection(collectionName)
	vars = collection.VARIABLES
	ColumnGroups = collection.ColumnGroups

	
	if timeQuery and collection.DateFormat:

		DateFormat = collection.DateFormat
		Q = td.generateQueries(DateFormat,timeQuery)
		TimeColNames = ColumnGroups['TimeColNames'] if 'TimeColNames' in ColumnGroups.keys() else []
		TimeColumns = ColumnGroups['TimeColumns'] if 'TimeColumns' in ColumnGroups.keys() else []

		if Q == None:					
			if TimeColumns:
				querySequence = []
				results = []
				metadata = None
			elif TimeColNames:
				TimeColNamesToReturn = []
		else:
			if TimeColNames:
				timeFormatter = td.mongotimeformatter(DateFormat)
				TimeColSONs = [timeFormatter(a) for a in TimeColNames]
				TimeColNamesToReturn = [a for (a,b) in zip(TimeColNames,TimeColSONs) if actQueries(Q,b)]
				if len(TimeColNamesToReturn) == len(TimeColNames):
					TimeColNamesToReturn = 'ALL'
			else:
				TimeColNamesToReturn = 'ALL'

		if querySequence:
			for (i,(action,args)) in enumerate(querySequence):
				if action in ['find','find_one']:
					if args:
						(posargs,kwargs) = getArgs(args)
					else:
						posargs = () ; kwargs = {}

					if TimeColNamesToReturn != 'ALL':
						retainCols = set(vars).difference(set(ColumnGroups['TimeColNames']).difference(TimeColNamesToReturn))
						if 'fields' in kwargs:
							kwargs['fields'] = list(retainCols.intersection(kwargs['fields']))
						else:
							kwargs['fields'] = list(retainCols)	
						if posargs:
							posargs[0][tuple(TimeColNamesToReturn)] = {'$exists':True}
						else:
							posargs = ({tuple(TimeColNamesToReturn) : {'$exists':True}},)
					
						
					if TimeColumns:
						for p in Q.keys():
							for t in TimeColumns:
								posargs[0][t + '.' + '.'.join(p)] = Q[p]
							
					querySequence[i] = (action,[posargs,kwargs])

	if querySequence:
	
		[Actions, Args] = zip(*querySequence)
		
		posArgs = []
		kwArgs = []
		for (action,args) in querySequence:
			if args:
				if action not in EXPOSED_ACTIONS:
					raise ValueError, 'Action type ' + str(action) + ' not recognized or exposed.'					
				(posargs,kwargs) = getArgs(args)	
				posargs = tuple([processArg(arg,collection) for arg in posargs])
				kwargs = dict([(argname,processArg(arg,collection)) for (argname,arg) in kwargs.items()])
			else:
				posargs = ()
				kwargs = {}
			posArgs.append(posargs)
			kwArgs.append(kwargs)
		
		R = collection	
		for (a,p,k) in zip(Actions,posArgs,kwArgs):
			R = getattr(R,a)(*p,**k)	
		
		sci,subcols = getsci(collection)
	
		if fh:
			fh.write('{"data":')
		if returnObj:
			Obj = {}
	
		if isinstance(R,pm.cursor.Cursor):
			if returnObj:
				Obj['data'] = []
			if fh:
				fh.write('[')
				
			for r in R:
				if processor:
					r = processor(r,collection)
					
				if fh:
					fh.write(json.dumps(r,default=pm.json_util.default) + ',')
				if returnObj:		
					Obj['data'].append(r)
					
				if sci and sci in r.keys():
					subcols.append((r['_id'],r[sci]))
					
			if fh:
				fh.write(']')
				
		else:
			if fh:
				fh.write(json.dumps(R,default=pm.json_util.default))
			if returnObj:
				Obj['data'] = R
					
					
		if returnMetadata:
			metadata = makemetadata(collection,sci,subcols)
			if fh:
				fh.write(',"metadata":' + json.dumps(metadata,default=pm.json_util.default))	
			if returnObj:
				Obj['metadata'] = metadata
			
		if fh:
			fh.write('}')									
		if returnObj:
			return Obj

def getsci(collection):
	if 'Subcollections' in collection.VARIABLES:
		sci = str(collection.VARIABLES.index('Subcollections'))
	else:
		sci = None
	subcols = []	
	
	return 	sci,subcols


def makemetadata(collection,sci,subcols):
	metadataInd = {'':('All',collection.meta[''])}
	metalist = {}
	if sci:
		for (ID,scs) in subcols:			
			for sc in scs:
				if sc in metalist.keys():
					metalist[sc].append(ID)
				else:
					metalist[sc] = ID
		for k in collection.subcollection_names():
			if k in metalist.keys():
				if len(metalist[k]) == len(subcols):
					metadataInd[k] = 'All'
				else:
					metadataInd[k] = metalist[k]
	metadata = dict([(k,(metadataInd[k],collection.meta[k])) for k in metadataInd.keys()])
	return metadata

def actQueries(Q,O):
	for p in Q:
		q = Q[p]
		o = td.rgetattr(O,p)
		if o != None:
			if not (hasattr(q,'keys') and any([a.startswith('$') for a in q.keys()])):
				if q != o:
					return False
			elif hasattr(q,'keys') and all([a.startswith('$') for a in q.keys()]):
				if not all([actionAct(a,q[a],o) for a in q.keys()]):
					return False
		else:
			return False
			
	return True
		
def actionAct(a,v,o):
	if a == '$lt':
		return o < v
	elif a == '$gte':
		return o >= v
	elif a == '$exists':
		return True
		

def processArg(arg,collection):
	V = collection.VARIABLES
	C = collection.ColumnGroups
	if is_string_like(arg):
		argsplit = arg.split('.')
		if argsplit[0] in V:
			argsplit[0] = str(V.index(argsplit[0]))
			return '.'.join(argsplit)
		elif arg in C.keys():
			return [str(V.index(c)) for c in C[arg]]
		else:
			return arg
	elif isinstance(arg, list):
		T = [processArg(d,collection) for d in arg]
		Tr = []
		for t in T:
			if isinstance(t,str):
				Tr.append(t)
			else:
				Tr += t
		return Tr
	elif isinstance(arg,tuple):
		return tuple(processArg(list(arg),collection))
	elif isinstance(arg,dict):
		T = [(processArg(k,collection), v) for (k,v) in arg.items()]
		S = dict([(k,v) for (k,v) in T if not (isinstance(k,list) or isinstance(k,tuple))])
		CodeStrings = []
		for (k,v) in T:
			if isinstance(k,list) or isinstance(k,tuple):
				if not isinstance(v,dict) or not any([key.startswith('$') for key in v.keys()]):
					orgroup = '( ' + ' || '.join(['this["' + str(kk) + '"] ' + js_translator('$e',v) for kk in k]) + ' )' 
					CodeStrings.append(orgroup)
				else:
					assert all([key in ['$exists','$gt','gte','$lt','$lte','$ne'] for key in v.keys()]), 'Cannot handle this query.'
					for key in v.keys():
						orgroup =  '( ' + ' || '.join(['this["' + str(kk) + '"] ' + js_translator(key,v[key])  for kk in k]) + ' )' 
						CodeStrings.append(orgroup)
		if CodeStrings:
			codeString = 'function(){ return '  + ' && '.join(CodeStrings) + ';}'
			S['$where'] = pm.code.Code(codeString)
		return S
	else:
		return arg
		
def js_translator(key,value):
	if key == '$e':
		return ' === "' + str(value) + '"'
	elif key == '$exists':
		return ('!==' if value else '===') + ' undefined'
	elif key == '$ne':
		return ' !== "' + str(value) + '"'
	elif key == '$gt':
		return ' > "' + str(value) + '"'
	elif key == '$lt':
		return ' < "' + str(value) + '"'	
	elif key == '$gte':
		return ' >= "' + str(value) + '"'
	elif key == '$lte':
		return ' <= "' + str(value) + '"'		
		
class Collection(pm.collection.Collection):
	
	def __init__(self,name,connection = None):
		if connection == None:
			connection = pm.Connection()
		assert 'govdata' in connection.database_names(), 'govdata collection not found.'
		db = connection['govdata']
		assert name in db.collection_names(), 'collection ' + name + ' not found in govdata database.'
		pm.collection.Collection.__init__(self,db,name)
		metaname = '__' + name + '__'
		assert metaname in db.collection_names(), 'No metadata collection associated with ' + name + ' found.'
		self.metaCollection = db[metaname]		
		self.meta = dict([(l['_id'],l) for l in self.metaCollection.find()])
		
	def subcollection_names(self):
		return self.meta.keys()
		
	def __getattr__(self,name):
		try:
			V = self.meta[''][name]
		except KeyError:
			raise AttributeError, "Can't find attribute " + name
		else:
			return V
		
def getArgs(args):
	if isinstance(args,list):
		assert len(args) == 2
		posargs = args[0]
		kwargs = args[1]
	elif isinstance(args,dict):
		posargs = ()
		kwargs = args
	elif isinstance(args,tuple):
		posargs = args
		kwargs = {}
	else:
		raise ValueError, 'querySequence'	
	
	return (posargs,kwargs)
	
	
#=-=-=-=-=-=-=-=-=-=-=-=-=-
import itertools
def expand(r):
	L = [k for k in r.keys() if isinstance(r[k],list)]
	NL = [k for k in r.keys() if is_string_like(r[k])]
	I = itertools.product(*tuple([r[k] for k in L]))
	return [dict([(k,r[k]) for k in NL] + zip(L,x)) for x in I]
	
def getquerylist(collectionName,keys):
	collection = Collection(collectionName)
	R = get(collectionName,[('find',{'fields':keys})])['data']
	colnames = [k for k in keys if k in collection.VARIABLES]
	colgroups = [k for k in keys if k in collection.ColumnGroups]
	T= ListUnion([collection.ColumnGroups[k] for k in colgroups])
	R = [dict([(collection.VARIABLES[int(i)],r[i]) for i in r.keys() if i.isdigit() and r[i]]) for r in R]
	R = [dict(  [(k,v) for (k,v) in r.items() if k not in T] + [(g,[r[k] for k in collection.ColumnGroups[g] if k in r.keys() and r[k]]) for g in colgroups ]  ) for r in R]
	return ListUnion([expand(r) for r in R])



 
	