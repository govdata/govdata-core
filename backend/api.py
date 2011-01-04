#!/usr/bin/env python

import os
import json
import ast
import functools
import time
import string 

import tornado.web
import tornado.httpclient
import pymongo as pm
import pymongo.json_util

import common.timedate as td
import common.location as loc
import common.solr as solr

from common.utils import IsFile, listdir, is_string_like, ListUnion, Flatten, is_num_like, uniqify
from common.acursor import asyncCursorHandler
from common.mongo import processArg, Collection, SPECIAL_KEYS


#=-=-=-=-=-=-=-=-=-=-=-=-=-
#GET
#=-=-=-=-=-=-=-=-=-=-=-=-=    


EXPOSED_ACTIONS = ['find','find_one','group','skip','limit','sort','count','distinct']

class getHandler(asyncCursorHandler):
    @tornado.web.asynchronous
    def get(self):
        
        self.TIMER = time.time()
        args = self.request.arguments
        for k in args.keys():
            args[k] = args[k][0]
        
        # pre take off any paramaters besides q
        self.jsonPcallback = args.pop('callback',None)

        q = json.loads(args['q'])
            
        self.get_response(q)


    @tornado.web.asynchronous
    def post(self):

        args = json.loads(self.request.body)
        self.get_response(args)
        
        
    def get_response(self,args):
        
        args = dict([(str(x),y) for (x,y) in args.items()])

        collectionName = args.pop('collection')
        querySequence = args.pop('query')        
        
        if isinstance(querySequence,dict):
            querySequence = [querySequence]
        for (i,x) in enumerate(querySequence):
            querySequence[i] = (x.get('action'),[x.get('args',()),x.get('kargs',{})])
            
        self.returnObj = args.pop('returnObj',False)   
        self.stream = args.pop('stream',True)
        
        self.returnMetadata = args.pop('returnMetadata',False)   
                
        self.processor = functools.partial(gov_processor,args.pop('processor',None))
       
        passed_args = dict([(key,args.get(key,None)) for key in ['timeQuery','spaceQuery','versionNumber','returnMetadata']]) 
 
        A,collection,needsVersioning,versionNumber,uniqueIndexes,vars = get_args(collectionName,querySequence,**passed_args)
                       
        self.needsVersioning = needsVersioning
        self.versionNumber = versionNumber
        self.uniqueIndexes = uniqueIndexes
        self.VarMap = dict(zip(vars,[str(x) for x in range(len(vars))])) 
        self.sci = self.VarMap.get('subcollections')
        self.subcols = []
        self.vNInd =  self.VarMap['__versionNumber__']
        self.retInd = self.VarMap['__retained__']
        
        self.add_async_cursor(collection,A)
        

        
    def begin(self):
        if self.jsonPcallback:
            self.write(self.jsonPcallback + '(')
        if self.stream:
            self.write('{"data":')
        if self.returnObj:
            self.data = []
            
                              
    def end(self):        

        if self.returnObj:
            returnedObj = {'data':self.data}
            
        returnMetadata = self.returnMetadata

        if returnMetadata:
            collection = self.collection
            sci = self.sci
            subcols = self.subcols
            
            metadata = makemetadata(collection,sci,subcols)
            
            if self.stream:
                 self.write(',"metadata":' + json.dumps(metadata,default=pm.json_util.default))    
            if self.returnObj:
                returnedObj["metadata"] = metadata
                
        if self.stream:
            self.write('}')
            
        if self.returnObj and not self.stream:
            self.write(json.dumps(returnedObj,default=pm.json_util.default))
            
        if self.jsonPcallback:
            self.write(')')

        self.finish()
          

def gov_processor(processor,r,handler,collection):

    needsVersioning = handler.needsVersioning
    versionNumber = handler.versionNumber
    uniqueIndexes = handler.uniqueIndexes
    VarMap = handler.VarMap
    sci= handler.sci
    subcols = handler.subcols
    vNInd =  handler.vNInd
    retInd = handler.retInd

    if handler.needsVersioning: 
        rV = r[handler.vNInd]
        if rV > versionNumber:
            s = dict([(VarMap[k],r[VarMap[k]]) for k in uniqueIndexes] + [(vNInd,{'$gte':versionNumber,'$lt':rV}),(retInd,True)])
            H = collection.find(s).sort(vNInd,pm.DESCENDING)
            for h in H:
                for hh in h.keys():
                    if hh not in SPECIAL_KEYS:
                        r[hh] = h[hh]
                    if '__addedKeys__' in h.keys():
                        for g in h['__addedKeys__']:
                            r.pop(g)
         
            r[vNInd] = versionNumber
     
    if sci and sci in r.keys():
        subcols.append((r['_id'],r[sci]))

    if processor:
        r = processor(r,collection)
        
    return r

       
def get_args(collectionName,querySequence,timeQuery=None, spaceQuery = None, versionNumber=None, returnMetadata=False):
    """
    collection : String => collection name e.g. BEA_NIPA
    query : JSON dictionary or list of dictionaries represnting the mongo query.  Each dictionary has the following keys:
         "action" : string, the mongo action, e.g. "find", "find_one", "count", etc.
         "args" : list, the positional arugments to be passed to the action as in pymongo
         "kargs" : dictionary, keys/values of keyword arguments to be passed to the action as in pymongo
         When this is a single dictionary, the mongo query consists of the one action, when several, the actions are applied in order.
         E.g.
             query={"action" : "find", "args" : [{"Topic":"Employment"}]}    represents   db.find({"Topic":"Employment"})
             query=[{"action" : "find", "args" :[{"Topic":"Employment"}]},{"action":"limit", "args":[10]}]   represnts   db.find({"Topic":"Employment"}).limit(10)

    timeQuery : Dict => {"format": ?, "begin": ?, "end": ?, "on": ?} begin, end, on are dates in "fomat" format
    spaceQuery : Dict => {"s": ?, "c": ?, "f": {"s", "c"}}
               : List => ["s", "c", "f.s"]
    returnMetadata : Boolean => to return meta data
    processor : lambda => processor applied to each row (TODO: fully implement this)
    versionNumber:  integer or 'ALL' ==> specify which version of data to be returned, defaults to currentVersion
        The logic implemented by this procedure for the version querying is:   
          -- deletions relative to next version those records where __retained__ key doesn't exist
          -- totally new records (e.g. with new uniqueIndexes) between v1 and v2 are those with:  v2 >= __originalVersion__ >= v1
          -- diffs relative to corresponding record in next version: __retained__key DOES exist, and:
                    -- keys which were deleted in next version or have differing values are stored
                    -- __addedKeys__ key lists keys added in next version
          -- Therefore, data at currentVersion are simply all records with  __versionNumber__ = currentVersion
          -- The data at version V where V < currentVersion are computed by:
                -- getting all data with __originalVersion__  <= V and __retained__ NOT exists, and __versionNumber__ >= V
                -- for each record in above with __versionNumber__ = V':
                        find all correponding records with __retained__ = true and __versionNumber__ < V' and for each  one, apply diffs, eading version history in backwards order.

    """

    collection = Collection(collectionName,versionNumber=versionNumber,attachMetadata=returnMetadata)
        
    versionNumber = collection.versionNumber
    currentVersion = collection.currentVersion
    
    needsVersioning = versionNumber != 'ALL' and versionNumber != currentVersion
    vars = collection.columns
    uniqueIndexes = collection.uniqueIndexes   
    
    ColumnGroups = collection.columnGroups

    if versionNumber != 'ALL':  
        insertions = []
        for (i,(action,args)) in enumerate(querySequence):
            if args:
                (posargs,kwargs) = getArgs(args)
            else:
                posargs = () ; kwargs = {}
                
            if action in ['find','find_one']:               
                posargs = setArgTuple(posargs,'__versionNumber__',{'$gte':versionNumber})
                posargs = setArgTuple(posargs,'__retained__',{'$exists':False})
                posargs = setArgTuple(posargs,'__originalVersion__',{'$lte':versionNumber})
                querySequence[i] = (action,[posargs,kwargs])  
            elif action in ['count','distinct'] and i == 0 :
                insertions.append((i,('find',   ({'__versionNumber__':{'$gte':versionNumber},'__retained__':{'$exists':False},'__originalVersion__':{'$lte':versionNumber}},))))
            
        for (i,v) in insertions:
            querySequence.insert(i,v)

    if timeQuery:
        if hasattr(collection,'overallDate'):
            OK = td.checkQuery(timeQuery, collection.overallDate)
        
            if not OK:
                querySequence = []
                results = []
                metdata = None
    
        if hasattr(collection,'dateFormat'):
            DateFormat = collection.dateFormat
        else:
            DateFormat = ''
    
        if querySequence and timeQuery:
            tQ = td.generateQueries(DateFormat,timeQuery)
            TimeColNames = ColumnGroups['timeColNames'] if 'timeColNames' in ColumnGroups.keys() else []
            TimeColumns = ColumnGroups['timeColumns'] if 'timeColumns' in ColumnGroups.keys() else []
    
            if tQ == None:                  
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
                    TimeColNamesToReturn = [a for (a,b) in zip(TimeColNames,TimeColSONs) if actQueries(tQ,b)]
                    if len(TimeColNamesToReturn) == len(TimeColNames):
                        TimeColNamesToReturn = 'ALL'
                else:
                    TimeColNamesToReturn = 'ALL'
    else:
        tQ = None
        TimeColNamesToReturn = 'ALL'
    
    if querySequence and spaceQuery:
        if hasattr(collection,'overallLocation'):
            OK = loc.checkQuery(spaceQuery, collection.overallLocation)
        
            if not OK:
                querySequence = []
                results = []
                metdata = None
        
        if querySequence and spaceQuery:
            sQ = loc.generateQueries(spaceQuery)
            SpaceColNames = ColumnGroups['spaceColNames'] if 'spaceColNames' in ColumnGroups.keys() else []
            SpaceColumns = ColumnGroups['spaceColumns'] if 'spaceColumns' in ColumnGroups.keys() else []
    
            if SpaceColNames:                   
                SpaceColNamesToReturn = [a for a in SpaceColNames if actQueries(sQ,a)]
                if len(SpaceColNamesToReturn) == len(SpaceColNames):
                    SpaceColNamesToReturn = 'ALL'
            else:
                SpaceColNamesToReturn = 'ALL'
    else:
        sQ = None
        SpaceColNamesToReturn = 'ALL'
        
    if querySequence and (sQ or tQ):
        for (i,(action,args)) in enumerate(querySequence):
            if action in ['find','find_one']:
                if args:
                    (posargs,kwargs) = getArgs(args)
                else:
                    posargs = () ; kwargs = {}

                if TimeColNamesToReturn != 'ALL' or SpaceColNamesToReturn != 'ALL' :
                    remove = (TimeColNames if TimeColNamesToReturn != 'ALL' else []) + (SpaceColNames if SpaceColNamesToReturn != 'ALL' else [])
                    retain = (TimeColNamesToReturn if TimeColNamesToReturn != 'ALL' else []) + (SpaceColNamesToReturn if SpaceColNamesToReturn != 'ALL' else [])

                    if 'fields' in kwargs:
                        kwargs['fields'] += retain
                    else:
                        retainCols = set(vars).difference(set(remove).difference(retain))
                        kwargs['fields'] =  list(retainCols) 
            
                    posargs = setArgTuple(posargs,tuple(retain),{'$exists':True})
                                                
                if tQ and TimeColumns:
                    for p in tQ.keys():
                        for t in TimeColumns:
                            posargs = setArgTuple(posargs,t + '.' + '.'.join(p),tQ[p])
                            
                if sQ and SpaceColumns:
                    for p in sQ.keys():
                        for t in SpaceColumns:
                            posargs = setArgTuple(posargs,t + '.' + '.'.join(p),sQ[p])
                        
                querySequence[i] = (action,[posargs,kwargs])                 

    if querySequence:
    
        [Actions, Args] = zip(*querySequence)
          
        posArgs = []
        kwArgs = []
        for (action,args) in querySequence:

            if action not in EXPOSED_ACTIONS:
                raise ValueError, 'Action type ' + str(action) + ' not recognized or exposed.'                  
            (posargs,kwargs) = getArgs(args)    
            
            if needsVersioning and  'fields' in kwargs.keys() and  action in ['find','find_one']:
                kwargs['fields'] += ['__versionNumber__'] + uniqueIndexes
          
            posargs = tuple([processArg(arg,collection) for arg in posargs])
            kwargs = dict([(argname,processArg(arg,collection)) for (argname,arg) in kwargs.items()])

            posArgs.append(posargs)
            kwArgs.append(kwargs)
        
        return zip(Actions,zip(posArgs,kwArgs)),collection,needsVersioning,versionNumber,uniqueIndexes,vars


def get(*args,**kwargs):
    fh=kwargs.pop('fh',None)    
    returnObj = kwargs.pop('returnObj',True)   
    returnMetadata = kwargs.pop('returnMetadata',True)   
    processor = kwargs.pop('processor',None)

    A,collection,needsVersioning,versionNumber,uniqueIndexes,vars = get_args(*args,**kwargs)
    
    R = collection  
    for (a,(p,k)) in A:
        R = getattr(R,a)(*p,**k)    
    
    VarMap = dict(zip(vars,[str(x) for x in range(len(vars))]))  
    
    sci = VarMap.get('subcollections',None)
    subcols = []
    vNInd = VarMap['__versionNumber__']
    retInd = VarMap['__retained__']
    
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
            if needsVersioning: 
                rV = r[vNInd]
                if rV > versionNumber:
                    s = dict([(VarMap[k],r[VarMap[k]]) for k in uniqueIndexes] + [(vNInd,{'$gte':versionNumber,'$lt':rV}),(retInd,True)])
                    H = collection.find(s).sort(vNInd,pm.DESCENDING)
                    for h in H:
                        for hh in h.keys():
                            if hh not in SPECIAL_KEYS:
                                r[hh] = h[hh]
                            if '__addedKeys__' in h.keys():
                                for g in h['__addedKeys__']:
                                    r.pop(g)
                 
                    r[vNInd] = versionNumber
             
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


def setArgTuple(t,k,v):
    if t:
        t[0][k] = v
    else:
        t = ({k:v},)
    return t


def makemetadata(collection,sci,subcols):
    metadataInd = {'':'All'}
    metalist = {}
    if sci:
        for (ID,scs) in subcols:            
            for sc in scs:
                if sc in metalist.keys():
                    metalist[sc].append(ID)
                else:
                    metalist[sc] = [ID]

        for k in collection.subcollection_names():
            if k in metalist.keys():
                if len(metalist[k]) == len(subcols):
                    metadataInd[k] = 'All'
                else:
                    metadataInd[k] = metalist[k]
    metadata = dict([(k,(metadataInd[k],collection.metadata.get(k,{}))) for k in metadataInd.keys()])
    return metadata

def actQueries(Q,O):
    for p in Q:
        q = Q[p]
        o = td.rgetattr(O,p)
        if o != None:
            if q == {'$exists': False}:
                return False
            elif not (hasattr(q,'keys') and any([a.startswith('$') for a in q.keys()])):
                if q != o:
                    return False
            elif hasattr(q,'keys') and all([a.startswith('$') for a in q.keys()]):
                if not all([actionAct(a,q[a],o) for a in q.keys()]):
                    return False
        else:
            if q != {'$exists': False}:
                return False
            
    return True
        
def actionAct(a,v,o):
    if a == '$lt':
        return o < v
    elif a == '$gte':
        return o >= v
    elif a == '$exists':
        return True
        
       
def processJSValue(code,collection):
    vars = collection.columns
    varMap = dict(zip(vars,[repr(str(x)) for x in range(len(vars))])) 
    T = string.Template(code)
    return T.substitute(varMap)
    
        
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
    elif args == None:
        posargs = ()
        kwargs = {}
    else:
        raise ValueError, 'querySequence'   
    
    kwargs = dict([(str(key),val) for (key,val) in kwargs.items()])

    return (posargs,kwargs)
    
    
#=-=-=-=-=-=-=-=-=-=-=-=-=-
#TABLE
#=-=-=-=-=-=-=-=-=-=-=-=-=-


def getTable(handler):

    if handler.data:
        cols = [{'id':id,'label':label,'type':getType(handler,i,id)} for (i,(id,label)) in enumerate(handler.fields)]
    else:
        cols = [{'id':id,'label':label,'type':'string'} for (i,(id,label)) in enumerate(handler.fields)]
        handler.status = 'warning'
        handler.warnings = [{'reason':'other','message':'No results.'}]
        
    returnCols = handler.args.get("returnCols",True) 
    if returnCols:
        return {'cols':cols,'rows':handler.data}
    else:
        return {"rows":handler.data}
    

def getType(handler,i,id):

    dp = handler.data[0][i]
    if isinstance(dp,bool):
        return 'boolean'
    elif isinstance(dp,int) or isinstance(dp,float):
        return 'number'
    elif is_string_like(dp):
        return 'string'
    else:
        return 'object'

import numpy
isnan = numpy.isnan
def table_processor(handler,x,collection):
    def clean(d):
        if is_string_like(d):
            return d.replace("\n","\\\n")
        elif is_num_like(d) and isnan(d):
            return None
        else:
            return d
    return [clean(x.get(id,None)) for (id,label) in handler.fields]


class tableHandler(getHandler):

    tablemaker = getTable
    
    @tornado.web.asynchronous
    def get(self):
        args = self.request.arguments
        for k in args.keys():
            args[k] = args[k][0]
        
        # pre take off any paramaters besides q
        self.jsonPcallback = args.pop('callback',None)

        args = json.loads(args['q'])
        self.args = args
 
        querySequence = args['query']

        if isinstance(querySequence, dict):
            querySequence = [querySequence]
        actions = [q['action'] for q in querySequence]
        if set(actions) <= set(EXPOSED_ACTIONS) and 'find' == actions[0]:
            args['returnObj'] = True
            args['stream'] = False                 
            args['processor'] = functools.partial(table_processor,self)
            self.field_order = querySequence[0].get('kargs',{}).get('fields',None)
            self.get_response(args)            
        else:
            self.begin()
            self.status = 'error'
            self.errors = [{'reason':'invalid_query'}]
            self.end()


    def end(self):
        if not hasattr(self,'status'):
            self.status = 'ok'

        D = {}    

        if self.status == 'ok':
            table = self.tablemaker()
            if table:
                D['data'] = table

        D['status'] = self.status    
        if self.status == 'error':       
            D['errors'] = self.errors
        elif self.status == 'warning':
            D['warnings'] = self.warnings
            
        if self.returnMetadata:
            collection = self.collection
            sci = self.sci
            subcols = self.subcols
            
            D["metadata"] = makemetadata(collection,sci,subcols)
        
        self.write(json.dumps(D,default=pm.json_util.default))

        if self.jsonPcallback:
            self.write(')')
  
        self.finish()
        

    def organize_fields(self,fields):
        vars = self.collection.columns

        TimeColumns = self.collection.columnGroups.get('timeColumns',[])
        SpaceColumns = self.collection.columnGroups.get('spaceColumns',[])
        TimeColNames = self.collection.columnGroups.get('timeColNames',[])
        TimeColNames.sort()

        if fields == None:

            V = [i for (i,x) in enumerate(vars) if x not in SPECIAL_KEYS and x not in TimeColNames] + [vars.index(x) for x in TimeColNames]
            fields = map(str,V)

        else:
            fields = fields.keys()
            fields.sort()

            TimeColFields = [str(vars.index(x))  for x in TimeColNames if str(vars.index(x)) in fields]
            SpecialFields = [str(vars.index(x))  for x in SPECIAL_KEYS +  ['subcollections'] if str(vars.index(x)) in fields]

            fields = SpecialFields + [i for i in fields if i not in TimeColFields + SpecialFields] + TimeColFields

        if self.field_order:
            fields = uniqify(ListUnion([[self.VarMap[kk] for kk in self.collection.columnGroups.get(k,[k])] for k in self.field_order]) + [k for k in fields if k != '_id' and vars[int(k)] not in self.field_order])


        ids = ['_id'] + [field.encode('utf-8') for field in fields]
        labels = ['_id'] + [str(vars[int(field)]) for field in fields]        

        self.fields = zip(ids,labels)
    
    


    
#=-=-=-=-=-=-=-=-=-=-=-=-=-
#TIMELINE
#=-=-=-=-=-=-=-=-=-=-=-=-=-

def infertimecol(collection):
    if hasattr(collection,'dateFormat') and 'Y' in collection.dateFormat:
        if collection.columnGroups.has_key('timeColNames'):
            return '__keys__'
        elif collection.columnGroups.has_key('timeColumns') and collection.columnGroups['timeColumns']:
            return collection.columnGroups['timeColumns'][0]
            
     
def getTimelineTable(handler):

    obj = handler.data
    if not obj:
        handler.status = 'error'
        handler.errors = [{'reason':'other','message':'No results.'}]
        return

    timecol = handler.args.get('timecol',None)
    
    if timecol == None:
        timecol = infertimecol(handler.collection)

    if timecol == None:
        handler.status = 'error'
        handler.errors = [{'reason':'other','message':'TimeCol not found.'}]
        return        
     
    cols = [{'id':id,'label':label,'type':getType(handler,i,id)} for (i,(id,label)) in enumerate(handler.fields)]
    labels  = [c['label'] for c in cols]
    
    if timecol == '__keys__':
    
        timecolname = handler.args.get('timecolname','Date')
        
        cG = handler.collection.metadata['']['columnGroups']
        labelcols =  map(str,cG['labelColumns'])
        labelcols = ListUnion([cG[x]  if x in cG else [x] for x in labelcols])
    
        assert set(labelcols) <= set(labels) 
        labelcolInds = [labels.index(l) for l in labelcols]
        
        timevalNames = [name for name in labels if name in handler.collection.columnGroups['timeColNames']]
        timevalNames.sort()
                  
        timevalInds = [labels.index(x) for x in timevalNames]
        
        timevalNames = uniqify(ListUnion([[labels[i] for i in timevalInds if r[i] != None] for r in obj]))
        timevalNames.sort()
        timevalInds = [labels.index(x) for x in timevalNames]

        formatter1 = td.mongotimeformatter(handler.collection.dateFormat)
        formatter2 = td.MongoToJSDateFormatter(handler.collection.dateFormat)
        timevals = [formatter2(formatter1(x)) for x in timevalNames]
        
        othercols = [', '.join([r[l] for l in labelcolInds if r[l]]) for r in obj]
        
        cols = [{'id':'Date','label': timecolname, 'type':'date'}] + [{'id':str(j),'label': o, 'type':'number'} for  (j,o) in enumerate(othercols)]
                
        obj = [[tv] + [r[i] for r in obj]  for (i,tv) in zip(timevalInds,timevals)]
       
    
    elif timecol in handler.vars:
       
        assert timecol in labels
        timecolInd = labels.index(timecol)
        assert cols[timecolInd]['type'] == 'Date'


        if timecolInd != 0:
            cols.insert(cols.pop(timeColInd),0)            
            for r in obj:
                r.insert(r.pop(timeColInd),0)        
   
    return {'cols':cols,'rows':obj}


class timelineHandler(tableHandler):    

    tablemaker = getTimelineTable
 
        

    
#=-=-=-=-=-=-=-=-=-=-=-=-=-
#FIND
#=-=-=-=-=-=-=-=-=-=-=-=-=-

     
def create_responder(handler,**params):
    def responder(response):
        if response.error: raise tornado.web.HTTPError(500)
        wt = params.get('wt',None)
        callback = params.get('callback',[None])[0]
        if callback:
            handler.write(callback + '(')
        if wt == 'json':
            handler.write(response.body)
        elif wt == 'python':
            X = ast.literal_eval(response.body)
            #do stuff to X
            jsonstr = json.dumps(X,default=pm.json_util.default)
            handler.write(jsonstr)
        if callback:
            handler.write(')')
        handler.finish()
    return responder   
               
class findHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        args = self.request.arguments
        query = args.pop('q',[''])[0]
        if not query:
            query = '*:*';
            args['qt'] = 'standard'
 
        args['wt'] = args.get('wt','json') # set default wt = 'json'
        self.set_header("Content-Type","application/json")
        http = tornado.httpclient.AsyncHTTPClient()
        http.fetch(find(query,**args),callback=self.async_callback(create_responder(self,**args)))
            
def find(q, timeQuery = None, spaceQuery = None, hlParams=None,facetParams=None,mltParams = None, **params):
    """
        q : String => the query to solr
        timeQuery => see get
        spaceQuery => see get
        hlParams => highlighting solr
        facetParams => faceting solr
        mltParams => mlt more like this solr thing
        params: String|List => other params to solr
    """
    if 'qt' not in params.keys():
        params['qt'] = 'dismax'
    if 'sort' not in params.keys():
        params['sort'] = 'score desc, volume desc'
    if 'rows' not in params.keys():
        params['rows'] = '20'
        
    if timeQuery:
        fq = td.queryToSolr(timeQuery)
        
        if 'fq' not in params.keys():
            params['fq'] = fq
        else:
            params['fq'] = Flatten([params['fq'],fq])
    
    if params.get('fl',None) == None:
        params['fl'] = '*,score'
            
    if spaceQuery:
        fq = loc.queryToSolr(spaceQuery)
        if 'fq' not in params.keys():
            params['fq'] = fq
        else:
            params['fq'] = Flatten([params['fq'],fq])
            
    if facetParams == None and params.get('facet',None) == None and params.get('facet.field',None) == None:
        facetParams = {'field':['agency','subagency','datasetTight','dateDivisionsTight']}
        
    paramsets = [('',params),('facet',facetParams),('hl',hlParams),('mlt',mltParams)]
           
    return solr.solrURL('select',paramsets, q = q)

class mltHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        args = self.request.arguments     
        args['wt'] = args.get('wt','json') 
        http = tornado.httpclient.AsyncHTTPClient()
        http.fetch(solr.solrURL('mlt',[('',args)]),callback=self.async_callback(create_responder(self,**args)))
    
class termsHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        args = self.request.arguments
        args['wt'] = args.get('wt','json') 
        http = tornado.httpclient.AsyncHTTPClient()
        http.fetch(solr.solrURL('terms',[('',args)]),callback=self.async_callback(create_responder(self,**args)))

               
#=-=-=-=-=-=-=-=-=-=-=-=-=-
#METADATA
#=-=-=-=-=-=-=-=-=-=-=-=-=-

class sourceHandler(asyncCursorHandler):

    @tornado.web.asynchronous
    def get(self):
        args = self.request.arguments
        for k in args:
            args[k] = args[k][0]
        
        self.jsonPcallback = args.pop('callback',None)
        
        querySequence = json.loads(args.get('querySequence','[]'))

        if (not querySequence) or querySequence[0][0] not in ['find','find_one']:
                querySequence.insert(0,['find',None])
           
        querySequence = [[str(action),list(getArgs(args))] for (action,args) in querySequence]
    
        if querySequence[0][1][0] == () or not (querySequence[0][1][0][0].has_key('versionOffset') or querySequence[0][1][0][0].has_key('version')):
            querySequence[0][1][0] = setArgTuple(querySequence[0][1][0],'versionOffset',0)        
            
        self.stream = False
        self.returnObj = True
        
        connection = pm.Connection(document_class=pm.son.SON)
        db = connection['govdata']
        collection = db['____SOURCES____']

        self.add_async_cursor(collection,querySequence)


               
#=-=-=-=-=-=-=-=-=-=-=-=-=-
#SOURCE VERIFICATION
#=-=-=-=-=-=-=-=-=-=-=-=-=-

class verificationHandler(asyncCursorHandler):

    @tornado.web.asynchronous
    def get(self):
        args = self.request.arguments
        for k in args:
            args[k] = args[k][0]
        
        self.jsonPcallback = args.pop('callback',None)
        
        name = args['name']

        querySequence = [['find_one',[({'name':name},),{}]]]
           
        connection = pm.Connection(document_class=pm.son.SON)
        db = connection['govdata']
        collection = db['__COMPONENTS__']

        self.add_async_cursor(collection,querySequence)
        
        
#=-=-=-=-=-=-=-=-=-=-=-=-=-
#OAI
#=-=-=-=-=-=-=-=-=-=-=-=-=-

        
BASE_URL = 'http://govdata.org/oai'
ADMIN_EMAIL = 'govdata@lists.hmdc.harvard.edu'


import xml.etree.ElementTree as ET

DC_KEYS = ['source','description','keywords','URL']
DDI_KEYS = []

def dc_formatter(record,handler,collection):
    metadata = record['metadata']
    elt = ET.Element('oai_dc:dc',attrib={"xmlns:oai_dc":"http://www.openarchives.org/OAI/2.0/oai_dc/","xmlns:dc":"http://purl.org/dc/elements/1.1/","xmlns:xsi":"http://www.w3.org/2001/XMLSchema-instance","\
xsi:schemaLocation":"http://www.openarchives.org/OAI/2.0/oai_dc.xsd"})
    vals = {}
    title = metadata['source']['dataset']
    vals['title'] = title if is_string_like(title) else title['name']
    vals['creator'] = ', '.join([key + ': ' + (value if is_string_like(value) else value['name']) for (key,value) in record['source'].items() if key != 'dataset'])
    vals['identifier'] = record['name']
    vals['source'] = metadata.get('URL','')
    vals['subject'] = ', '.join(metadata.get('keywords',''))
    vals['description'] = metadata.get('description','')
    vals['date'] = td.convertToUTC(record['timeStamp'])
    for (k,v) in vals.items():
        if v:
            e = ET.Element('dc:' + k)
            e.text = v
            elt.insert(0,e) 
    m = ET.Element('metadata') ; m.insert(0,elt)
    h = header(record)
    r = ET.Element('record') ; r.insert(0,h); r.insert(1,m)
    
    return r


def ddi_formatter(record,handler,collection):
    """
    see: http://www.ddialliance.org/sites/default/files/dtd/DDI2-1-tree.html
    """
    
    metadata = record['metadata']
    
    Element = ET.Element
    codeBook = Element('codeBook')
    stdyDscr = add(codeBook,'stdyDscr')
    
    citation = add(stdyDscr,'citation')
    titlStmt = add(citation,'titlStmt')
    prodStmt = add(citation,'prodStmt')
    distStmt = add(citation,'distStmt')
    verStmt = add(citation,'verStmt')

    add(titlStmt,'tltl',metadata['title'])
    add(titlStmt,'id',record['name'])
    
    source = metadata['source']
    for k in source:
       attrib = {'type':k}
       if source[k].has_key('shortName'):
           attrib['abbr'] = source[k]['sourceName']       
       add(prodStmt,'producer',value = source[k]['name'],attrib=attrib)

    if metadata.has_key('contactInfo'):
        add(distStmt,'contact',metadata['contactInfo'])  
    if metadata.has_key('dateReleased'):
        add(distStmt,'distDate',metadata['dateReleased'])
   
    add(verStmt,'version',str(record['version']))
   
    stdyInfo = add(stdyDscr,'stdyInfo')
    subject = add(stdyInfo,'subject')
    keyword = add(subject,'keyword', ', '.join(metadata['keywords']))
    add(stdyInfo,'abstract',metadata['description'])
     
    sumDscr = add(stdyInfo,'sumDscr')
    add(sumDscr,'timePrd',value=metadata['beginDate'],attrib = {'event':'start'})
    add(sumDscr,'timePrd',value=metadata['endDate'],attrib = {'event':'end'})
    add(sumDscr,'geogCover',value = loc.phrase(metadata['commonLocation']))
    for v in metadata['spatialDivisions']:
        add(sumDscr,'geogUnit',v)
   
    method = add(stdyDscr,'method')
    dataColl = add(method,'dataColl')
    for k in metadata['dateDivisions']:
        add(dataColl,'frequenc',k)
   
    fileDscr = add(codeBook,'fileDscr')
    fileTxt = add(fileDscr,'fileTxt')
    fileStrc = add(fileTxt,'fileStrc')
    subcollections = record['subcollections']
    for k in subcollections:
        if k:
            scol = subcollections[k]
            recGrp = add(fileStrc,'recGrp',attrib={'id':k})
            labl = add(recGrp,'labl',value = scol['title'])
            dimensns = add(recGrp,'dimensns')
            add(dimensns,'caseQnty',str(scol['volume']))
    dimensns = add(fileTxt,'dimensns')
    add(dimensns,'caseQnty',str(metadata['volume']))
    add(dimensns,'varQnty',str(len(metadata['columns'])))
    
    dataDscr = add(codeBook,'dataDscr')
   
    columns = metadata['columns']
    columnGroups = metadata['columnGroups']
    columnDescriptions = metadata['columnDescriptions'] 
    varFormats = metadata['varFormats']
   
    for c in columnGroups:
        varGrp = add(dataDscr,'varGrp',attrib = {'name':c,'var': ', '.join(columnGroups[c])})
        if columnDescriptions.has_key(c) and columnDescriptions[c].has_key('description'):
            add(varGrp,'txt',value=columnDescriptions[c]['description'])
       
    for c in columns:
        attrib = {'name':c,'geog':repr(c in columnGroups.get('spaceColumns',[])),'temporal':repr(c in columnGroups.get('timeColumns',[])),'ID':str(columns.index(c))}
        var = add(dataDscr,'var',attrib=attrib)
        if columnDescriptions.has_key(c) and columnDescriptions[c].has_key('description'):
            add(var,'txt',value=columnDescriptions[c]['description'])
        varFormat = add(var,'varFormat',attrib = {'type':varFormats[c]})

    return codeBook

def add(to,name,value=None,attrib = None):
    elt = Element(name,attrib=attrib)
    if value:
        elt.text = value
    elt.text = value
    to.append(elt)
    return elt
    

    
def list_identifier_formatter(record,handler,collection):
    return header(record)

def header(record):
    elt = ET.Element('header')
    id = ET.Element('identifier')
    id.text = record['name'] + ':' + str(record['version'])
    elt.insert(0,id)
    date = ET.Element('datestamp')
    date.text = td.convertToUTC(record['timeStamp'])
    elt.insert(1,date)
    spec = ET.Element('setSpec')
    spec.text = SourceSpec_to_setSpec(record['source'])
    elt.insert(2,spec)
    return elt
    

OAI_FORMATS={'oai_dc':{'metadataPrefix':'oai_dc','keys':DC_KEYS,'formatter':dc_formatter,'schema':'http://www.openarchives.org/OAI/2.0/oai_dc.xsd','metadataNameSpace':'http://www.openarchives.org/OAI/2.0/oai_dc/','limit':10},'ddi':{'metadataPrefix':'ddi','keys':DDI_KEYS,'formatter':ddi_formatter,'schema':'','metadataNameSpace':'','limit':10}}

class oaiHandler(asyncCursorHandler):

    @tornado.web.asynchronous
    def get(self):
        args = self.request.arguments
 
        self.stream = False
        self.returnObj = True

        badArgs = any([len(v) > 1 for v in args.values()])            
        for k in args:      
            args[k] = args[k][0]           
        self.args = args
        if badArgs:
            self.bad_argument()
            return 

        verb = args.get('verb','')
        if not verb in ['GetRecord','Identify','ListIdentifiers','ListMetadataFormats','ListRecords','ListSets']:
            self.error('badVerb','Illegal Verb or verb argument not specified.')
            return 
        else:
            self.verb = verb
                
        if verb == 'GetRecord':
            if set(args.keys()) != set(['identifier','metadataPrefix','verb']):
                self.bad_argument()
            else:
                id = args['identifier']
                mp = args['metadataPrefix']
                self.GetRecord(id,mp)
        elif verb == 'Identify':
            if args.keys() != ['verb']:
                self.bad_argument()
            else:
                self.Identify()
        elif verb == 'ListIdentifiers':
            properArgs = set(args.keys())  <= set(['from','until','set','metadataPrefix','resumptionToken','verb'])
            baseArg = ('metadataPrefix' in args and 'resumptionToken' not in args) or set(args.keys()) == set(['resumptionToken','verb'])
            if not (properArgs and baseArg):
                self.bad_argument()
            else:
                mp = args['metadataPrefix']
                From = args.get('from','')
                Until = args.get('until','')
                Set = args.get('set')
                rt = args.get('resumptionToken')
                self.ListIdentifiers(mp,From,Until,Set,rt)
        elif verb == 'ListMetadataFormats':
            if args.keys() != ['verb']:
                self.bad_argument()
            else:
                self.ListMetadataFormats()
        elif verb == 'ListRecords':
            properArgs = set(args.keys())  <= set(['from','until','set','metadataPrefix','resumptionToken','verb'])
            baseArg = ('metadataPrefix' in args and 'resumptionToken' not in args) or set(args.keys()) == set(['resumptionToken','verb'])
            if not (properArgs and baseArg):
                self.bad_argument()
            else:
                mp = args.get('metadataPrefix',None)
                From = args.get('from','')
                Until = args.get('until','')
                Set = args.get('set')
                rt = args.get('resumptionToken')
                self.ListRecords(mp,From,Until,Set,rt)
        elif verb == 'ListSets':
            properArgs = set(args.keys())  <= set(['resumptionToken','verb'])
            if not properArgs:
                self.bad_argument()
            else:
                 rt = args.get('resumptionToken')
                 self.ListSets(rt)
                   
    def end(self):
        self.finalize()
        if not hasattr(self,'wrap_text'):  self.wrap_text = ''
        if not hasattr(self,'wrap_attrib'): self.wrap_attrib = None
        v = oai_wrap(self.data,self.args,self.verb,text = self.wrap_text,attrib=self.wrap_attrib)
        self.set_header("Content-Type", "text/xml")
        self.write(ET.tostring(v))
        self.finish()
        
    def error(self,code,text):
        self.data = []
        self.verb = 'error'
        self.wrap_text = text
        self.wrap_attrib = {'code':code}
        self.end()
        
    def bad_argument(self):
        self.error('badArgument','The request includes illegal arguments, is missing required arguments, includes a repeated argument, or values for arguments have an illegal syntax.') 
                 
    def GetRecord(self,id,mp):
        if mp not in OAI_FORMATS.keys():
            self.error('cannotDisseminateFormat','Format ' + repr(mp) + ' is not supported by the item or by the repository.')
            return 
        if id.count(':') != 1:
            self.error('badArgument','id format incorrect -- must contain exactly one ":".')
            return 
        name = id.split(':')[0].strip() ; version = int(id.split(':')[-1])    
        connection = pm.Connection(document_class=pm.son.SON)
        collection = connection['govdata']['____SOURCES____']       
        self.processor = OAI_FORMATS[mp]['formatter']
        querySequence = [('find',[({'name':name,'version':version},),{}])]
        self.add_async_cursor(collection,querySequence)
        
    def Identify(self):
        data = []
        elt = ET.Element('repositoryName'); elt.text = 'Harvard GovData'; data.append(elt)
        elt = ET.Element('baseURL'); elt.text = BASE_URL; data.append(elt)
        elt = ET.Element('protocolVersion'); elt.text = '2.0'; data.append(elt)
        elt = ET.Element('adminEmail'); elt.text = ADMIN_EMAIL; data.append(elt)
        elt = ET.Element('baseURL'); elt.text = BASE_URL; data.append(elt)
        elt = ET.Element('earliestDatstamp'); elt.text = '2000-01-01T00:00:00Z'; data.append(elt)
        elt = ET.Element('granularity'); elt.text = 'YYYY-MM-DDThh:mm:ssZ'; data.append(elt)        
        self.data = data
        self.end()
    
    def ListIdentifiers(self,mp,From,Until,Set,rt):          
        query = {}
        status = self.get_tquery(query,From,Until)
        if not status:
            return
        status = self.get_setquery(query,Set)
        if not status:
            return
        querySequence = [('find',[(query,),{'fields':['name','version','timeStamp','source']}])]
        connection = pm.Connection(document_class=pm.son.SON)
        collection = connection['govdata']['____SOURCES____']       
        self.processor = list_identifier_formatter
        self.add_async_cursor(collection,querySequence)       
          
    def ListMetadataFormats(self):
        data = []
        for k in OAI_FORMATS:
            elt = ET.Element('metadataFormat')      
            for (i,l) in enumerate(['metadataPrefix','schema','metadataNameSpace']):
                elt0 = ET.Element(l)
                elt0.text = OAI_FORMATS[k][l]
                elt.insert(i,elt0)
            data.append(elt)
        self.data = data
        self.end()
        

    def ListRecords(self,mp,From,Until,Set,rt):
        if not rt and  mp not in OAI_FORMATS.keys():
            self.error('cannotDisseminateFormat','Format ' + repr(mp) + ' is not supported by the item or by the repository.')
            return 
        connection = pm.Connection(document_class=pm.son.SON)
        collection = connection['govdata']['____SOURCES____']   
        if rt:
            count,From,Until,skip,cursor,mp = rt.split('!')
            count = int(count)
            skip = int(skip)
            self.resumptionToken = rt             
        query = {}
        status = self.get_tquery(query,From,Until)
        if not status:
            return
        status = self.get_setquery(query,Set)
        if not status:
            return

        if not rt:
            count = collection.find(query).count()
            skip = 0
            cursor = -1
            self.resumptionToken = '!'.join([str(count),From,Until,str(skip),str(cursor),mp])
        if count == 0:
            self.error('noRecordsMatch','The combination of the values of the from, until, and set arguments results in an empty list.')
        elif skip >= count:
            self.error('badArgument','The rt is bad')
        else:
            querySequence = [('find',[(query,),{}]),('skip',[(skip,),{}]),('limit',[(OAI_FORMATS[mp]['limit'],),{}])]      
            self.processor = OAI_FORMATS[mp]['formatter']
            self.add_async_cursor(collection,querySequence)                  
    
    def ListSets(self,rt):
        connection = pm.Connection(document_class=pm.son.SON)
        collection = connection['govdata']['____SOURCES____']
        Sources = collection.distinct('source')
        setSpecs = []
        subspecs = []
        for Source in Sources:
            for (i,k) in enumerate(Source):
                if hasattr(Source[k],'keys'):
                    subspec = pm.son.SON([(l,Source[l]) for l in Source.keys()[:i+1]])
                    if subspec not in subspecs:
                        setSpec = ET.Element('setSpec') ; setSpec.text = SourceSpec_to_setSpec(subspec)
                        setName = ET.Element('setName') ; setName.text = SourceSpec_to_setName(subspec)
                        e = ET.Element('set') ; e.insert(0,setSpec) ; e.insert(1,setName)
                        setSpecs.append(e)
        self.data = setSpecs
        self.end()


    def get_tquery(self,query,From,Until):
        if From or Until:
            query['timeStamp'] = {}
            if From:
                FromDate = td.convertFromUTC(From)
                if FromDate:
                    query['timeStamp']['$gte'] = FromDate
                    return True
                else:
                    self.error('badArgument','"from" timestamp in wrong format')
                    return False
            if Until:
                UntilDate = td.convertFromUTC(Until)
                if UntilDate:
                    query['timeStamp']['$lte'] = UntilDate
                    return True
                else:
                    self.error('badArgument','"until" timestamp in wrong format')
                    return False
        else:
            return True


    def get_setquery(self,query,Set):
        if Set:
            SourceSpec = setSpec_to_SourceSpec(Set)
            if SourceSpec:
                for k in SourceSpec:
                    query['source.'+ k] = SourceSpec[k]
            else:
                 self.error('badArgument','setSpec is formed incorrect')
                 return False
        return True

        
    def finalize(self):
        if self.verb == 'GetRecord':
            if len(self.data) == 0:
                self.verb = 'error'
                self.wrap_text  = 'The value of the identifier is unknown in this repository.'
                self.wrap_attrib = {'code':'idDoesNotExist'}
        elif self.verb == 'ListIdentifiers':
            if len(self.data) == 0:
                self.verb = 'error'
                self.wrap_text  = 'The combination of the values of the from, until, and set arguments results in an empty list.'
                self.wrap_attrib = {'code':'noRecordsMatch'}
            else:
                elt = ET.Element('resumptionToken',attrib={'completeListSize':str(len(self.data)),'cursor':'0' })
                self.data.append(elt)
        elif self.verb == 'ListRecords':
            count,From,Until,skip,cursor,mp = self.resumptionToken.split('!')
            cursor = str(int(cursor) + 1)
            elt = ET.Element('resumptionToken',attrib={'completeListSize':count,'cursor':cursor})
            count = int(count) ; skip = int(skip) 
            skip += len(self.data)
            if count > skip:
                elt.text =  '!'.join([str(count),From,Until,str(skip),cursor,mp])
            self.data.append(elt)

        
import time
def oai_wrap(elts,args,verb,text = '',attrib = None):
    if attrib == None:
        attrib = {}
        
    response = ET.Element('OAI-PMH',attrib={'xmlns':"http://www.openarchives.org/OAI/2.0/",'xmlns:xsi':"http://www.w3.org/2001/XMLSchema-instance", 'xsi:schemaLocation':"http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd"})
    
    responseDate = ET.Element('responseDate')
    responseDate.text = time.strftime('%Y-%m-%dT%H:%M:%SZ') 
    
    request = ET.Element('request',attrib=args)
    request.text = BASE_URL
    body = ET.Element(verb,attrib=attrib)
    if text:
        body.text = text
    for (i,e) in enumerate(elts):
        body.insert(i,e)
        
    response.insert(0,responseDate)
    response.insert(1,request)
    response.insert(2,body)
    
    return response
    

def setSpec_to_SourceSpec(spec):
    x = spec.split(':')
    if all([y.count('!') == 1 for y in x]):
        Source = {}
        for y in x:
            key,name = y.split('!')
            Source[key + '.ShortName'] = name

        return Source
     
def SourceSpec_to_setSpec(Spec):
    return ':'.join([k +'!' + Spec[k]['ShortName'] for k in Spec if hasattr(Spec[k],'keys')])

def SourceSpec_to_setName(Spec):
    return Spec[Spec.keys()[-1]]['Name']