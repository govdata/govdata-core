#!/usr/bin/env python

import tornado.web
import tornado.httpclient
import os
import json
import ast
import pymongo as pm
import pymongo.json_util
from common.utils import IsFile, listdir, is_string_like, ListUnion, Flatten, is_num_like, uniqify
import common.mongo as CM
import common.timedate as td
import common.location as loc
import common.solr as solr
import functools
from common.acursor import asyncCursorHandler

SPECIAL_KEYS = CM.SPECIAL_KEYS


#=-=-=-=-=-=-=-=-=-=-=-=-=-
#GET
#=-=-=-=-=-=-=-=-=-=-=-=-=    


EXPOSED_ACTIONS = ['find','find_one','group','skip','limit','sort','count','distinct']

class GetHandler(asyncCursorHandler):
    @tornado.web.asynchronous
    def get(self):
        
        args = self.request.arguments
        for k in args.keys():
            args[k] = args[k][0]

        args['query'] = json.loads(args['query']) 

        if 'timeQuery' in args.keys():
            args['timeQuery'] = json.loads(args['timeQuery'])
        if 'spaceQuery' in args.keys():
            args['spaceQuery'] = json.loads(args['spaceQuery'])
            
        self.get_response(args)


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
        
        self.jsonPcallback = args.pop('callback',None)
        
        self.processor = functools.partial(gov_processor,args.pop('processor',None))
       
        passed_args = dict([(key,args.get(key,None)) for key in ['timeQuery','spaceQuery','versionNumber']]) 
 
        A,collection,needsVersioning,versionNumber,uniqueIndexes,vars = get_args(collectionName,querySequence,**passed_args)

               
        self.needsVersioning = needsVersioning
        self.versionNumber = versionNumber
        self.uniqueIndexes = uniqueIndexes
        self.VarMap = dict(zip(vars,[str(x) for x in range(len(vars))])) 
        self.sci = self.VarMap.get('Subcollections')
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

       
def get_args(collectionName,querySequence,timeQuery=None, spaceQuery = None, versionNumber=None):
    """
    collectionName : String => collection name e.g. BEA_NIPA
    querySequence : List[Pair[action,args]] => mongo db action read pymongo docs e.g. 
        case args switch {
            tuple => (pymongoargs,) args is a positional args to be sent to action e.g. single element tuple
            dict => args is the dictionary of keyword arguments
            two element list => [tuple,dict] first position element Tuple and second is keyword dictionary 
        }
        e.g.
            tuple -> [("find",({"Topic":"Employment"},))]
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
    if versionNumber != 'ALL':
        collection = CM.Collection(collectionName,versionNumber=versionNumber)
    else:
        collection =  CM.Collection(collectionName)
   
    versionNumber = collection.versionNumber
    currentVersion = collection.currentVersion
    
    needsVersioning = versionNumber != 'ALL' and versionNumber != currentVersion
    vars = collection.totalVariables
    uniqueIndexes = collection.UniqueIndexes   
    
    ColumnGroups = collection.ColumnGroups

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
        if hasattr(collection,'OverallDate'):
            OK = td.checkQuery(timeQuery, collection.OverallDate)
        
            if not OK:
                querySequence = []
                results = []
                metdata = None
    
        if hasattr(collection,'DateFormat'):
            DateFormat = collection.DateFormat
        else:
            DateFormat = ''
    
        if querySequence and timeQuery:
            tQ = td.generateQueries(DateFormat,timeQuery)
            TimeColNames = ColumnGroups['TimeColNames'] if 'TimeColNames' in ColumnGroups.keys() else []
            TimeColumns = ColumnGroups['TimeColumns'] if 'TimeColumns' in ColumnGroups.keys() else []
    
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
        if hasattr(collection,'OverallLocation'):
            OK = loc.checkQuery(spaceQuery, collection.OverallLocation)
        
            if not OK:
                querySequence = []
                results = []
                metdata = None
        
        if querySequence and spaceQuery:
            sQ = loc.generateQueries(spaceQuery)
            SpaceColNames = ColumnGroups['SpaceColNames'] if 'SpaceColNames' in ColumnGroups.keys() else []
            SpaceColumns = ColumnGroups['SpaceColumns'] if 'SpaceColumns' in ColumnGroups.keys() else []
    
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
            
                    #posargs = setArgTuple(posargs,tuple(retain),{'$exists':True})
                                                
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
    
    sci = VarMap.get('Subcollections',None)
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
    metadataInd = {'':('All',collection.metadata[''])}
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
        

def processArg(arg,collection):
    """Translates the arg to human readable to collections"""
    V = collection.totalVariables
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
            if is_string_like(t):
                Tr.append(t)
            else:
                Tr += t
        return Tr
    elif isinstance(arg,tuple):
        return tuple(processArg(list(arg),collection))
    elif isinstance(arg,dict):
        T = [(processArg(k,collection), v)  for (k,v) in arg.items() if k != '$where' ]
        S = dict([(k,v) for (k,v) in T if not (isinstance(k,list) or isinstance(k,tuple))])
        CodeStrings = [processJSValue(arg['$where'],collection)] if '$where' in arg.keys() else []
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

import string        
def processJSValue(code,collection):
    vars = collection.totalVariables
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
        
    return {'cols':cols,'rows':handler.data}

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

def table_processor(handler,x,collection):
    return [x.get(id,None) for (id,label) in handler.fields]


class TableHandler(GetHandler):

    tablemaker = getTable
    
    @tornado.web.asynchronous
    def get(self):
        args = self.request.arguments
        for k in args.keys():
            args[k] = args[k][0]
        self.args = args
 
        args['query'] = querySequence = json.loads(args['query'])
        args['timeQuery'] = json.loads(args.get('timeQuery','null'))
        args['spaceQuery'] = json.loads(args.get('spaceQuery','null'))

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
        
        self.write(json.dumps(D,default=pm.json_util.default) + ');')

        if self.jsonPcallback:
            self.write(')')
  
        self.finish()
        

    def organize_fields(self,fields):
        vars = self.collection.totalVariables

        TimeColumns = self.collection.ColumnGroups.get('TimeColumns',[])
        SpaceColumns = self.collection.ColumnGroups.get('SpaceColumns',[])
        TimeColNames = self.collection.ColumnGroups.get('TimeColNames',[])
        TimeColNames.sort()

        if fields == None:

            V = [i for (i,x) in enumerate(vars) if x not in SPECIAL_KEYS and x not in TimeColNames] + [vars.index(x) for x in TimeColNames]
            fields = map(str,V)

        else:
            fields = fields.keys()
            fields.sort()

            TimeColFields = [str(vars.index(x))  for x in TimeColNames if str(vars.index(x)) in fields]
            SpecialFields = [str(vars.index(x))  for x in SPECIAL_KEYS +  ['Subcollections'] if str(vars.index(x)) in fields]

            fields = SpecialFields + [i for i in fields if i not in TimeColFields + SpecialFields] + TimeColFields

        if self.field_order:
            fields = uniqify(ListUnion([[self.VarMap[kk] for kk in self.collection.ColumnGroups.get(k,[k])] for k in self.field_order]) + [k for k in fields if k != '_id' and vars[int(k)] not in self.field_order])


        ids = ['_id'] + [field.encode('utf-8') for field in fields]
        labels = ['_id'] + [str(vars[int(field)]) for field in fields]        

        self.fields = zip(ids,labels)
    
    


    
#=-=-=-=-=-=-=-=-=-=-=-=-=-
#TIMELINE
#=-=-=-=-=-=-=-=-=-=-=-=-=-

def infertimecol(collection):
    if hasattr(collection,'DateFormat') and 'Y' in collection.DateFormat:
        if collection.ColumnGroups.has_key('TimeColNames'):
            return '__keys__'
        elif collection.ColumnGroups.has_key('TimeColumns') and collection.ColumnGroups['TimeColumns']:
            return collection.ColumnGroups['TimeColumns'][0]
            
     
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
        
        labelcols =  handler.collection.metadata['']['ColumnGroups']['LabelColumns']
        assert set(labelcols) <= set(labels)
        labelcolInds = [labels.index(l) for l in labelcols]
        
        timevalNames = [name for name in labels if name in handler.collection.ColumnGroups['TimeColNames']]
        timevalNames.sort()
                  
        timevalInds = [labels.index(x) for x in timevalNames]
        
        timevalNames = uniqify(ListUnion([[labels[i] for i in timevalInds if r[i] != None] for r in obj]))
        timevalNames.sort()
        timevalInds = [labels.index(x) for x in timevalNames]

        formatter1 = td.mongotimeformatter(handler.collection.DateFormat)
        formatter2 = td.MongoToJSDateFormatter(handler.collection.DateFormat)
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


class TimelineHandler(TableHandler):    

    tablemaker = getTimelineTable
 
        


    
#=-=-=-=-=-=-=-=-=-=-=-=-=-
#FIND
#=-=-=-=-=-=-=-=-=-=-=-=-=-

               
class FindHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        args = self.request.arguments
        assert 'q' in args.keys() and len(args['q']) == 1
        query = args['q'][0]
        args.pop('q')
        args['wt'] = args.get('wt','json') # set default wt = 'json'
        http = tornado.httpclient.AsyncHTTPClient()
        http.fetch(find(query,**args),callback=self.async_callback(self.create_responder(**args)))
    
    def create_responder(self,**params):
        def responder(response):
            if response.error: raise tornado.web.HTTPError(500)
            wt = params.get('wt',None)
            callback = params.get('callback',[None])[0]
            if callback:
                self.write(callback + '(')
            if wt == 'json':
                self.write(response.body)
            elif wt == 'python':
                X = ast.literal_eval(response.body)
                #do stuff to X
                jsonstr = json.dumps(X,default=pm.json_util.default)
                self.write(jsonstr)
            if callback:
                self.write(')')
            self.finish()
        return responder
    
    @tornado.web.asynchronous
    def post(self):
        args = json.loads(self.request.body)
        args = dict([(str(x),y) for (x,y) in args.items()])
        query = args.pop('q')
        args['wt'] = args.get('wt','json') # set default wt = 'json'
        http = tornado.httpclient.AsyncHTTPClient()
        http.fetch(find(query,**args),callback=self.async_callback(self.create_responder(**args)))
        


def find(q, timeQuery = None, spaceQuery = None, hlParams=None,facetParams=None,mltParams = None, **params):
    """
        query : String => the query to solr
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
            
    if spaceQuery:
        fq = loc.queryToSolr(spaceQuery)
        if 'fq' not in params.keys():
            params['fq'] = fq
        else:
            params['fq'] = Flatten([params['fq'],fq])
            
    if facetParams == None and params.get('facet.field',None) == None:
        facetParams = {'field':['agency','subagency','dataset','dateDivisions']}
                    
    return solr.queryUrl(q,hlParams,facetParams,mltParams,**params)
                
                
#=-=-=-=-=-=-=-=-=-=-=-=-=-
#SOURCES
#=-=-=-=-=-=-=-=-=-=-=-=-=-

class SourceHandler(asyncCursorHandler):

    @tornado.web.asynchronous
    def get(self):
        args = self.request.arguments
        for k in args:
            args[k] = args[k][0]
        
        
        querySequence = json.loads(args.get('querySequence','[]'))

        if (not querySequence) or querySequence[0][0] not in ['find','find_one']:
                querySequence.insert(0,['find',None])
           
        querySequence = [[str(action),list(getArgs(args))] for (action,args) in querySequence]
    
        if querySequence[0][1][0] == () or not (querySequence[0][1][0][0].has_key('version_offset') or querySequence[0][1][0][0].has_key('version')):
            querySequence[0][1][0] = setArgTuple(querySequence[0][1][0],'version_offset',0)        
        
    
        self.stream = False
        self.returnObj = True
        
        connection = pm.Connection(document_class=pm.son.SON)
        db = connection['govdata']
        collection = db['____SOURCES____']

        self.add_async_cursor(collection,querySequence)
        
            


