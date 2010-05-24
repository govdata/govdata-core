#!/usr/bin/env python

import tornado.web
import tornado.httpclient
import os
import json
import ast
import pymongo as pm
import pymongo.json_util
from pymongo.code import Code
from pymongo.objectid import ObjectId
from common.utils import IsFile, listdir, is_string_like, ListUnion, Flatten, is_num_like
import common.mongo as CM
import common.acursor as AC
import common.timedate as td
import common.location as loc
import common.solr as solr
import tornado.ioloop
import socket
import functools


EXPOSED_ACTIONS = ['find','find_one','group','skip','limit','sort','count','distinct']

              
class GetHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        
        args = self.request.arguments
        for k in args.keys():
            args[k] = args[k][0]

        args['querySequence'] = json.loads(args['querySequence']) 
        
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
        collectionName = args.pop('collectionName')
        querySequence = args.pop('querySequence')        
        
        self.returnObj = args.pop('returnObj',False)   
        self.stream = args.pop('stream',True)
        
        self.returnMetadata = args.pop('returnMetadata',False)   
        self.processor = args.pop('processor',None)        
               
        A,collection,needsVersioning,versionNumber,uniqueIndexes,vars = get_args(collectionName,querySequence,**args)
      
        self.collection = collection
        self.needsVersioning = needsVersioning
        self.versionNumber = versionNumber
        self.uniqueIndexes = uniqueIndexes
        self.VarMap = dict(zip(vars,[str(x) for x in range(len(vars))])) 
        self.sci = self.VarMap.get('Subcollections')
        self.subcols = []
        self.vNInd =  self.VarMap['__versionNumber__']
        self.retInd = self.VarMap['__retained__']
                
        self.begin()


        R = collection 

        for (a,p,k) in A[:-1]:
            R = getattr(R,a)(*p,**k)   
            
        (act,arg,karg) = A[-1]
        
        if act == 'count':
        
            spec,fields,skip,limit = R._Cursor__spec,R._Cursor__fields,R._Cursor__skip,R._Cursor__limit
            
            command = {"query":spec,"fields":fields,"count":collection.name}
            
            with_limit_and_skip = karg.get('with_limit_and_skip',False)
            
            if with_limit_and_skip:
                if limit:
                    command["limit"] = limit
                if skip:
                    command["skip"] = self.__skip           
                    
            self.add_command_handler(collection,command,process_count)        

  
        elif act == 'distinct':
        
            spec = R._Cursor__spec
            
            command = {"distinct":collection.name,"key":arg[0]}
            if spec:
                command['query'] = spec
            
            self.add_command_handler(collection,command,process_distinct)
       
        elif act == 'find_one':
        
            spec = arg[0]
            fields = karg.get('fields',None)
            
            if spec is None:
                spec = SON()
            elif isinstance(spec, ObjectId):
                spec = SON({"_id": spec})
            
            callback = functools.partial(processor_handler,None)
            
            self.add_handler(collection,spec,fields,0,-1,callback)
            
        elif act == 'group':
            key,condition,initial,reduce = arg
            finalize = karg.get('finalize')
            group = {}
            if isinstance(key, basestring):
                group["$keyf"] = Code(processJSValue(key,collection))
            elif key is not None:
                group = {"key": collection._fields_list_to_dict(key)}
            group["ns"] = collection.name
            group["$reduce"] = Code(processJSValue(reduce,collection))
            group["cond"] = condition
            group["initial"] = initial
            if finalize is not None:
                group["finalize"] = Code(finalize)            
                
            command = {"group":group}

            self.add_command_handler(collection,command,process_group) 
            
        else:
            R = getattr(R,act)(*arg,**karg)
            
            if isinstance(R,pm.cursor.Cursor):
            
                if self.stream:
                    self.write('[')
                
                spec,fields,skip,limit = R._Cursor__spec,R._Cursor__fields,R._Cursor__skip,R._Cursor__limit
          
                self.organize_fields(fields)
                
                self.add_handler(collection,spec,fields,skip,limit,loop)
         
            else:    
                if self.stream:
                    self.write(json.dumps(R,default=pm.json_util.default))
                self.end()
            
   
    def begin(self):
        
        if self.stream:
            self.write('{"data":')
        if self.returnObj:
            self.returnedObj = {'data':[]}
            
                          
    def end(self):        
    
        io_loop = self.settings['io_loop']
        sock = self.socket
        io_loop.remove_handler(sock.fileno())
       
        returnMetadata = self.returnMetadata

        if returnMetadata:
            collection = self.collection
            sci = self.sci
            subcols = self.subcols
            
            metadata = makemetadata(collection,sci,subcols)
            
            if self.stream:
                 self.write(',"metadata":' + json.dumps(metadata,default=pm.json_util.default))    
            if self.returnObj:
                self.returnedObj["metadata"] = metadata
                
        if self.stream:
            self.write('}')  
            
        if self.returnObj and not self.stream:
            self.write(json.dumps(self.returnedObj,default=pm.json_util.default))

        self.finish()
          

    def add_handler(self,collection,spec,fields,skip,limit,callback,_must_use_master=False,_is_command=False):
    
        if fields is not None:
            if not fields:
                fields = ["_id"]
            fields = collection._fields_list_to_dict(fields)
    
        slave_okay = collection.database.connection.slave_okay
        timeout = True
        tailable = False
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        H = sock.connect(("localhost", 27017))
        sock.setblocking(0)
        self.socket = sock
           
        cursor = AC.Cursor(collection,spec,fields,skip,limit, slave_okay, timeout,tailable,_sock=sock,_must_use_master=_must_use_master, _is_command=_is_command)
        callback = functools.partial(callback,self,cursor)
        io_loop = self.settings['io_loop']
        io_loop.add_handler(sock.fileno(), callback, io_loop.READ)  
        
        cursor._refresh()
    
    
    def add_command_handler(self,collection,command,processor):
    
        cmdCollection = collection.database['$cmd']
        
        callback = functools.partial(processor_handler,processor)
           
        self.add_handler(cmdCollection,command,None,0,-1,callback,_must_use_master=True,_is_command= True)
        
        

    def organize_fields(self,fields,vars):
        pass
              
def wire_query_processor(tqx):

    return dict([(v.split(':')[0],':'.join(v.split(':')[1:])) for v in list(csv.reader([tqx],delimiter=';',escapechar='\\'))[0]])

    
def getTableAndSignature(handler):

    cols = [{'id':id,'label':label,'type':getType(handler,i,id)} for (i,(id,label,processor)) in enumerate(handler.fields)]

    signature = str(''.join([r['c'][0]['v'] for r in handler.returnedObj['data']]).__hash__())
   
    return {'cols':cols,'rows':handler.returnedObj['data']},signature
    

def getType(handler,i,id):
    if id in handler.field_types.keys():
        return handler.field_types[id]
    else:
        dp = handler.returnedObj['data'][0]['c'][i]['v']
        if isinstance(dp,bool):
            return 'boolean'
        elif isinstance(dp,int) or isinstance(dp,float):
            return 'number'
        else:
            return 'string'


def wire_processor(handler,x,collection):
    return {'c':[{'v': processor(x.get(id,None))} for (id,label,processor) in handler.fields]}

import csv
class TableHandler(GetHandler):
    @tornado.web.asynchronous
    def get(self):
        args = self.request.arguments
        for k in args.keys():
            args[k] = args[k][0]
            
            
        tqx = wire_query_processor(args.get('tqx',''))
        
        self.responseHandler = tqx.get('responseHandler','google.visualization.Query.setResponse')
                  
        if 'sig' in tqx:
            
            self.status = 'warning'
            self.warnings = [{'reason':'not_modified'}]
            self.sig = tqx['sig']
            self.reqId = tqx['reqId']
            
            self.end()
                
        else:
            out = tqx.get('out','json')
            if out != 'json':
                self.status = 'error'
                self.errors = [{'reason':'not_supported','message':'Only Json format is supported, not ' + out + '.'}]
                self.end()
                
            else:
                print args['query']
                
                self.reqId = tqx.get('reqId',str(args['query'].__hash__()))
            
                query = json.loads(args['query'])
                
                query['timeQuery'] = json.loads(query.get('timeQuery','null'))
                query['spaceQuery'] = json.loads(query.get('spaceQuery','null'))
    
                print query['querySequence']
                query['querySequence'] = querySequence = json.loads(query['querySequence']) 
                
                actions = zip(*querySequence)[0]
                
                if set(actions) <= set(EXPOSED_ACTIONS):
            
                    query['returnObj'] = True
                    query['stream'] = False                 
                    query['processor'] = functools.partial(wire_processor,self)
                    
                    self.get_response(query)
                    
                else:
                
                    self.status = 'error'
                    self.errors = [{'reason':'invalid_query'}]
                    
                    self.end()


    def organize_fields(self,fields):
        vars = self.collection.totalVariables
        
        if fields == None:
            fields = self.VarMap.values()
        else:
            fields = fields.keys()

        ids = ['_id'] + [field.encode('utf-8') for field in fields]
        labels = ['_id'] + [str(vars[int(field)]) for field in fields]
        
        TimeColumns = self.collection.ColumnGroups.get('TimeColumns',[])
        SpaceColums = self.collection.ColumnGroups.get('SpaceColumns',[])
        
        self.field_types = {}
        
        if hasattr(self.collection,'DateFormat'):
            DateFormat = self.collection.DateFormat
            timeformatter = td.MongoToJSDateFormatter(DateFormat)
            if not timeformatter:
                timeformatter = td.reverse(DateFormat)
            else:
                for t in TimeColumns:
                    self.field_types[t] = 'Date'
            spaceformatter = lambda  x : loc.phrase2(x).encode('utf-8')
        
        processors = []
        for label in labels:
            if label in TimeColumns:
                processors.append(timeformatter)
            elif label in SpaceColums:
                processors.append(spaceformatter)
            elif label == '_id':
                processors.append(str)
            else:
                processors.append(lambda x : x.encode('utf-8') if is_string_like(x) else x)
                
                
        self.fields = zip(ids,labels,processors)


    def post(self):
        raise BaseException, 'post not handled by wire protocol'
                           
        
    def end(self):
    
        io_loop = self.settings['io_loop']
        sock = self.socket
        io_loop.remove_handler(sock.fileno())
          
        if not hasattr(self,'status'):
            self.status = 'ok'
            
        D = {}    
        D['status'] = self.status
        if self.status == 'ok':
            D['table'],self.sig = getTableAndSignature(self)

        elif self.status == 'error':       
            D['errors'] = self.errors
        elif self.status == 'warning':
            D['warnings'] == self.warnings
         
        if hasattr(self,'reqId'):
            D['reqId'] = self.reqId
        if hasattr(self,'sig'):
            D['sig'] = self.sig
            
        D['version'] = '0.6'
       
        self.write(self.responseHandler + '(' + GoogleJson(D) + ');')
        
        #self.write("google.visualization.Query.setResponse({table:{rows:[{c:[{v:'CN'},{v:1.32297E9,f:'1322970000'},{v:137.0,f:'137'}]},{c:[{v:'IN'},{v:1.13013E9,f:'1130130000'},{v:336.0,f:'336'}]},{c:[{v:'US'},{v:3.03605941E8,f:'303605941'},{v:31.0,f:'31'}]},{c:[{v:'ID'},{v:2.31627E8,f:'231627000'},{v:117.0,f:'117'}]},{c:[{v:'BR'},{v:1.86315468E8,f:'186315468'},{v:22.0,f:'22'}]},{c:[{v:'PK'},{v:1.626525E8,f:'162652500'},{v:198.0,f:'198'}]},{c:[{v:'BD'},{v:1.58665E8,f:'158665000'},{v:1045.0,f:'1045'}]},{c:[{v:'NG'},{v:1.48093E8,f:'148093000'},{v:142.0,f:'142'}]},{c:[{v:'RU'},{v:1.41933955E8,f:'141933955'},{v:8.4,f:'8.4'}]},{c:[{v:'JP'},{v:1.2779E8,f:'127790000'},{v:339.0,f:'339'}]}],cols:[{id:'_B',label:'Country code',type:'string'},{id:'C',label:'Population',type:'number'},{id:'D',label:'Population Density',type:'number'}]},reqId:'0',status:'ok'});")
        
             
        self.finish()
        
def GoogleJson(D):
    if isinstance(D,dict):
        return  '{' + ','.join([str(k) + ':' + GoogleJson(v) for (k,v) in D.items()]) + '}'
    elif isinstance(D,list) or isinstance(D,tuple):
        return '[' + ','.join([GoogleJson(k) for k in D]) + ']'
    elif is_string_like(D):
        return repr(D)
    elif is_num_like(D):
        return str(D)
    elif not D:
        return 'null'
    else:
        print type(D)
        raise ValueError, 'Value cant be converted.'
        

                
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
            if wt == 'json':
                self.write(response.body)
            elif wt == 'python':
                X = ast.literal_eval(response.body)
                #do stuff to X
                jsonstr = json.dumps(X,default=pm.json_util.default)
                self.write(jsonstr)
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
        


#=-=-=-=-=-=-=-=-=-=-=-=-=-
#GET
#=-=-=-=-=-=-=-=-=-=-=-=-=-

def processor_handler(processor,handler,cursor,sock,fd):

    r = cursor.next()
    if processor != None:
        result = processor(r)
    else:
        result = r
    
    handler.write(json.dumps(result,default=pm.json_util.default))
        
    handler.end()    
    

def process_count(r):

    if r.get("errmsg", "") == "ns missing":
        result = 0
    else:
        result = int(r["n"])
        
    return result
       

def process_distinct(r):

    return r["values"]
    
def process_group(r):

    return r["retval"]    
    

def loop(handler,cursor,sock,fd):
    
    hasdata = True

    collection = handler.collection
    needsVersioning = handler.needsVersioning
    versionNumber = handler.versionNumber
    uniqueIndexes = handler.uniqueIndexes
    VarMap = handler.VarMap
    sci= handler.sci
    subcols = handler.subcols
    vNInd =  handler.vNInd
    retInd = handler.retInd
    processor = handler.processor
    
    while hasdata:
        r = cursor.next()

        if len(cursor._Cursor__data) == 0:
            hasdata = False
            
               
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
         
        if processor:
            r = processor(r,collection)
             
        if handler.stream:
            handler.write(json.dumps(r,default=pm.json_util.default) + ',')
        if handler.returnObj:
            handler.returnedObj['data'].append(r)

        if sci and sci in r.keys():
            subcols.append((r['_id'],r[sci]))

    if cursor.alive:
        cursor._refresh()

        
    else:
        if handler.stream:
            handler.write(']')
        
        handler.end()
        

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
            elif action in ['count','distinct']:
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
            DateFormat = collection
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
                    remove = (TimeColNames if TimeColNamesToReturn != 'ALL' else []) + SpaceColNames if SpaceColNamesToReturn != 'ALL' else []
                    retain = (TimeColNamesToReturn if TimeColNamesToReturn != 'ALL' else []) + (SpaceColNamesToReturn if SpaceColNamesToReturn != 'ALL' else [])
                    retainCols = set(vars).difference(set(remove).difference(retain))
                    
                    kwargs['fields'] = list(retainCols.intersection(kwargs['fields'])) if 'fields' in kwargs else list(retainCols) 
            
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
            if args:
                if action not in EXPOSED_ACTIONS:
                    raise ValueError, 'Action type ' + str(action) + ' not recognized or exposed.'                  
                (posargs,kwargs) = getArgs(args)    
                
                if needsVersioning and  'fields' in kwargs.keys() and  action in ['find','find_one']:
                    kwargs['fields'] += ['__versionNumber__'] + uniqueIndexes

                
                posargs = tuple([processArg(arg,collection) for arg in posargs])
                kwargs = dict([(argname,processArg(arg,collection)) for (argname,arg) in kwargs.items()])

            else:
                posargs = ()
                kwargs = {}
            posArgs.append(posargs)
            kwArgs.append(kwargs)

 
        return zip(Actions,posArgs,kwArgs),collection,needsVersioning,versionNumber,uniqueIndexes,vars


def get(*args,**kwargs):
    fh=kwargs.pop('fh',None)    
    returnObj = kwargs.pop('returnObj',True)   
    returnMetadata = kwargs.pop('returnMetadata',True)   
    processor = kwargs.pop('processor',None)

    A,collection,needsVersioning,versionNumber,uniqueIndexes,vars = get_args(*args,**kwargs)
    
    R = collection  
    for (a,p,k) in A:
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
    else:
        raise ValueError, 'querySequence'   
    
    return (posargs,kwargs)
    
    
#=-=-=-=-=-=-=-=-=-=-=-=-=-
#FIND
#=-=-=-=-=-=-=-=-=-=-=-=-=-


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
        
