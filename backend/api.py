#!/usr/bin/env python

import tornado.web
import os
import json
import urllib2
import ast
import pymongo as pm
import pymongo.json_util
from common.utils import IsFile, listdir, is_string_like, ListUnion, Flatten
from common.mongo import Collection
import common.timedate as td
import common.location as loc
import urllib2,urllib


class GetHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello, get")

    def post(self):
        args = json.loads(self.request.body)
        args = dict([(str(x),y) for (x,y) in args.items()])
        collectionName = args.pop('collectionName')
        querySequence = args.pop('querySequence')
        get(collectionName,querySequence,returnObj=False,fh=self,**args)

        
        
class FindHandler(tornado.web.RequestHandler):
    def get(self):
        args = self.request.arguments
        assert 'q' in args.keys() and len(args['q']) == 1
        query = args['q'][0]
        args.pop('q')
        self.write(find(query,**args))

    def post(self):
        args = json.loads(self.request.body)
        args = dict([(str(x),y) for (x,y) in args.items()])
        query = args.pop('q')
        self.write(find(query,**args))
        



#=-=-=-=-=-=-=-=-=-=-=-=-=-
#GET
#=-=-=-=-=-=-=-=-=-=-=-=-=-


EXPOSED_ACTIONS = ['find','find_one','group','skip','limit','sort','count','distinct']

def get(collectionName,querySequence,timeQuery=None, spaceQuery = None, returnMetadata=False,fh = None,returnObj = True,processor = None):
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
    fh : Boolean => file handle to write to
    returnObj : Boolean => store and return computed object
    processor : lambda => processor applied to each row (TODO: fully implement this)
    """
    collection = Collection(collectionName)
    vars = collection.VARIABLES
    ColumnGroups = collection.ColumnGroups


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
                posargs = tuple([processArg(arg,collection) for arg in posargs])
                kwargs = dict([(argname,processArg(arg,collection)) for (argname,arg) in kwargs.items()])
            else:
                posargs = ()
                kwargs = {}
            posArgs.append(posargs)
            kwArgs.append(kwargs)
        # Here is where the real stuff happens the other stuff is preprocessing the query
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


def setArgTuple(t,k,v):
    if t:
        t[0][k] = v
    else:
        t = ({k:v},)
    return t

def getsci(collection):
    if 'Subcollections' in collection.VARIABLES:
        sci = str(collection.VARIABLES.index('Subcollections'))
    else:
        sci = None
    subcols = []    
    
    return  sci,subcols


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
    """Translates the arg to human readable to collections"""
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


def find(query, timeQuery = None, spaceQuery = None, hlParams=None,facetParams=None,mltParams = None, **params):
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
            
    if facetParams == None:
        facetParams = {'field':['agency','subagency','dataset','dateDivisions']}
                    
    params['wt'] = 'json'
    
    paramstring = processSolrArgList('',params)
    facetstring = processSolrArgList('facet',facetParams)
    hlstring = processSolrArgList('hl',hlParams)
    mltstring = processSolrArgList('mlt',mltParams)
    
    URL = 'http://localhost:8983/solr/select?q=' + urllib.quote(query) + paramstring + facetstring + hlstring + mltstring
    
    if params['wt'] == 'json':
        return urllib2.urlopen(URL).read()
    elif params['wt'] == 'python':
        X = ast.literal_eval(urllib.urlopen(URL).read())
        #do stuff to X
        return json.dumps(X,default=pm.json_util.default)
    

def processSolrArgList(base,valdict)    :
    return ('&' + ((base + '=true&') if base and '' not in valdict.keys() else '') + '&'.join([processSolrArg(base,key,valdict[key]) for key in valdict])) if valdict else ''       
    
def processSolrArg(base,key,value):
    return base + ('.' if key and base else '') + key + '=' + urllib.quote(value) if is_string_like(value) else '&'.join([base + ('.' if key and base else '') + key + '=' + urllib.quote(v) for v in value])
