#!/usr/bin/env python

from common.mongo import Collection, cleanCollection, SPECIAL_KEYS
from common.utils import IsFile, listdir, is_string_like, ListUnion, uniqify,createCertificate, rgetattr,rhasattr, dictListUniqify, Flatten,MakeDir, PathExists, IsDir
import common.timedate as td
import common.location as loc
import common.commonjs as commonjs
import common.solr as ourSolr
import backend.api as api
import solr
import itertools
import functools
import json
import pymongo as pm
import pymongo.json_util as ju
import pymongo.son as son
import os
import hashlib
import urllib
import urllib2
import ast
import math
from System.Protocols import activate

def pathToSchema():
    plist = os.getcwd().split('/')
    assert 'govlove' in plist, "You're not in a filesystem with name 'govlove'."
    return '/'.join(plist[:plist.index('govlove') + 1]) + '/backend/solr-home/solr/conf/schema.xml'
    
def expand(r):
    L = [k for (k,v) in r if isinstance(v,list)]
    I = itertools.product(*tuple([v for (k,v) in r if isinstance(v,list)]))
    return [tuple([(k,v) for (k,v) in r if is_string_like(v)] + zip(L,x)) for x in I]
    
def getQueryList(collection,keys,atVersion,toVersion,slicesCorrespondToIndexes):
    totalVariables = collection.columns
    VarMap = dict(zip(totalVariables,[str(x) for x in range(len(totalVariables))]))
    origInd = VarMap['__originalVersion__'] ; retInd = VarMap['__retained__'] ; vNInd = VarMap['__versionNumber__']
    keys = [str(x) for x in keys]
    existence = [(k,{'$exists':True,'$ne':''}) for k in keys]
    if keys:
        Q1 = api.processArg(dict([(origInd,{'$gt':atVersion}),(vNInd,toVersion)] + existence),collection)
        Q2 = api.processArg(dict([(retInd,{'$exists':False}),(vNInd,{'$lt':toVersion,'$gte':atVersion})] + existence),collection)
        Q3 = api.processArg(dict([(retInd,True),(vNInd,{'$lt':toVersion,'$gte':atVersion}),(origInd,{'$lte':atVersion})] + existence),collection)
        colnames = [k for k in keys if k.split('.')[0] in collection.columns]
        colgroups = [k for k in keys if k in collection.columnGroups]
        T= ListUnion([collection.columnGroups[k] for k in colgroups])
        kInds = getStrs(collection,colnames + T)
        R = list(collection.find(Q1,fields = kInds)) + list(collection.find(Q2,fields = kInds)) + (list(collection.find(Q3,fields = kInds)) if not slicesCorrespondToIndexes else [])
        R = [son.SON([(collection.columns[int(k)],r[k]) for k in r.keys() if k.isdigit() and r[k]]) for r in R]
        R = [[(k,rgetattr(r,k.split('.'))) for k in keys if  rhasattr(r,k.split('.')) if k not in T] + [(g,[r[k] for k in collection.columnGroups[g] if k in r.keys() and r[k]]) for g in colgroups ] for r in R]
        return uniqify(ListUnion([expand(r) for r in R]))
    else:
        return [()]
        
def subqueries(q):
    K = q.keys()
    ind = itertools.product(*[[0,1]]*len(K))
    return [son.SON([(K[i],q[K[i]]) for (i,k) in enumerate(j) if k]) for  j in ind]
    
def subTuples(T):
    ind = itertools.product(*[[0,1]]*len(T))
    return [tuple([t for (t,k) in zip(T,I) if k]) for I in ind]
    
STANDARD_META = ['title','subject','description','author','keywords','content_type','last_modified','dateReleased','links']
STANDARD_META_FORMATS = {'last_modified':'dt','dateReleased':'dt'}

@activate(lambda x : x[1],lambda x : x[2])
def updateCollectionIndex(collectionName,incertpath,certpath, verbose=False):
    """incrementally update the query database.
    
        For a given collection with name NAME, this creates (or updates)  the associated collection __NAME__SLICES__
        
        Documents in this slice collection has the following keys:
            'q' = list of pymongo simple queries (with keys in collection.sliceCols) corresponding to an identical slice
            'h' = hash of concentenated record ID lists of all records in the slice
            'v' = versionNumber of the document
            'o' = originalVersion where this slice  appears
            'd' = whether this slice is deleted after this version
        
        
        The incrementation logic is:
            Add "new" queries -- as measured by having __originalIndex__ > atVersion, where atVersion = maximum existing version of a slice (fast to evaluate) 
                -- and set the resulting records to have 'v' key and 'o' = currentNumber
                
            

                
            Set delete keys on all deleted queries, where "deleted" is measured by:
                -- being in version < current, and NOT having a __retained__ key
                -- not actually existing in the current version (this needs to be checked to handle the situation of something that has been deleted and then re-added)

    """


    S = ourSolr.query('collectionName:' + collectionName,fl = 'versionNumber',sort='versionNumber desc',wt = 'json')
    existing_slice = ast.literal_eval(S)['response']['docs']
    
    if len(existing_slice) > 0:
        atVersion = existing_slice[0]['versionNumber'][0]
    else:
        atVersion = -1
         
    collection = Collection(collectionName)
    currentVersion = collection.currentVersion
    sliceDB = collection.slices
    slicecount = sliceDB.find({'original':{'$gt':atVersion},'version':currentVersion}).count()
    block_size = 50000
    MakeDir(certpath)
  
    
    if slicecount < block_size:
        add_slices(collectionName,currentVersion,atVersion,0,None)
    else:       
        try:
            import System.grid as grid
        except ImportError:
            add_slices(collectionName,currentVerison,atVersion,0,None)
        else:
            num_blocks = int(math.ceil(float(slicecount)/block_size))
            jobdescrs = [{'argstr': "import backend.indexing as I; I.add_slices(" + ", ".join([repr(x) for x in [collectionName, currentVersion,atVersion, block_size*i, block_size]]) + ")",'outfile': certpath + str(i),'name': 'Index' + collectionName + '_' + str(i)} for i in range(num_blocks)]
            retvals = grid.submitJobs(jobdescrs)
                    
    delete_slices(sliceDB,currentVersion,atVersion)
    
    createCertificate(certpath + 'final.txt','Collection ' + collectionName + ' indexed.')     

  
def add_slices(collectionName,currentVersion, atVersion,skip,limit,verbose=False): 
    collection = Collection(collectionName)
  
    d,ArgDict = initialize_argdict(collection)        
    
    solr_interface = solr.SolrConnection("http://localhost:8983/solr")    
    
    sliceDB = collection.slices
    slicecount = sliceDB.count()
    
    slice = sliceDB.find({'original':{'$gt':atVersion},'version':currentVersion},timeout=False)
    
    if skip:
        slice = slice.skip(skip)
    if limit:
        slice = slice.limit(limit)

        
    for (i,r) in enumerate(slice):      
        q = r['slice']  
        if verbose or (i/100)*100 == i:
            print 'Adding:' , q, 'in', collectionName
            print i, '(', skip + i , ')'
        dd = d.copy()       
        try:
            addToIndex(q,dd,collection,solr_interface,slicecount,**ArgDict)  
        except AttributeError:
            addToIndex(q,dd,collection,solr_interface,slicecount,**ArgDict)
       
    solr_interface.commit()


def delete_slices(sliceDB,currentVersion,atVersion):
    solr_interface = solr.SolrConnection("http://localhost:8983/solr")    
    for r in sliceDB.find({'version':{'$gte':atVersion,'$lt':currentVersion},'original':{'$lte':atVersion}},timeout=False):
        q = r['slice']
        ID = mongoID(q)
        print 'deleting', ID, q
        solr_interface.delete_query('mongoID:' + ID) 
    solr_interface.commit()
      
    
    
def queryToText(q,processors):
    return ', '.join([key + '=' + translate(processors[key],decode_obj(value)) for (key,value) in q.items()])  

def queryKeys(q,processors):
    return q.keys()

def queryValues(q,processors):
    return [translate(processors[key],decode_obj(value)) for (key,value) in q.items()]
    

def mongoID(q,collectionName):
    queryID = [('collectionName',collectionName),('query',q)]
    return hashlib.sha1(json.dumps(queryID,default=ju.default)).hexdigest()
    
    
def addToIndex(q,d,collection,solr_interface,slicecount,contentColNums = None, timeColInds=None,timeColNames=None, timeColNameInds = None,timeColNameDivisions = None,timeColNamePhrases=None,overallDate = '', overallDateFormat = '', timeFormatter = None,reverseTimeFormatter = None,dateDivisions=None,datePhrases=None,mindate = None,maxdate = None,overallLocation = None, spaceColNames = None, spaceColInds = None,subColInd = None,Return=False,valueProcessors=None,valueProcessorsKey=None):


    q['__versionNumber__'] = collection.currentVersion
    query = api.processArg(q,collection)
    q.pop('__versionNumber__') 
    
    d['collectionName'] = collection.name
    
    d['query'] = json.dumps(q,default=ju.default)
    
    d['mongoID'] = mongoID(q,collection.name)
    
    d['mongoText'] = queryToText(q,valueProcessorsKey)    
    
    d['sliceValues'] = queryValues(q,valueProcessorsKey)
    
    d['sliceKeys'] = queryKeys(q,valueProcessorsKey)

    d['versionNumber'] = collection.currentVersion

    if dateDivisions == None:
        dateDivisions = []
    else:
        dateDivisions = dateDivisions[:]
    if datePhrases == None:
        datePhrases = []
    else:
        datePhrases = datePhrases[:]
    if spaceColNames == None:
        spaceColNames = []
    
        
    #stats
    d['volume'] = collection.find(query).count()
    
    contentColNums = [i for i in contentColNums if i not in query.keys()]
    
    if d['volume'] > 0:
        if d['volume'] < 5000:
            smallAdd(d,query,collection,contentColNums, timeColInds ,timeColNames , timeColNameInds ,timeColNameDivisions ,timeColNamePhrases ,overallDate , overallDateFormat, timeFormatter ,reverseTimeFormatter ,dateDivisions ,datePhrases ,mindate ,maxdate ,overallLocation , spaceColNames , spaceColInds ,subColInd, valueProcessors,slicecount)
        else:
            largeAdd(d,query,collection,contentColNums,  timeColInds ,timeColNames , timeColNameInds ,timeColNameDivisions ,timeColNamePhrases ,overallDate, overallDateFormat, timeFormatter ,reverseTimeFormatter ,dateDivisions ,datePhrases ,mindate ,maxdate ,overallLocation , spaceColNames , spaceColInds ,subColInd, valueProcessors)
    
        Subcollections = uniqify(ListUnion(collection.find(query).distinct(str(subColInd))))
        metadata = collection.metadata['']
        for sc in Subcollections:
            metadata.update(collection.metadata.get(sc,{}))
        for k in metadata.keys():
            if k in STANDARD_META:
                if k in STANDARD_META_FORMATS.keys():
                    val = coerceToFormat(metadata[k],STANDARD_META_FORMATS[k])
                    if val:
                        d[str(k)] = val
                else:
                    d[str(k)] = str(metadata[k])
    
        if Return:
            return d
        else:
            solr_interface.add(**d)
   
    
def smallAdd(d,query,collection,contentColNums, timeColInds ,timeColNames , timeColNameInds ,timeColNameDivisions ,timeColNamePhrases ,overallDate, overallDateFormat, timeFormatter ,reverseTimeFormatter ,dateDivisions ,datePhrases ,mindate ,maxdate ,overallLocation , spaceColNames , spaceColInds ,subColInd, valueProcessors,slicecount):

    R = collection.find(query,timeout=False)
    colnames = []
    d['sliceContents'] = []
    Subcollections = []
    
    spaceVals = spaceColNames
    commonLocation = overallLocation    
    for sv in spaceColNames:
        commonLocation = loc.intersect(commonLocation,sv)
        if not commonLocation:
            break     
  
    for (i,r) in enumerate(R):
        d['sliceContents'].append(' '.join([translate(valueProcessors.get(x,None),decode_obj(rgetattr(r,x.split('.')))) if rhasattr(r,x.split('.')) else '' for x in contentColNums]))
                      
        colnames  = uniqify(colnames + r.keys())
        
        if subColInd:
            Subcollections += r[str(subColInd)]
                
        if timeColInds:
            for x in timeColInds:
                if str(x) in r.keys():
                    time = r[str(x)]
                    if overallDate:
                        time = timeFormatter(overallDate + reverseTimeFormatter(time))
                    dateDivisions += td.getLowest(time)
                    datePhrases.append(td.phrase(time))     
                    mindate = td.makemin(mindate,time)
                    maxdate = td.makemax(maxdate,time)
        if spaceColInds:
            for x in spaceColInds:
                if str(x) in r.keys():
                    location = loc.integrate(overallLocation,r[str(x)])
                    commonLocation = loc.intersect(commonLocation,r[str(x)]) if commonLocation != None else None
                    spaceVals.append(location)
    
    d['sliceContents'] = ' '.join(d['sliceContents'])
    Subcollections = uniqify(Subcollections)
    d['columnNames'] = [collection.columns[int(x)] for x in colnames if x.isdigit()]
    d['dimension'] = len(d['columnNames'])
    #time/date
        
    if overallDateFormat:
        d['dateFormat'] = overallDateFormat
        
        if 'timeColNames' in collection.columnGroups.keys():
            K = [k for (k,j) in enumerate(timeColNameInds) if str(j) in colnames]
            dateDivisions += uniqify(ListUnion([timeColNameDivisions[k] for k in K]))
            mindate = td.makemin(mindate,min([timeColNames[k] for k in K]))
            maxdate = td.makemax(maxdate,max([timeColNames[k] for k in K]))         
            datePhrases += uniqify([timeColNamePhrases[k] for k in K])
        
        d['beginDate'] = td.convertToDT(mindate)
        d['endDate'] = td.convertToDT(maxdate,convertMode='High')
        d['dateDivisions'] = uniqify(dateDivisions)
        d['datePhrases'] = datePhrases if d['volume'] < 10000 else uniqify(datePhrases)

    if spaceVals:
        d['spatialDivisions'] = uniqify(ListUnion(map(loc.divisions,spaceVals)))
        d['spatialDivisionsTight'] = uniqify(ListUnion(map(loc.divisions2,spaceVals)))
        d['spatialPhrases'] = uniqify(map(loc.phrase,spaceVals))
        d['spatialPhrasesTight'] = uniqify(map(loc.phrase2,spaceVals))
        
    return d
    
def largeAdd(d,query,collection,contentColNums, timeColInds ,timeColNames , timeColNameInds ,timeColNameDivisions ,timeColNamePhrases ,overallDate , overallDateFormat, timeFormatter ,reverseTimeFormatter ,dateDivisions ,datePhrases ,mindate ,maxdate ,overallLocation , spaceColNames, spaceColInds, subColInd, valueProcessors):

    print '0'
    exists = []
    check = range(len(collection.columns))
    while check:
        k = check.pop(0)
        rec = collection.find_one( dict(query.items() + [(str(k),{'$exists':True})]))
        if rec:
            rec.pop('_id')
            new = map(int,rec.keys()) 
            check = list(set(check).difference(new))
            check.sort()
            exists += [(pos,collection.columns[pos]) for pos in new]
    
    print '1'
    exists = [e for e in exists if e[1] not in SPECIAL_KEYS] 
    (colnums,colnames) = zip(*exists)
    
    d['columnNames'] = colnames
    d['dimension'] = len(d['columnNames'])
       
    if overallDateFormat:
        d['dateFormat'] = overallDateFormat
        
        if timeColInds:
            dateColVals = ListUnion([collection.find(query).distinct(str(t)) for t in timeColInds if t in colnums])
            if overallDate:
                dateColVals = [timeFormatter(overallDate + reverseTimeFormatter(time)) for time in dateColVals]
        
            dateDivisions += uniqify(ListUnion(map(td.getLowest,dateColVals)))
            datePhrases += uniqify(map(td.phrase, dateColVals))
            mindate = td.makemin(mindate,min(dateColVals),)
            maxdate = td.makemax(maxdate,max(dateColVals),)
      
  
        if timeColNameInds:
            K = [k for (k,j) in enumerate(timeColNameInds) if k in colnums]
            dateDivisions += uniqify(ListUnion([timeColNameDivisions[k] for k in K]))
            mindate = td.makemin(mindate,min([timeColNames[k] for k in K]),)
            maxdate = td.makemax(maxdate,max([timeColNames[k] for k in K]),)
            datePhrases += [timeColNamePhrases[k] for k in K]

        dateDivisions = uniqify(dateDivisions)
        datePhrases = uniqify(datePhrases)
        
        d['beginDate'] = td.convertToDT(mindate)
        d['endDate'] = td.convertToDT(maxdate,convertMode='High')
        d['dateDivisions'] = uniqify(dateDivisions)
        d['datePhrases'] = datePhrases
    
    print '2'    
    if spaceColInds:
        spaceColVals = ListUnion([collection.find(query).distinct(str(t)) for t in spaceColInds if t in colnums])
        spaceColVals = [loc.integrate(overallLocation,scv) for scv in spaceColVals]   
    else:
        spaceColVals = []
    spaceVals = spaceColNames + spaceColVals 
    if spaceVals:
        d['spatialDivisions'] = uniqify(ListUnion(map(loc.divisions,spaceVals)))
        d['spatialDivisionsTight'] = uniqify(ListUnion(map(loc.divisions2,spaceVals)))
        d['spatialPhrases'] = uniqify(map(loc.phrase,spaceVals))
        d['spatialPhrasesTight'] = uniqify(map(loc.phrase2,spaceVals))
    commonLocation = overallLocation
    for sv in spaceVals:
        commonLocation = loc.intersect(commonLocation,sv)
        if not commonLocation:
            break 
    if commonLocation:
        d['commonLocation'] = loc.phrase(commonLocation)
                
    print '3'    

    d['sliceContents'] = ' '.join(uniqify(ListUnion([translate_list(valueProcessors.get(x,None) ,map(decode_obj,collection.find(query).distinct(x))) for x in contentColNums])))

    return d


def translate(trans,l):
    return trans(l) if trans else l

def translate_list(trans,l):
    
    return map(trans,l) if trans else l
    
def initialize_argdict(collection):

    d = {} ; ArgDict = {}
    
    sliceCols = uniqify(Flatten(collection.sliceCols))
    sliceColList = ListUnion([[x] if x.split('.')[0] in collection.columns else collection.columnGroups.get(x,[]) for x in sliceCols])
    
    if hasattr(collection,'contentCols'):
        contentColList = ListUnion([[x] if x.split('.')[0] in collection.columns else collection.columnGroups.get(x,[]) for x in collection.contentCols])
        contentCols = uniqify(contentColList + sliceColList)
    else:
        contentCols = sliceColList
    contentColNums = getStrs(collection,contentCols)
    ArgDict['contentColNums'] = contentColNums
    
    if hasattr(collection,'dateFormat'):
        dateFormat = collection.dateFormat
        ArgDict['overallDateFormat'] = dateFormat
        timeFormatter = td.mongotimeformatter(dateFormat)
        ArgDict['timeFormatter'] = timeFormatter
    else:
        dateFormat = ''
        
    if hasattr(collection,'overallDate'):
        od = collection.overallDate['date']
        odf = collection.overallDate['format']
        ArgDict['overallDate'] = od
        overallDateFormat = odf + dateFormat
        ArgDict['overallDateFormat'] = overallDateFormat
        timeFormatter = td.mongotimeformatter(overallDateFormat)
        ArgDict['timeFormatter'] = timeFormatter

        OD = timeFormatter(overallDate +'X'*len(dateFormat))
        ArgDict['dateDivisions'] = td.getLowest(OD)
        ArgDict['datePhrases'] = [td.phrase(OD)]
        ArgDict['mindate'] = OD
        ArgDict['maxdate'] = OD             
        
        if dateFormat:
            reverseTimeFormatter = td.reverse(dateFormat)
            ArgDict['reverseTimeFormatter'] = reverseTimeFormatter
            
    else:
        od = ''
                    
    if 'timeColNames' in collection.columnGroups.keys():
        timeColNamesInd = getNums(collection,collection.columnGroups['timeColNames'])
        tcs = [timeFormatter(od + t) for t in collection.columnGroups['timeColNames']]
        ArgDict['timeColNames'] = tcs 
        ArgDict['timeColNameInds'] = timeColNamesInd
        ArgDict['timeColNameDivisions'] = [[td.TIME_DIVISIONS[x] for x in td.getLowest(tc)] for tc in tcs] 
        ArgDict['timeColNamePhrases'] = [td.phrase(t) for t in tcs]

    if 'timeColumns' in collection.columnGroups.keys():
        ArgDict['timeColInds'] = getNums(collection,collection.columnGroups['timeColumns'])
            
    #overall location
    if hasattr(collection,'overallLocation'):
        ol = Collection.overallLocation
        ArgDict['overallLocation'] = ol
    else:
        ol = None
        
    #get divisions and phrases from OverallLocation and SpaceColNames
    if 'spaceColNames' in collection.columnGroups.keys():
        spaceColNames = collection.columnGroups['spaceColNames']
        ArgDict['spaceColNames'] = [loc.integrate(ol,x) for x in spaceColNames]

        
    if 'spaceColumns' in collection.columnGroups.keys():
        ArgDict['spaceColInds'] = getNums(collection,collection.columnGroups['spaceColumns'])

    Source = collection.source
    SourceNameDict = son.SON([(k,Source[k]['name'] if isinstance(Source[k],dict) else Source[k]) for k in Source.keys()])
    SourceAbbrevDict = dict([(k,Source[k]['shortName']) for k in Source.keys() if isinstance(Source[k],dict) and 'shortName' in Source[k].keys() ])
    d['sourceSpec'] = json.dumps(SourceNameDict,default=ju.default)
    d['agency'] = SourceNameDict['agency']
    d['subagency'] = SourceNameDict['subagency']
    d['dataset'] = SourceNameDict['dataset']
    for k in set(SourceNameDict.keys()).difference(['agency','subagency','dataset']):
        d['source_' + str(k).lower()] = SourceNameDict[k]
    for k in SourceAbbrevDict.keys():
        d['source_' + str(k).lower() + '_acronym'] = SourceAbbrevDict[k]
    d['source'] = ' '.join(SourceNameDict.values() + SourceAbbrevDict.values())
        
    if 'subcollections' in collection.columns:
        ArgDict['subColInd'] = collection.columns.index('subcollections')
     
    value_processor_instructions = stringifyDictElements(collection.valueProcessors)
    vpcontext = commonjs.translatorContext(value_processor_instructions)
    ArgDict['valueProcessors'],ArgDict['valueProcessorsKey'] = get_processors(value_processor_instructions,collection, vpcontext ,commonjs.js_call)
    
                                    
    return d, ArgDict
    


def get_processors(instruction_set,collection,context,callfunc):
    
    processors = {}   
    processors_key = {}
    for (i,name) in enumerate(collection.columns):
        x = str(i)
        processors[x] = None
        processors_key[name] = None
        if instruction_set.has_key(name):
            processors[x] = functools.partial(callfunc,context,name)
            processors_key[name] = processors[x]
    vpcolgroups = [x for x in instruction_set.keys() if x in collection.columnGroups.keys()]
    for vpc in vpcolgroups:
        for name in collection.columnGroups[vpc]:
            x = str(collection.columns.index(name))
            processors[x] =  functools.partial(callfunc,context,vpc)
            processors_key[name] = processors[x]
            
    return processors,processors_key
    

def stringifyDictElements(d):
    
    return dict([(str(k),stringifyDictElements(v) if hasattr(v,'keys') else str(v)) for (k,v) in d.items()])

def getSliceColTuples(collection):
    sliceColList = collection.sliceCols
    sliceColU = uniqify(ListUnion(sliceColList))
    sliceColInds = getStrs(collection,sliceColU)
    OK = dict([(x,x in collection.columnGroups.keys() or MoreThanOne(collection,y)) for (y,x) in zip(sliceColInds,sliceColU)])
    sliceColList = [tuple([x for x in sliceColU if x in sc and OK[x]]) for sc in sliceColList]
    sliceColTuples = uniqify(ListUnion([subTuples(sc) for sc in sliceColList]))
    
    return sliceColTuples
    
def MoreThanOne(collection,key):
    v1 = rgetattr(list(collection.find({key:{'$exists':True}}).sort([(key,-1)]).limit(1))[0],key.split('.'))
    v2 = rgetattr(list(collection.find({key:{'$exists':True}}).sort([(key,1)]).limit(1))[0],key.split('.'))
    return v1 != v2
 

    
def getNums(collection,namelist):
    numlist = []
    for n in namelist:
        if n in collection.columns:
            numlist.append(collection.columns.index(n))
        else:
            numlist.append([collection.columns.index(m) for m in collection.columnGroups[n]])
    return numlist
    
def getStrs(collection,namelist):
    numlist = []
    for n in namelist:
        ns = n.split('.')
        dot = '.' if len(ns) > 1 else ''
        if ns[0] in collection.columns:
            numlist.append(str(collection.columns.index(ns[0])) + dot + '.'.join(n.split('.')[1:]))
        else:
            numlist.append([str(collection.columns.index(m)) for m in collection.columnGroups[n]])
    return numlist
        

def decode_obj(x):
    if is_string_like(x):
        return decode(x)
    elif hasattr(x,'keys'):
        return pm.son.SON([(k,decode_obj(x[k])) for k in x.keys()]) 
    elif is_instance(x,list):
        return [decode_obj(y) for y in x]
    else:
        return x

def decode(v):
    try:
        v = v.decode('utf-8')
    except (UnicodeEncodeError,UnicodeDecodeError):
        try:
            return v.decode('latin-1').encode('utf-8')
        except (UnicodeEncodeError,UnicodeDecodeError):
            return unicode(v.encode('utf-8'),errors='ignore')
    else:
        return v
    
        
def coerceToFormat(md,format):
    if format == 'tplist':
        return ', '.join(md)
