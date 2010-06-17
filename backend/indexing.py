#!/usr/bin/env python

from common.mongo import Collection, cleanCollection, SPECIAL_KEYS
from common.utils import IsFile, listdir, is_string_like, ListUnion, uniqify,createCertificate, rgetattr,rhasattr, dictListUniqify, Flatten
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
    totalVariables = collection.totalVariables
    VarMap = dict(zip(totalVariables,[str(x) for x in range(len(totalVariables))]))
    origInd = VarMap['__originalVersion__'] ; retInd = VarMap['__retained__'] ; vNInd = VarMap['__versionNumber__']
    keys = [str(x) for x in keys]
    existence = [(k,{'$exists':True,'$ne':''}) for k in keys]
    if keys:
        Q1 = api.processArg(dict([(origInd,{'$gt':atVersion}),(vNInd,toVersion)] + existence),collection)
        Q2 = api.processArg(dict([(retInd,{'$exists':False}),(vNInd,{'$lt':toVersion,'$gte':atVersion})] + existence),collection)
        Q3 = api.processArg(dict([(retInd,True),(vNInd,{'$lt':toVersion,'$gte':atVersion}),(origInd,{'$lte':atVersion})] + existence),collection)
        colnames = [k for k in keys if k.split('.')[0] in collection.totalVariables]
        colgroups = [k for k in keys if k in collection.ColumnGroups]
        T= ListUnion([collection.ColumnGroups[k] for k in colgroups])
        kInds = getStrs(collection,colnames + T)
        R = list(collection.find(Q1,fields = kInds)) + list(collection.find(Q2,fields = kInds)) + (list(collection.find(Q3,fields = kInds)) if not slicesCorrespondToIndexes else [])
        R = [son.SON([(collection.totalVariables[int(k)],r[k]) for k in r.keys() if k.isdigit() and r[k]]) for r in R]
        R = [[(k,rgetattr(r,k.split('.'))) for k in keys if  rhasattr(r,k.split('.')) if k not in T] + [(g,[r[k] for k in collection.ColumnGroups[g] if k in r.keys() and r[k]]) for g in colgroups ] for r in R]
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
STANDARD_META_FORMATS = {'keywords':'tplist','last_modified':'dt','dateReleased':'dt'}

@activate(lambda x : x[1],lambda x : x[2])
def updateCollectionIndex(collectionName,incertpath,certpath, slicesCorrespondToIndexes = False):
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
    collection = Collection(collectionName)
    currentVersion = collection.currentVersion
  
    d,ArgDict = initialize_argdict(collection)

    S = ourSolr.query('collectionName:' + collectionName,fl = 'versionNumber',sort='versionNumber desc',wt = 'json')
    existing_slice = ast.literal_eval(S)['response']['docs']
    
    if len(existing_slice) > 0:
        atVersion = existing_slice[0]['versionNumber'][0]
    else:
        atVersion = -1

    solr_interface = solr.SolrConnection("http://localhost:8983/solr")    
    
    sliceDB = collection.slices
    
    for r in sliceDB.find({'original':{'$gt':atVersion},'version':currentVersion},timeout=False):      
        q = r['slice']  
        print 'Adding:' , q, 'in', collectionName    
        dd = d.copy()       
        addToIndex(q,dd,collection,solr_interface,**ArgDict)  
        

    for r in sliceDB.find({'version':{'$gte':atVersion,'$lt':currentVersion},'original':{'$lte':atVersion}},timeout=False):
        q = r['slice']
        ID = mongoID(q)
        print 'deleting', ID, q
        solr_interface.delete_query('mongoID:' + ID)
                
        
    solr_interface.commit()
    
    createCertificate(certpath,'Collection ' + collectionName + ' indexed.')        
    
    
def queryToText(q,context):
    return ', '.join([key + '=' + (commonjs.js_call(context,key,value) if context.instructions.has_key(key) else value) for (key,value) in q.items()])  
    

def mongoID(q,collectionName):
    queryID = [('collectionName',collectionName),('query',q)]
    return hashlib.sha1(json.dumps(queryID,default=ju.default)).hexdigest()
    
    
def addToIndex(q,d,collection,solr_interface,contentColNums = None, timeColInds=None,timeColNames=None, timeColNameInds = None,timeColNameDivisions = None,timeColNamePhrases=None,OverallDate = '', OverallDateFormat = '', timeFormatter = None,reverseTimeFormatter = None,dateDivisions=None,datePhrases=None,mindate = None,maxdate = None,OverallLocation = None, spaceColNames = None, spaceColInds = None,subColInd = None,Return=False,translatorContext=None):


    q['__versionNumber__'] = collection.currentVersion
    query = api.processArg(q,collection)
    q.pop('__versionNumber__') 
    
    d['collectionName'] = collection.name
    
    d['query'] = json.dumps(q,default=ju.default)
    
    d['mongoID'] = mongoID(q,collection.name)
    
    d['mongoText'] = queryToText(q,translatorContext)
    
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
    
    if d['volume'] < 1000:
        smallAdd(d,query,collection,contentColNums, timeColInds ,timeColNames , timeColNameInds ,timeColNameDivisions ,timeColNamePhrases ,OverallDate , OverallDateFormat, timeFormatter ,reverseTimeFormatter ,dateDivisions ,datePhrases ,mindate ,maxdate ,OverallLocation , spaceColNames , spaceColInds ,subColInd, translatorContext)
    else:
        largeAdd(d,query,collection,contentColNums,  timeColInds ,timeColNames , timeColNameInds ,timeColNameDivisions ,timeColNamePhrases ,OverallDate, OverallDateFormat, timeFormatter ,reverseTimeFormatter ,dateDivisions ,datePhrases ,mindate ,maxdate ,OverallLocation , spaceColNames , spaceColInds ,subColInd, translatorContext)

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
        else:
            if is_string_like(metadata[k]):
                d[str(k) + '_t'] = metadata[k]        

    if Return:
        return d
    else:
        solr_interface.add(**d)
   
    
def smallAdd(d,query,collection,contentColNums, timeColInds ,timeColNames , timeColNameInds ,timeColNameDivisions ,timeColNamePhrases ,OverallDate, OverallDateFormat, timeFormatter ,reverseTimeFormatter ,dateDivisions ,datePhrases ,mindate ,maxdate ,OverallLocation , spaceColNames , spaceColInds ,subColInd,translatorContext ):

    R = collection.find(query,timeout=False)
    colnames = []
    d['sliceContents'] = []
    d['slicePhrases'] = []
    Subcollections = []
    
    spaceVals = spaceColNames
    commonLocation = OverallLocation    
    for sv in spaceColNames:
        commonLocation = loc.intersect(commonLocation,sv)
        if not commonLocation:
            break     

    
    Translators =  dict ([(x,functools.partial(commonjs.js_call,translatorContext,collection.totalVariables[int(x)]) if collection.translators.has_key(collection.totalVariables[int(x)]) else None)  if '.' not in x else (None,None) for x in contentColNums])
    
    for (i,r) in enumerate(R):
    
        d['sliceContents'].append( ' '.join([makestr(r,x,Translators[x]) if rhasattr(r,x.split('.')) else '' for x in contentColNums]))
                      
        colnames  = uniqify(colnames + r.keys())
        
        if subColInd:
            Subcollections += r[str(subColInd)]
                
        if timeColInds:
            for x in timeColInds:
                if str(x) in r.keys():
                    time = r[str(x)]
                    if OverallDate:
                        time = timeFormatter(OverallDate + reverseTimeFormatter(time))
                    dateDivisions += td.getLowest(time)
                    datePhrases.append(td.phrase(time))     
                    mindate = td.makemin(mindate,time)
                    maxdate = td.makemax(maxdate,time)
        if spaceColInds:
            for x in spaceColInds:
                if str(x) in r.keys():
                    location = loc.integrate(OverallLocation,r[str(x)])
                    commonLocation = loc.intersect(commonLocation,r[str(x)]) if commonLocation != None else None
                    spaceVals.append(location)
                   
    d['sliceContents'] = ' '.join(d['sliceContents'])
    d['slicePhrases'] = ', '.join(d['slicePhrases'])
    Subcollections = uniqify(Subcollections)
    d['columnNames'] = [collection.totalVariables[int(x)] for x in colnames if x.isdigit()]
    d['dimension'] = len(d['columnNames'])
    #time/date
        
    if OverallDateFormat:
        d['dateFormat'] = OverallDateFormat
        
        if 'TimeColNames' in collection.ColumnGroups.keys():
            K = [k for (k,j) in enumerate(timeColNameInds) if str(j) in colnames]
            dateDivisions += uniqify(ListUnion([timeColNameDivisions[k] for k in K]))
            mindate = td.makemin(mindate,min([timeColNames[k] for k in K]))
            maxdate = td.makemax(maxdate,max([timeColNames[k] for k in K]))         
            datePhrases += uniqify([timeColNamePhrases[k] for k in K])
        
        d['begin_date'] = td.convertToDT(mindate)
        d['end_date'] = td.convertToDT(maxdate,convertMode='High')
        d['dateDivisions'] = ' '.join(uniqify(dateDivisions))
        d['datePhrases'] = ', '.join(datePhrases if d['volume'] < 10000 else uniqify(datePhrases))

    if spaceVals:
        d['spatialDivisions'] = ', '.join(uniqify(ListUnion(map(loc.divisions,spaceVals))))
        d['spatialDivisionsTight'] = ', '.join(uniqify(ListUnion(map(loc.divisions2,spaceVals))))
        d['spatialPhrases'] = uniqify(map(loc.phrase,spaceVals))
        d['spatialPhrasesTight'] = uniqify(map(loc.phrase2,spaceVals))
        
    return d
    
def largeAdd(d,query,collection,contentColNums, timeColInds ,timeColNames , timeColNameInds ,timeColNameDivisions ,timeColNamePhrases ,OverallDate , OverallDateFormat, timeFormatter ,reverseTimeFormatter ,dateDivisions ,datePhrases ,mindate ,maxdate ,OverallLocation , spaceColNames, spaceColInds, subColInd, translatorContext):

    print '0'
    exists = []
    check = range(len(collection.totalVariables))
    while check:
        k = check.pop(0)
        rec = collection.find_one( dict(query.items() + [(str(k),{'$exists':True})]))
        if rec:
            rec.pop('_id')
            new = map(int,rec.keys()) 
            check = list(set(check).difference(new))
            check.sort()
            exists += [(pos,collection.totalVariables[pos]) for pos in new]
    
    print '1'
    exists = [e for e in exists if e[1] not in SPECIAL_KEYS] 
    (colnums,colnames) = zip(*exists)
    
    d['columnNames'] = colnames
    d['dimension'] = len(d['columnNames'])
       
    if OverallDateFormat:
        d['dateFormat'] = OverallDateFormat
        
        if timeColInds:
            dateColVals = ListUnion([collection.find(query).distinct(str(t)) for t in timeColInds if t in colnums])
            if OverallDate:
                dateColVals = [timeFormatter(OverallDate + reverseTimeFormatter(time)) for time in dateColVals]
        
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
        
        d['begin_date'] = td.convertToDT(mindate)
        d['end_date'] = td.convertToDT(maxdate,convertMode='High')
        d['dateDivisions'] = ' '.join(uniqify(dateDivisions))
        d['datePhrases'] = ', '.join(datePhrases)
    
    print '2'    
    if spaceColInds:
        spaceColVals = ListUnion([collection.find(query).distinct(str(t)) for t in spaceColInds if t in colnums])
        spaceColVals = [loc.integrate(OverallLocation,scv) for scv in spaceColVals]   
    else:
        spaceColVals = []
    spaceVals = spaceColNames + spaceColVals 
    if spaceVals:
        d['spatialDivisions'] = ', '.join(uniqify(ListUnion(map(loc.divisions,spaceVals))))
        d['spatialDivisionsTight'] = ', '.join(uniqify(ListUnion(map(loc.divisions2,spaceVals))))
        d['spatialPhrases'] = uniqify(map(loc.phrase,spaceVals))
        d['spatialPhrasesTight'] = uniqify(map(loc.phrase2,spaceVals))
    commonLocation = OverallLocation
    for sv in spaceVals:
        commonLocation = loc.intersect(commonLocation,sv)
        if not commonLocation:
            break 
    if commonLocation:
        d['commonLocation'] = loc.phrase(commonLocation)
        
    Translators =  dict ([(x,functools.partial(commonjs.js_call,translatorContext,collection.totalVariables[int(x)]) if collection.translators.has_key(collection.totalVariables[int(x)]) else None)  if '.' not in x else (None,None) for x in contentColNums])
        
    print '3'    

    d['sliceContents'] = ' '.join(uniqify(ListUnion([ Translate(collection.find(query).distinct(x), Translators[x]) for x in contentColNums])))

    return d
    

def Translate(L,translator):
    return map(translator,L) if translator else L

    
def initialize_argdict(collection):

    d = {} ; ArgDict = {}
    
    sliceCols = uniqify(Flatten(collection.sliceCols))
    sliceColList = ListUnion([[x] if x.split('.')[0] in collection.totalVariables else collection.ColumnGroups.get(x,[]) for x in sliceCols])
    
    if hasattr(collection,'contentCols'):
        contentColList = ListUnion([[x] if x.split('.')[0] in collection.totalVariables else collection.ColumnGroups.get(x,[]) for x in collection.contentCols])
        contentCols = uniqify(contentColList + sliceColList)
    else:
        contentCols = sliceColList
    contentColNums = getStrs(collection,contentCols)
    ArgDict['contentColNums'] = contentColNums
    
    if hasattr(collection,'DateFormat'):
        DateFormat = collection.DateFormat
        ArgDict['OverallDateFormat'] = DateFormat
        timeFormatter = td.mongotimeformatter(DateFormat)
        ArgDict['timeFormatter'] = timeFormatter
    else:
        DateFormat = ''
        
    if hasattr(collection,'OverallDate'):
        od = collection.OverallDate['date']
        odf = collection.OverallDate['format']
        ArgDict['OverallDate'] = od
        OverallDateFormat = odf + DateFormat
        ArgDict['OverallDateFormat'] = OverallDateFormat
        timeFormatter = td.mongotimeformatter(OverallDateFormat)
        ArgDict['timeFormatter'] = timeFormatter

        OD = timeFormatter(OverallDate +'X'*len(DateFormat))
        ArgDict['dateDivisions'] = td.getLowest(OD)
        ArgDict['datePhrases'] = [td.phrase(OD)]
        ArgDict['mindate'] = OD
        ArgDict['maxdate'] = OD             
        
        if DateFormat:
            reverseTimeFormatter = td.reverse(DateFormat)
            ArgDict['reverseTimeFormatter'] = reverseTimeFormatter
            
    else:
        od = ''
                    
    if 'TimeColNames' in collection.ColumnGroups.keys():
        TimeColNamesInd = getNums(collection,collection.ColumnGroups['TimeColNames'])
        tcs = [timeFormatter(od + t) for t in collection.ColumnGroups['TimeColNames']]
        ArgDict['timeColNames'] = tcs 
        ArgDict['timeColNameInds'] = TimeColNamesInd
        ArgDict['timeColNameDivisions'] = [[td.TIME_DIVISIONS[x] for x in td.getLowest(tc)] for tc in tcs] 
        ArgDict['timeColNamePhrases'] = [td.phrase(t) for t in tcs]

    if 'TimeColumns' in collection.ColumnGroups.keys():
        ArgDict['timeColInds'] = getNums(collection,collection.ColumnGroups['TimeColumns'])
            
    #overall location
    if hasattr(collection,'OverallLocation'):
        ol = Collection.OverallLocation
        ArgDict['OverallLocation'] = ol
    else:
        ol = None
        
    #get divisions and phrases from OverallLocation and SpaceColNames
    if 'SpaceColNames' in collection.ColumnGroups.keys():
        spaceColNames = collection.ColumnGroups['SpaceColNames']
        ArgDict['spaceColNames'] = [loc.integrate(ol,x) for x in spaceColNames]

        
    if 'SpaceColumns' in collection.ColumnGroups.keys():
        ArgDict['spaceColInds'] = getNums(collection,collection.ColumnGroups['SpaceColumns'])

    Source = collection.Source
    SourceNameDict = son.SON([(k,Source[k]['Name'] if isinstance(Source[k],dict) else Source[k]) for k in Source.keys()])
    SourceAbbrevDict = dict([(k,Source[k]['ShortName']) for k in Source.keys() if isinstance(Source[k],dict) and 'ShortName' in Source[k].keys() ])
    d['SourceSpec'] = json.dumps(SourceNameDict,default=ju.default)
    d['agency'] = SourceNameDict['Agency']
    d['subagency'] = SourceNameDict['Subagency']
    d['dataset'] = SourceNameDict['Dataset']
    for k in set(SourceNameDict.keys()).difference(['Agency','Subagency','Dataset']):
        d['source_' + str(k).lower()] = SourceNameDict[k]
    for k in SourceAbbrevDict.keys():
        d['source_' + str(k).lower() + '_acronym'] = SourceAbbrevDict[k]
    d['source'] = ' '.join(SourceNameDict.values() + SourceAbbrevDict.values())
        
    if 'Subcollections' in collection.totalVariables:
        ArgDict['subColInd'] = collection.totalVariables.index('Subcollections')
    
    translators = stringifyDictElements(collection.translators)
    ArgDict['translatorContext'] = commonjs.translatorContext(translators)
            
    return d, ArgDict
    

def stringifyDictElements(d):
    
    return dict([(str(k),stringifyDictElements(v) if hasattr(v,'keys') else str(v)) for (k,v) in d.items()])

def getSliceColTuples(collection):
    sliceColList = collection.sliceCols
    sliceColU = uniqify(ListUnion(sliceColList))
    sliceColInds = getStrs(collection,sliceColU)
    OK = dict([(x,x in collection.ColumnGroups.keys() or MoreThanOne(collection,y)) for (y,x) in zip(sliceColInds,sliceColU)])
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
        if n in collection.totalVariables:
            numlist.append(collection.totalVariables.index(n))
        else:
            numlist.append([collection.totalVariables.index(m) for m in collection.ColumnGroups[n]])
    return numlist
    
def getStrs(collection,namelist):
    numlist = []
    for n in namelist:
        ns = n.split('.')
        dot = '.' if len(ns) > 1 else ''
        if ns[0] in collection.totalVariables:
            numlist.append(str(collection.totalVariables.index(ns[0])) + dot + '.'.join(n.split('.')[1:]))
        else:
            numlist.append([str(collection.totalVariables.index(m)) for m in collection.ColumnGroups[n]])
    return numlist
        

def makestr(r,x,translator = None):
    
    v = rgetattr(r,x.split('.'))
    if translator:
        v = translator(v)
    try:
        v = v.encode('utf-8')
    except UnicodeEncodeError:
        return v.decode('latin-1').encode('utf-8')
    else:
        return v
    
        
def coerceToFormat(md,format):
    if format == 'tplist':
        return ', '.join(md)
