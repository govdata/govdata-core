#!/usr/bin/env python

from common.mongo import Collection, cleanCollection, SPECIAL_KEYS
from common.utils import IsFile, listdir, is_string_like, ListUnion, uniqify,createCertificate, rgetattr,rhasattr
import common.timedate as td
import common.location as loc
import common.solr as ourSolr
import backend.api as api
import solr
import itertools
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
    if keys:
        Q1 = api.processArg(dict([(origInd,{'$gt':atVersion}),(vNInd,toVersion)] + [(k,{'$exists':True}) for k in keys]),collection)
        Q2 = api.processArg(dict([(retInd,{'$exists':False}),(vNInd,{'$lt':toVersion,'$gte':atVersion})] + [(k,{'$exists':True}) for k in keys]),collection)
        Q3 = api.processArg(dict([(retInd,True),(vNInd,{'$lt':toVersion,'$gte':atVersion}),(origInd,{'$lte':atVersion})] + [(k,{'$exists':True}) for k in keys]),collection)
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
  
    sliceColTuples = getSliceColTuples(collection)

    d = {} ; ArgDict = {}
    initialize_argdict(d,ArgDict,collection)

    S = ourSolr.query('collectionName:' + collectionName,fl = 'versionNumber',sort='versionNumber desc',wt = 'json')
    existing_slice = ast.literal_eval(S)['response']['docs']
    
    if len(existing_slice) > 0:
        atVersion = existing_slice[0]['versionNumber'][0]
    else:
        atVersion = -1

    solr_interface = solr.SolrConnection("http://localhost:8983/solr")    

    for sliceCols in sliceColTuples:
        Q = getQueryList(collection,sliceCols,atVersion,currentVersion,slicesCorrespondToIndexes)
        for q in Q:
            q = pm.son.SON(q)
            q['__versionNumber__'] = currentVersion
            query = api.processArg(q,collection)
            if collection.find_one(query):
                q.pop('__versionNumber__')       
                print 'Adding:' , query, 'in', collectionName
                sliceCursor = collection.find(query,timeout=False)
                dd = d.copy()
                queryID = [('collectionName',collectionName),('query',q)]
                dd['collectionName'] = collectionName
                dd['query'] = json.dumps(q,default=ju.default)
                dd['mongoID'] = hashlib.sha1(json.dumps(queryID,default=ju.default)).hexdigest()
                dd['mongoText'] = ', '.join([key + '=' + value for (key,value) in q.items()])
                dd['versionNumber'] = currentVersion
                addToIndex(sliceCursor,dd,collection,solr_interface,**ArgDict)  
            else:
                q.pop('__versionNumber__')
                queryID = [('collectionName',collectionName),('query',q)]
                mongoID = hashlib.sha1(json.dumps(queryID,default=ju.default)).hexdigest()
                print 'deleting', mongoID, queryID
                solr_interface.delete_query('mongoID:' + mongoID)
                
        
    solr_interface.commit()
    
    createCertificate(certpath,'Collection ' + collectionName + ' indexed.')        
   
def getSliceColTuples(collection):
    sliceColList = collection.sliceCols
    sliceColU = uniqify(ListUnion(sliceColList))
    OK = dict([(x,x in collection.ColumnGroups.keys() or len(api.get(collection.name,[('distinct',(x,))])['data']) > 1) for x in sliceColU])
    sliceColList = [tuple([x for x in sliceColU if x in sc and OK[x]]) for sc in sliceColList]
    sliceColTuples = uniqify(ListUnion([subTuples(sc) for sc in sliceColList]))
    
    return sliceColTuples
    
def initialize_argdict(d,ArgDict,collection):
    sliceColList = collection.sliceCols
    sliceCols = uniqify(ListUnion(sliceColList))
    if hasattr(collection,'contentCols'):
        contentCols = collection.contentCols
    else:
        contentCols = sliceCols
    contentColNums = getStrs(collection,contentCols)
    ArgDict['contentColNums'] = contentColNums
    if hasattr(collection,'phraseCols'):
        phraseCols = collection.phraseCols
    else:
        phraseCols = contentCols
    phraseColNums = getStrs(collection,phraseCols)
    ArgDict['phraseCols'] = phraseCols
    ArgDict['phraseColNums'] = phraseColNums
    
    if hasattr(collection,'DateFormat'):
        DateFormat = collection.DateFormat
        ArgDict['DateFormat'] = DateFormat
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
        ArgDict['timeCols'] = tcs 
        ArgDict['timeColNameInds'] = TimeColNamesInd
        ArgDict['timeColNameDivisions'] = [[td.TIME_DIVISIONS[x] for x in td.getLowest(tc)] for tc in tcs] 
        ArgDict['timeColNamePhrases'] = [td.phrase(t) for t in tcs]

    if 'TimeColumns' in collection.ColumnGroups.keys():
        ArgDict['timeColInds'] = getNums(collection,collection.ColumnGroups['TimeColumns'])
            
    #overall location
    if hasattr(collection,'OverallLocation'):
        ol = Collection.OverallLocation
        ArgDict['OverallLocation'] = ol
        ArgDict['spatialDivisions'] = loc.divisions(ol)
        ArgDict['spatialPhrases'] = [loc.phrase(ol)]
    else:
        ol = None
        
    #get divisions and phrases from OverallLocation and SpaceColNames
    if 'SpaceColNames' in collection.ColumnGroups.keys():
        spaceColNames = collection.ColumnGroups['SpaceColNames']
        ArgDict['spaceColNames'] = [loc.integrate(ol,x) for x in spaceColNames]
        ArgDict['spaceColNameInds'] = getNums(collection,spaceColNames)
        ArgDict['spaceColNameDivisions'] = [loc.divisions(x) for x in SpaceColNames]
        ArgDict['spaceColNamePhrases'] = [loc.phrase(x) for x in SpaceColNames]
        
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
        

def makestr(x):
    v = rgetattr(r,x.split('.'))
    try:
        v = x.encode('utf-8')
    except UnicodeEncodeError:
        return x.decode('latin-1').encode('utf-8')
    else:
        return v
    
        
    
def addToIndex(R,d,collection,solr_interface,contentColNums = None, phraseCols = None, phraseColNums = None,DateFormat = None,timeColNameInds = None,timeColNameDivisions = None,timeColNamePhrases=None,timeColInds=None,timeCols=None,subColInd = None,OverallDate = '', OverallDateFormat = '', timeFormatter = None,reverseTimeFormatter = None,dateDivisions=None,datePhrases=None,mindate = None,maxdate = None,OverallLocation = None, spatialDivisions=None, spatialPhrases=None,spaceColNames = None, spaceColNameInds = None, spaceColNameDivisions = None, spaceColNamePhrases = None, spaceColInds = None):
        
    d['sliceContents'] = []
    d['slicePhrases'] = []
    colnames = []
    d['volume'] = 0
    Subcollections = []
    if dateDivisions == None:
        dateDivisions = []
    else:
        dateDivisions = dateDivisions[:]
    if datePhrases == None:
        datePhrases = []
    else:
        datePhrases = datePhrases[:]
    if spatialDivisions == None:
        spatialDivisions = []
    else:
        spatialDivisions = spatialDivisions[:]
    if spatialPhrases == None:
        spatialPhrases = []
    else:
        spatialPhrases = spatialPhrases[:]
    commonLocation = OverallLocation    
        
    
    for (i,r) in enumerate(R):

        if i/10000 == i/float(10000):
            print '. . . at', i
                
        d['sliceContents'].append( ' '.join(ListUnion([([str(rgetattr(r,x.split('.')))] if rhasattr(r,x.split('.')) else []) if is_string_like(x) else [str(rgetattr(r,xx.split('.'))) for xx in x if rhasattr(r,xx.split('.'))] for x in contentColNums])))
        
        sP = ListUnion([([s + ':' + str(rgetattr(r,x.split('.')))] if rhasattr(r,x.split('.')) else []) if is_string_like(x) else [s + ':' + str(rgetattr(r,xx.split('.'))) for xx in x if rhasattr(r,xx.split('.'))] for (s,x) in zip(phraseCols,phraseColNums)])
        for ssP in sP:
            if ssP not in d['slicePhrases']:
                d['slicePhrases'].append(ssP)
        
        colnames  = uniqify(colnames + r.keys())
        d['volume'] += 1
        
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
                    commonLocation = loc.intersect(commonLocation,r[str(x)]) if commonLocation != None else r[str(x)]
                    spatialDivisions += loc.divisions(location)
                    spatialPhrases.append(loc.phrase(location))
                   
    d['sliceContents'] = ' '.join(d['sliceContents'])
    Subcollections = uniqify(Subcollections)
    d['columnNames'] = [collection.totalVariables[int(x)] for x in colnames if x.isdigit()]
    d['dimension'] = len(d['columnNames'])
    #time/date
        
    if OverallDateFormat:
        d['dateFormat'] = OverallDateFormat
        
        if 'TimeColNames' in collection.ColumnGroups.keys():
            K = [k for (k,j) in enumerate(timeColNameInds) if str(j) in colnames]
            dateDivisions += uniqify(ListUnion([timeColNameDivisions[k] for k in K]))
            mindate = td.makemin(mindate,min([timeCols[k] for k in K]))
            maxdate = td.makemax(maxdate,max([timeCols[k] for k in K]))         
            datePhrases += uniqify([timeColNamePhrases[k] for k in K])
        
        d['begin_date'] = td.convertToDT(mindate)
        d['end_date'] = td.convertToDT(maxdate,convertMode='High')
        d['dateDivisions'] = ' '.join(uniqify(dateDivisions))
        d['datePhrases'] = '|||'.join(datePhrases)

    
    if 'SpaceColNames' in collection.ColumnGroups.keys():
        K = [k for (k,j) in enumerate(spaceColNameInds) if str(j) in colnames]
        spatialDivisions += uniqify(ListUnion([spaceColNameDivisions[k] for k in K]))
        spatialPhrases += uniqify([spaceColPhrases[k] for k in K])  
        for k in K:
            commonLocation = spaceColNames[k] if commonLocation == None else loc.intersect(commonLocation,spaceColNames[k])

        
    if spatialDivisions:
        d['spatialDivisions'] = '|||'.join(uniqify(spatialDivisions))
        d['spatialPhrases'] = '|||'.join(spatialPhrases)
        if commonLocation:
            d['commonLocation'] = loc.phrase(commonLocation)
    
    
    #metadata
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
    
    
    solr_interface.add(**d)

def coerceToFormat(md,format):
    if format == 'tplist':
        return '|||'.join(md)