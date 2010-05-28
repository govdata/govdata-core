#!/usr/bin/env python

from common.mongo import Collection, cleanCollection, SPECIAL_KEYS
from common.utils import IsFile, listdir, is_string_like, ListUnion, uniqify,createCertificate, rgetattr,rhasattr, dictListUniqify
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
  
    sliceColTuples = getSliceColTuples(collection)

    d,ArgDict = initialize_argdict(collection)

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
                print 'Adding:' , q, 'in', collectionName
                dd = d.copy()
                queryID = [('collectionName',collectionName),('query',q)]
                dd['collectionName'] = collectionName
                dd['query'] = json.dumps(q,default=ju.default)
                dd['mongoID'] = hashlib.sha1(json.dumps(queryID,default=ju.default)).hexdigest()
                dd['mongoText'] = ', '.join([key + '=' + value for (key,value) in q.items()])
                dd['versionNumber'] = currentVersion
                addToIndex(query,dd,collection,solr_interface,**ArgDict)  
            else:
                q.pop('__versionNumber__')
                queryID = [('collectionName',collectionName),('query',q)]
                mongoID = hashlib.sha1(json.dumps(queryID,default=ju.default)).hexdigest()
                print 'deleting', mongoID, queryID
                solr_interface.delete_query('mongoID:' + mongoID)
                
        
    solr_interface.commit()
    
    createCertificate(certpath,'Collection ' + collectionName + ' indexed.')        


def addToIndex(query,d,collection,solr_interface,contentColNums = None, phraseColNums = None, phraseCols = None, timeColInds=None,timeColNames=None, timeColNameInds = None,timeColNameDivisions = None,timeColNamePhrases=None,OverallDate = '', OverallDateFormat = '', timeFormatter = None,reverseTimeFormatter = None,dateDivisions=None,datePhrases=None,mindate = None,maxdate = None,OverallLocation = None, spaceColNames = None, spaceColInds = None,subColInd = None):

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
        smallAdd(d,query,collection,contentColNums, phraseColNums, phraseCols , timeColInds ,timeColNames , timeColNameInds ,timeColNameDivisions ,timeColNamePhrases ,OverallDate , OverallDateFormat, timeFormatter ,reverseTimeFormatter ,dateDivisions ,datePhrases ,mindate ,maxdate ,OverallLocation , spaceColNames , spaceColInds ,subColInd )
    else:
        largeAdd(d,query,collection,contentColNums, phraseColNums, phraseCols , timeColInds ,timeColNames , timeColNameInds ,timeColNameDivisions ,timeColNamePhrases ,OverallDate, OverallDateFormat, timeFormatter ,reverseTimeFormatter ,dateDivisions ,datePhrases ,mindate ,maxdate ,OverallLocation , spaceColNames , spaceColInds ,subColInd)

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
    
    #return d
    solr_interface.add(**d)
   
    
def smallAdd(d,query,collection,contentColNums, phraseColNums, phraseCols , timeColInds ,timeColNames , timeColNameInds ,timeColNameDivisions ,timeColNamePhrases ,OverallDate, OverallDateFormat, timeFormatter ,reverseTimeFormatter ,dateDivisions ,datePhrases ,mindate ,maxdate ,OverallLocation , spaceColNames , spaceColInds ,subColInd ):

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

    
    for (i,r) in enumerate(R):
    
        d['sliceContents'].append( ' '.join(ListUnion([([makestr(r,x)] if rhasattr(r,x.split('.')) else []) if is_string_like(x) else [makestr(r,xx) for xx in x if rhasattr(r,xx.split('.'))] for x in contentColNums])))
        
        sP = ListUnion([([s + ':' + makestr(r,x)] if rhasattr(r,x.split('.')) else []) if is_string_like(x) else [s + ':' + makestr(r,xx) for xx in x if rhasattr(r,xx.split('.'))] for (s,x) in zip(phraseCols,phraseColNums)])
        for ssP in sP:
            if ssP not in d['slicePhrases']:
                d['slicePhrases'].append(ssP)
        
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
        d['spatialPhrases'] = uniqify(map(loc.phrase,spaceVals))
        d['spatialPhrasesTight'] = uniqify(map(loc.phrase2,spaceVals))
        
    return d
    
def largeAdd(d,query,collection,contentColNums, phraseColNums, phraseCols , timeColInds ,timeColNames , timeColNameInds ,timeColNameDivisions ,timeColNamePhrases ,OverallDate , OverallDateFormat, timeFormatter ,reverseTimeFormatter ,dateDivisions ,datePhrases ,mindate ,maxdate ,OverallLocation , spaceColNames , spaceColInds ,subColInd ):

   
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
      
    if spaceColInds:
        spaceColVals = ListUnion([collection.find(query).distinct(str(t)) for t in spaceColInds if t in colnums])
        spaceColVals = [loc.integrate(OverallLocation,scv) for scv in spaceColVals]   
    else:
        spaceColVals = []
    spaceVals = spaceColNames + spaceColVals 
    if spaceVals:
        d['spatialDivisions'] = ', '.join(uniqify(ListUnion(map(loc.divisions,spaceVals))))
        d['spatialPhrases'] = uniqify(map(loc.phrase,spaceVals))
        d['spatialPhrasesTight'] = uniqify(map(loc.phrase2,spaceVals))
    commonLocation = OverallLocation
    for sv in spaceVals:
        commonLocation = loc.intersect(commonLocation,sv)
        if not commonLocation:
            break 
    if commonLocation:
        d['commonLocation'] = loc.phrase(commonLocation)
 
        
    contents = dict([(x,collection.find(query).distinct(x)) for x in uniqify(contentColNums + phraseColNums)])

    d['sliceContents'] = ' '.join(uniqify(ListUnion([contents[x] for x in contentColNums])))
    d['slicePhrases'] = ', '.join(ListUnion([[y + '=' + xx for xx in contents[x]] for (x,y) in zip(phraseColNums,phraseCols)]))

    return d
    
def initialize_argdict(collection):

    d = {} ; ArgDict = {}
    
    sliceCols = uniqify(ListUnion(collection.sliceCols))
    sliceColList = ListUnion([[x] if x.split('.')[0] in collection.totalVariables else collection.ColumnGroups.get(x,[]) for x in sliceCols])
    
    if hasattr(collection,'contentCols'):
        contentColList = ListUnion([[x] if x.split('.')[0] in collection.totalVariables else collection.ColumnGroups.get(x,[]) for x in collection.contentCols])
        contentCols = uniqify(contentColList + sliceColList)
    else:
        contentCols = sliceColList
    contentColNums = getStrs(collection,contentCols)
    ArgDict['contentColNums'] = contentColNums
    if hasattr(collection,'phraseCols'):
        phraseColList = ListUnion([[x] if x.split('.')[0] in collection.totalVariables else collection.ColumnGroups.get(x,[]) for x in collection.phraseCols])
        phraseCols = list(set(phraseColList).difference(sliceColList))
    else:
        phraseCols = []
    phraseColNums = getStrs(collection,phraseCols)
    ArgDict['phraseCols'] = phraseCols
    ArgDict['phraseColNums'] = phraseColNums
    
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
        
        
    return d, ArgDict

   
def getSliceColTuples(collection):
    sliceColList = collection.sliceCols
    sliceColU = uniqify(ListUnion(sliceColList))
    OK = dict([(x,x in collection.ColumnGroups.keys() or len(api.get(collection.name,[('distinct',(x,))])['data']) > 1) for x in sliceColU])
    sliceColList = [tuple([x for x in sliceColU if x in sc and OK[x]]) for sc in sliceColList]
    sliceColTuples = uniqify(ListUnion([subTuples(sc) for sc in sliceColList]))
    
    return sliceColTuples
    
    
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
        

def makestr(r,x):
    v = rgetattr(r,x.split('.'))
    try:
        v = v.encode('utf-8')
    except UnicodeEncodeError:
        return v.decode('latin-1').encode('utf-8')
    else:
        return v
    
        
def coerceToFormat(md,format):
    if format == 'tplist':
        return ', '.join(md)