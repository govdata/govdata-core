"""This module sets initializes directory structure at high level and has some very basic common parsing utilities. 
"""

import numpy as np
import tabular as tb
import backend.indexing as indexing
from System.Protocols import ApplyOperations2, activate
from System.Utils import MakeDir,MakeDirs, PathExists, RecursiveFileList
import os
import hashlib
import pymongo as pm
import gridfs as gfs
import cPickle as pickle
import tabular as tb
from common.utils import IsFile, listdir, is_string_like, ListUnion,createCertificate, uniqify, IsDir, Flatten
from common.mongo import cleanCollection, SPECIAL_KEYS, Collection
import common.timedate as td
import common.location as loc
import re
isnan = np.isnan

root = '../Data/OpenGovernment/'
protocolroot = '../Protocol_Instances/OpenGovernment/'
CERT_ROOT = root + 'Certificates/'
CERT_PROTOCOL_ROOT = '../Protocol_Instances/OpenGovernment/Certificates/'
MONGOSOURCES_PATH = '../Data/OpenGovernment/MongoSources/'
DOWNLOAD_ROOT = '../Data/OpenGovernment/Downloads/'

def initialize(creates = protocolroot):
    MakeDir(protocolroot)
    
def initialize_downloads(creates = DOWNLOAD_ROOT):
    MakeDir(creates)    

def initialize_mongosources(creates = root + 'MongoSources/'):
    MakeDir(creates)
    
def initialize_backendCertificates(creates = CERT_ROOT):
    MakeDir(creates)

def initialize_cert_protocol_root(creates = CERT_PROTOCOL_ROOT):
    MakeDir(creates)

def filldown(x):
    y = np.array([xx.strip() for xx in x])
    nz = np.append((y != '').nonzero()[0],[len(y)])
    return y[nz[:-1]].repeat(nz[1:] - nz[:-1])

    
def gethierarchy(x,f,postprocessor = None):
    hl = np.array([f(xx) for xx in x])
    # normalize 
    ind = np.concatenate([(hl == min(hl)).nonzero()[0], np.array([len(hl)])])
    if ind[0] != 0:
        ind = np.concatenate([np.array([0]), ind])  
    hl2 = []
    for i in range(len(ind)-1):
        hls = hl[ind[i]:ind[i+1]].copy()
        hls.sort()
        hls = tb.utils.uniqify(hls)
        D = dict(zip(hls, range(len(hls))))
        hl2 += [D[h] for h in hl[ind[i]:ind[i+1]]]

    hl = np.array(hl2)
    m = max(hl)
    cols = []
    for v in range(m+1):
        vxo = hl < v
        vx = hl == v
        if vx.any():
            nzv = np.append(vx.nonzero()[0],[len(x)])
            col = np.append(['']*nzv[0],x[nzv[:-1]].repeat(nzv[1:] - nzv[:-1]))
            col[vxo] = ''
            cols.append(col)
        else:
            cols.append(np.array(['']*len(x)))
    
    if postprocessor:
        for i in range(len(cols)):
            cols[i] = np.array([postprocessor(y) for y in cols[i]])
            
    return [cols,hl]
 
            

def notEqual(x,y):
    """
        Helper for processRecord
    """
    return x != y and not (isnan(x) and isnan(y))
    
def getT(x):
    """
        Helper for createCollection and updateCollection
    """
    
    return tuple([(k,v) for (k,v) in x.items() if k != 'f'])
    
 
 
 
 #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def backendProtocol(collectionName,parser,downloader = None, downloadProtocol= None,downloadArgs = None, downloadKwargs = None, parserArgs = None, parserKwargs = None, trigger = None, certdir = None, createCertDir = False, downloadPath = None , createPath = None, indexPath = None, slicesCorrespondToIndexes=True, write = True,ID = None,incremental = False,uptostep=None):
    if ID == None:
        ID = collectionName
    if ID and not ID.endswith('_'):
        ID += '_'
    
    outdir = CERT_PROTOCOL_ROOT + collectionName  + '/'
    
    if not PathExists(outdir):
        MakeDir(outdir)


    StepList = []
    
    if certdir == None and any([x == None for x in [downloadPath,createPath,indexPath]]):
        certdir = CERT_ROOT
        
    if certdir:
        if createCertDir:
            StepList += [(ID + 'initialize',MakeDir,(certdir,))]
        if downloadPath == None:
            downloadPath = certdir + ID + 'downloadCertificate.txt'
        if createPath == None:
            createPath = certdir + ID + 'createCertificate.txt'
        if indexPath == None:
            indexPath = certdir + ID + 'indexCertificates/'   
    

    if downloader:
        if isinstance(downloader,list):
            if downloadArgs == None:
                downloadArgs = [()]*len(downloader)
            if downloadKwargs == None:
                downloadKwargs = [{}]*len(downloader)
            downloadStepsGen = lambda DIR,T : [(ID + 'download_'  + n +  ('_' + T if T else ''),d,[(DIR ,) + a,b]) for ((d,n),a,b) in zip(downloader,downloadArgs,downloadKwargs)]   
        else:
            assert hasattr(downloader,'__call__')
            if downloadArgs == None:
                downloadArgs = ()
            if downloadKwargs == None:
                downloadKwargs = {}
            downloadStepsGen = lambda DIR,T : [(ID + 'download' + ('_' + T if T else '') ,downloader,[(DIR,) + downloadArgs,downloadKwargs])]
    elif downloadProtocol:
        if downloadArgs == None:
            downloadArgs = ()
        if downloadKwargs == None:
            downloadKwargs = {}
        downloadStepsGen = lambda DIR,T : downloadProtocol(DIR,T,*downloadArgs,**downloadKwargs)

    download_root = DOWNLOAD_ROOT + collectionName + '/'
    
    T = trigger() if trigger else ''
        
    if incremental:
        if PathExists(outdir + 'manifest.tsv'):
            M = tb.tabarray(SVfile = outdir + 'manifest.tsv')
            newinc = max(M['increment']) + 1
            if T:
                if T in M['trigger']:
                    print 'It appears the increment, ' +  T + ', has already run, not adding.'
                else:
                    print 'Adding new increment', T
                    M = M.addrecords((newinc,T))
        else:
            M = tb.tabarray(records = [(0,T)],names = ['increment','trigger'],formats='int,str')
                         
    else:
        M = tb.tabarray(records = [(-1,T)],names = ['increment','trigger'],formats='int,str')
    M.saveSV(outdir + 'manifest.tsv')
    
    StepList += ListUnion([downloadStepsGen(increment_format(download_root, m['increment']),m['trigger']) for m in M])
    
    StepList += [(ID + 'download_check',download_check,(download_root,incremental,downloadPath))]
        
    StepList += [(ID + 'updateCollection',updateCollection,[(download_root,collectionName,parser,downloadPath,createPath),{'parserArgs':parserArgs,'parserKwargs':parserKwargs,'incremental':incremental}]),
    (ID + 'updateCollectionIndex',indexing.updateCollectionIndex,(collectionName,createPath,indexPath),{'slicesCorrespondToIndexes':slicesCorrespondToIndexes})]

    if uptostep:
        for (i,d) in enumerate(StepList):
            if d[0] == ID + uptostep:
                StepList = StepList[:i+1]
                break

    if write:
        outfile = outdir + 'steps.py'
        ApplyOperations2(outfile,StepList)

        
def get_and_check_increments(download_dir):
    X = re.compile('__[\d]+__$')
    L = [x for x in listdir(download_dir) if not x.startswith('.')]
    assert all([X.match(x) for x in L])
    return [int(x[2:-2]) for x in L]
  
def get_increment_paths(download_dir):
    increments = get_and_check_increments(download_dir)
    return [increment_format(download_dir,i) for i in increments]
  
def increment_format(download_dir,i):
    assert isinstance(i,int), 'i must be integer'
    if i >= 0:
        return download_dir + '__' + str(i) + '__/'
    else:
        return download_dir
  
def get_max_increment(download_dir):  
    P = get_and_check_increments(download_dir)
    if P:
        return max(P)
    else:
        return -1
    
def get_next_increment_path(download_dir):
    return  increment_format(download_dir,get_max_increment(download_dir)+1)
    
def get_max_increment_path(download_dir):
    return increment_format(download_dir,get_max_increment(download_dir))

def get_and_check_increments_fromDB(versions):
    V = versions.find(fields=['startIncrement','endIncrement']).sort('versionNumber',pm.ASCENDING)
    V = [(v['startIncrement'],v['endIncrement']) for v in V]
    (S,E) = zip(*V)
    assert all([S[i] < S[i+1] and E[i] == S[i+1] - 1 for i in range(len(S)-1)]) and all([s <= e for (s,e) in V ]), 'Something\'s wrong with version history increments.'
    
    return (S,E)
    
def get_max_increment_fromDB(versions):
    (S,E) = get_and_check_increments_fromDB(versions)
    return max(E)
    
@activate(lambda x : x[0],lambda x : x[2])
def download_check(download_dir, incremental, certpath):

    if not incremental: 
        check_list = [download_dir ]
    else:
        check_list = get_increment_paths(download_dir)
       

    assert all(['__PARSE__' in listdir(p) for p in check_list])
            
    createCertificate(certpath,'Collection properly downloaded and pre-parsed.')


class dataIterator():

    def __iter__(self):
        return self
        
    def __getattr__(self,attr):
    
        try:
            V = self.metadata[''][attr]
        except KeyError:
            raise AttributeError, "Can't find attribute " + attr
        else:
            return V        
            
            
class csv_parser(dataIterator):

    def __init__(self,source):
        self.metadata = pickle.load(open(source + '__metadata.pickle'))
        
    def refresh(self,file):
        print 'refreshing', file
        self.Data = tb.tabarray(SVfile = file,verbosity = 0)
        self.IND = 0
    
    def next(self):
        if self.IND < len(self.Data):
            r = self.Data[self.IND]
            r = pm.son.SON([(self.Data.dtype.names[i],float(xx) if isinstance(xx,float) else int(xx) if isinstance(xx,int) else xx) for (i,xx) in enumerate(r) if xx != ''])
            
            if 'subcollections' in r.keys():
                r['subcollections'] = r['subcollections'].split(',')
                
            for k in self.ColumnGroups.get('timeColumns',[]) + self.ColumnGroups.get('spaceColumns',[]):
                if k in r.keys():
                    r[k] = eval(r[k])               
            
            self.IND += 1
                
            return r
            
        else:
            raise StopIteration
    
    

@activate(lambda x :  (x[0] + '/',x[3]),lambda x : x[4])
def updateCollection(download_dir,collectionName,parserClass,checkpath,certpath,parserArgs=None,parserKwargs=None,incremental=False):
    
    connection =  pm.Connection(document_class=pm.son.SON)
    db = connection['govdata']
    assert not '__' in collectionName, 'collectionName must not contain consecutive underscores'
    metaCollectionName = '__' + collectionName + '__'
    versionName = '__' + collectionName + '__VERSIONS__'
    sliceDBName =  '__' + collectionName + '__SLICES__'
    
    collection = db[collectionName]
    metacollection = db[metaCollectionName]
    versions = db[versionName]     
    sliceDB = db[sliceDBName]
            
    if incremental:     
        if versionName not in db.collection_names():
            startInc = 0
        else:
            startInc = get_max_increment_fromDB(versions) + 1
        endInc = get_max_increment(download_dir)
        sources = [increment_format(download_dir,i) for i in range(startInc,endInc + 1)]
    else:
        sources = [download_dir]
        startInc = endInc = None
        
    if parserArgs == None:
        parserArgs = ()
    if parserKwargs == None:
        parserKwargs = {}
        
        
    if sources:
        iterator = parserClass(sources[0],*parserArgs,**parserKwargs)
    
        assert hasattr(iterator,'uniqueIndexes'),  'No unique indexes specified'
        uniqueIndexes = iterator.uniqueIndexes
        ColumnGroups = iterator.columnGroups
        
        sliceColTuples = getSliceColTuples(iterator.sliceCols)
        sliceColTuplesFlat = uniqify([tuple(sorted(uniqify(Flatten(sct)))) for sct in sliceColTuples])
      
        sliceColList = uniqify(Flatten(ListUnion(sliceColTuples)))
        ContentCols = set(sliceColList + getContentCols(iterator))
            
        if hasattr(iterator,'dateFormat'):
            TimeFormatter = td.mongotimeformatter(iterator.dateFormat)
            
    
        if collectionName in db.collection_names():
            versionNumber = max(versions.distinct('versionNumber')) + 1
            storedAllMetadata = metacollection.find_one({'name':'','versionNumber':versionNumber-1})
            totalVariables = storedAllMetadata['columns']
            VarMap = dict(zip(totalVariables,[str(x) for x in range(len(totalVariables))]))   
            
            #check things are the same 
            #and check consistent  do so for all soruces
            
        else:
            versionNumber = 0
            IndexCols = uniqify([x for x in ['subcollections'] + sliceColList + ListUnion([ColGroupsFlatten(ColumnGroups,k) for k in ['indexColumns','labelColumns','timeColumns','spaceColumns']]) if x not in uniqueIndexes])
            
            totalVariables = SPECIAL_KEYS + uniqueIndexes + IndexCols
            
            assert not any(['.' in x or ('__' in x and x not in SPECIAL_KEYS) or x in ColumnGroups.keys() for x in totalVariables])
            
            VarMap = dict(zip(totalVariables,map(str,range(len(totalVariables)))))  
            
            cols = zip([VarMap[c] for c in uniqueIndexes + ['__versionNumber__']],[pm.DESCENDING]*(len(uniqueIndexes) + 1))
            collection.ensure_index(cols,unique=True,dropDups=True)
    
            for col in IndexCols:
                collection.ensure_index(VarMap[col])
            
            sliceDB.ensure_index('slice',unique=True,dropDups=True)
                
                
        vNInd = VarMap['__versionNumber__']
        retInd = VarMap['__retained__']
        
        specialKeyInds = [VarMap[k] for k in SPECIAL_KEYS]
    
        if 'timeColumns' in iterator.ColumnGroups.keys():
            tcs = iterator.columnGroups['timeColumns']
        else:
            tcs = []
        
        if 'spaceColumns' in iterator.ColumnGroups.keys():
            spcs = iterator.columnGroups['spaceColumns']
        else:
            spcs = []
                  
        toParse = ListUnion([RecursiveFileList(source + '__PARSE__') for source in sources])
            
        oldc = None
        SpaceCache = {}    
        for file in toParse:
            iterator.refresh(file) 
            tcs = iterator.columnGroups.get('timeColumns',[])
            spcs = iterator.columnGroups.get('spaceColumns',[])
            index = 0
            for c in iterator: 
                newVars = [x for x in c.keys() if not x in totalVariables]
                assert not any (['__' in x or '.' in x or x in ColumnGroups.keys() for x in newVars]) , '__ and . must not appear in key names.'     
                totalVariables += newVars
                VarMap.update(dict(zip(newVars,map(str,range(len(totalVariables) - len(newVars),len(totalVariables))))))
                
                for tc in tcs:   #time handling 
                    if tc in c.keys():
                        c[tc] = TimeFormatter(c[tc])
                if completeSpace:        
                    for spc in spcs:
                        if spc in c.keys():   #space
                            t = getT(c[spc])
                            if t in SpaceCache.keys():
                                c[spc] = SpaceCache[t].copy()
                            else:
                                c[spc] = loc.SpaceComplete(c[spc])
                                SpaceCache[t] = c[spc].copy()      
                if index % 100 == 0:
                    print 'At', index
                index += 1
                sctf = processSct(sliceColTuplesFlat,oldc,c)
                processRecord(c,collection,VarMap,totalVariables,uniqueIndexes,versionNumber,specialKeyInds,incremental,sliceDB,sctf,ContentCols)
                oldc = c
                
        if incremental:
            collection.update({vNInd:versionNumber - 1, retInd : {'$exists':False}}, {'$set':{vNInd:versionNumber}})                    
            sliceDB.update({},{'$set':{'version':versionNumber}})
    
        updateMetacollection(iterator, collection, metacollection,incremental,versionNumber,totalVariables,tcs,spcs)
        
        updateAssociatedFiles(sources,collection)
        
        updateVersionHistory(versionNumber,versions,startInc,endInc)
    
    connection.disconnect()
    createCertificate(certpath,'Collection ' + collectionName + ' written to DB.')


def processSct(sct,oldc,c):
    if oldc == None:
        return sct
    else:
        keep = []
        for (i,s) in enumerate(sct):
            if any([oldc.get(k) != c.get(k) for k in s]):
                keep.append(i)
        return [sct[i] for i in keep]


def ColGroupsFlatten(ColumnGroups,k):
    return Flatten([x if x not in ColumnGroups else ColGroupsFlatten(ColumnGroups,x) for x in ColumnGroups.get(k,[])])
    

def getContentCols(iterator):

    if hasattr(iterator,'contentCols'):
        contentColList = ListUnion([iterator.ColumnGroups.get(x,[x]) for x in iterator.contentCols])
    else:
        contentColList =  []
    
    if hasattr(iterator,'phraseCols'):
        phraseColList = ListUnion([iterator.ColumnGroups.get(x,[x]) for x in iterator.phraseCols])
    else:
        phraseColList = []
    
    return contentColList + phraseColList
    
    
def getSliceColTuples(sliceCols):
    sliceColList = sliceCols
    sliceColTuples = uniqify(ListUnion([subTuples(tuple(sc)) for sc in sliceColList]))
    return sliceColTuples
    
    
import itertools   
def subTuples(T):
    ind = itertools.product(*[[0,1]]*len(T))
    return [tuple([t for (t,k) in zip(T,I) if k]) for I in ind]
    

def updateVersionHistory(versionNumber, versions,startInc,endInc):

    ts = td.Now()
    if startInc != None and endInc != None:
        newVersion = {'versionNumber': versionNumber, 'timeStamp':ts, 'startIncrement':startInc, 'endIncrement':endInc}
    else:
        newVersion = {'versionNumber': versionNumber, 'timeStamp':ts}
    versions.insert(newVersion)
                    

def updateAssociatedFiles(sources,collection):
    for source in sources:
        if IsDir(source + '__FILES__'):
            G = gfs.GridFS(collection.database,collection=collection.name)
            for file in listdir(source + '__FILES__'):
                os.environ['PROTECTION'] = 'OFF'
                S = open(source + '__FILES__/' + file,'r').read()
                os.environ['PROTECTION'] = 'ON'
                G.put(S,filename = file)   
                

def updateMetacollection(iterator, collection, metacollection,incremental,versionNumber,totalVariables,tcs,spcs):
    
    metadata = iterator.metadata
    
    metadata['']['columns'] = totalVariables
    metadata['']['source'] = pm.son.SON(metadata['']['source'])
    
    metadata['']['title'] = metadata['']['source']['dataset']['name']
     metadata['']['shortTitle'] = metadata['']['source']['dataset']['shortName']
    
    value_processors = metadata[''].get('valueProcessors',{})
    if metadata['']['columnGroups'].get('timeColumns',None):
        value_processors['timeColumns'] = 'return require("timedate").phrase(value);'
    if metadata['']['columnGroups'].get('spaceColumns',None):
        value_processors['spaceColumns'] =  'return require("location").phrase(value);'
    metadata['']['valueProcessors'] = value_processors    
    
    name_processors = metadata[''].get('nameProcessors',{})
    if metadata['']['columnGroups'].get('timeColNames',None):
        dateFormat = metadata['']['dateFormat']
        name_processors['timeColNames'] = 'return require("timedate").phrase(require("timedate").stringtomongo(value,"' + dateFormat + '"));'
    metadata['']['nameProcessors'] = name_processors  

    metacollection.ensure_index([('name',pm.DESCENDING),('versionNumber',pm.DESCENDING)], unique=True)   
    
    #times 
    getCommonDatesLocations(metadata,iterator,collection,totalVariables.versionNumber)
    
    if incremental:
        previousMetadata = dict([(p["name"],p) for  p in metacollection.find({'versionNumber':versionNumber - 1})])
        if previousMetadata:
            for x in previousMetadata.values():
                x.pop('_id')
            
            for k in previousMetadata.keys():
                if k not in metadata.keys():
                    metadata[k] = previousMetadata[k]
                    
            for k in previousMetadata[''].keys():
                if k not in metadata[''].keys():
                    metadata[''][k] = previousMetadata[''][k]
            
            for k in previousMetadata['']['columnGroups'].keys():
                if k not in metadata['']['columnGroups'].keys():
                    metadata['']['columnGroups'][k] = previousMetadata['']['columnGroups'][k]
                else:
                    metadata['']['columnGroups'][k] += previousMetadata['']['columnGroups'][k] 
                                

    for k in metadata.keys():

        x = metadata[k]
        x['name'] = k
        x['versionNumber'] = versionNumber
        id = metacollection.insert(x,safe=True)
            

def getCommonDatesLocations(iterator,collection,totalVariables,versionNumber):
    vNInd = '0'
    overallDateFormat = iterator.overallDateFormat if hasattr(iterator,'overallDateFormat') else ''
    dateFormat = iterator.dateFormat if hasattr(iterator,'dateFormat') else ''
    overallDate = iterator.overallDate if hasattr(iterator,'overallDate') else ''
    if overallDateFormat or dateFormat:
		DF = overallDateFormat + dateFormat

		F = td.mongotimeformatter(DF)
		T1 = [F(overallDate + x for x) in iterator.columnGroups['timeColNames']]
		T2 = ListUnion([[x for x in collection.find({vNInd:versionNumber}).distinct(totalVariables.index(c))] for c in tcs])
        if overallDateFormat:
            reverseF = td.reverse(dateFormat)
            T2 = [F(overallDate + y) for y in map(reverseF,T2)]
		mindate = min(T1 + T2)
		maxdate = max(T1 + T2)
		divisions = uniqify(ListUnion([td.getLowest(t) for t in T1 + T2]))
		metadata['']['beginDate'] = mindate
		metadata['']['endDate'] = maxdate
		metadata['']['dateDivisions'] = divisions
    #locations
    if spcs:
        locs = ListUnion([[x for x in collection.find({vNInd:versionNumber}).distinct(totalVariables.index(c))] for c in spcs])
        locs = dictUniqify([loc.integrate(interator.overallLocation,l) for l in locs])
        metadata['']['spatialDivisions'] = uniqify(ListUnion([loc.divisions((x) for x in in locs]))
        metadata['']['commonLocation'] = reduce(loc.intersect,locs)
       

def processRecord(c,collection,VarMap,totalVariables,uniqueIndexes,versionNumber,specialKeyInds,incremental,sliceDB,sliceColTuples,ContentCols):
    """Function which adds a given record to a collection, handling the incremental version properly.  
    
        The basic logic is:  to add a given record 'c':
            -- add __versionNumber__ key to 'c',  with value = currentVersion
            -- see if there is a corresponding record in the previous version
                if so: 
                    add __originalVersion__ key to 'c',  with the same value as corresponding old record's __originalVersion__ 
                    compute the differences between the new record and old corresponding record, including:
                        -- added keys
                        -- removed keys
                        -- differing values
                    if there are differences:
                        add the __retained__ key to the OLD corresponding record, and, to save room, remove any non-differeing values from it (except for the uniqueIndex values)
                    else:
                        set _id key of 'c'  that of old record
                        remove the old record from the collection
                    
                if not:  
                    just set __originalVersion__ and __versionNumber__ keys to  equal currentVersion
                
             -- finally,  insert the record
          
    """

    vNInd = VarMap['__versionNumber__']
    retInd = VarMap['__retained__']
    aKInd = VarMap['__addedKeys__']
    origInd = VarMap['__originalVersion__']
    
    c = dict([(VarMap[k],c[k]) for k in c.keys()])
    c[vNInd] = versionNumber
    s = dict([(VarMap[k],c[VarMap[k]]) for k in uniqueIndexes])
    s[vNInd] = versionNumber - 1
    
    H = collection.find_one(s)
   
    if H:
        if incremental:
            diff = dict([(k,H[k]) for k in H.keys() if k != '_id' and k not in specialKeyInds and k in c.keys() and notEqual(H[k],c[k]) ])
            newc = dict([(k,H[k]) for k in H.keys() if k != '_id' and k not in specialKeyInds and k not in c.keys() ])
            newc.update(c)
            c = newc
        else:
            diff = dict([(k,H[k]) for k in H.keys() if k != '_id' and k not in specialKeyInds and (k not in c.keys() or notEqual(H[k],c[k])) ])
        
        c[origInd] = H[origInd]
        
        if diff:
            diff[retInd] = True
        newkeys = [k for k in c.keys() if k not in H.keys()]
        if newkeys:
            diff[aKInd] = newkeys
        
        if diff:   
            DIFF = ContentCols.intersection([totalVariables[int(k)] for k in diff.keys()])
            diff.update(s)
            collection.update(s,diff)
            print 'Diff:' ,  diff
        else:
            DIFF = False
            c['_id'] = H['_id']
            collection.remove(s)
                
    else:
        c[origInd] = versionNumber
        diff = c
        DIFF = True
            
    id = collection.insert(c) 
    sliceInsert(c,collection,sliceColTuples,VarMap,sliceDB,DIFF,versionNumber)
    
    return id
    
def sliceInsert(c,collection,sliceColTuples,VarMap,sliceDB,DIFF,version):      
    
    dontcheck = []
    for sct in sliceColTuples:
        if all([VarMap[k] in c.keys() for k in sct]):
            slice = pm.son.SON([(k,c[VarMap[k]]) for k in sct if VarMap[k] in c.keys()])
            dc = sct in dontcheck
            if dc or not sliceDB.find_one({'slice':slice,'version':version}):
                if not dc:
                    SCT = set(sct)
                    dontcheck = uniqify(dontcheck + [ss for ss in sliceColTuples if SCT <= set(ss)])
                if DIFF:
                    sliceDB.update({'slice':slice},{'$set':{'version':version,'original':version}},upsert=True)
                else:
                    sliceDB.update({'slice':slice},{'$set':{'version':version}})

def updateSourceDBFromCollections(collectionNames = None):

    connection = pm.Connection(document_class=pm.son.SON)
    db = connection['govdata']
    if collectionNames == None:
        collectionNames = [n for n in db.collection_names() if not '__' in n and '.' not in n]
    
    sName = '____SOURCES____'
    sCollection = db[sName]
    
    if sName not in db.collection_names():
        sCollection.ensure_index([('name',pm.ASCENDING),('version',pm.DESCENDING)],unique=True,dropDups=True)
    
    for collectionName in collectionNames:
        print 'updating', collectionName , 'metadata in source DB.'
        collection = Collection(collectionName,connection=connection)
                
        old_version = sCollection.find_one({'name':collectionName})
        old_version_number = old_version['version'] if old_version else -1
        new_version_number = collection.currentVersion

        subcollections = collection.metadata.values()
        if old_version_number != new_version_number:
            vCollectionName = '__' + collectionName + '__VERSIONS__'
            vCollection = db[vCollectionName]
            tstamp = vCollection.find_one({'versionNumber':new_version_number},fields=[ 'timeStamp'])['timeStamp']
            
            rec = {'name':collectionName,'version':new_version_number,'versionOffset':-1,'subcollections':subcollections,'metadata':collection.metadata[''],'source':collection.Source,'isCollection':True,'timeStamp':tstamp}
            sCollection.insert(rec,safe=True)
            sCollection.update({'name':collectionName},{'$inc':{'versionOffset':1}})
        else:
            rec = {'subcollections':subcollections,'metadata':collection.metadata[''],'source':collection.Source,'isCollection':True}
            sCollection.update({'name':collectionName,'version':new_version_number},{'$set':rec},upsert=True)
        

def updateSourceDBByHand(data):

    connection = pm.Connection(document_class=pm.son.SON)
    db = connection['govdata']
    
    sName = '____SOURCES____'
    sCollection = db[sName]
    
    if sName not in db.collection_names():
        sCollection.ensure_index([('name',pm.ASCENDING),('version',pm.DESCENDING)],unique=True,dropDups=True)
        
    data_collections = sCollection.find({'isCollection':True}).distinct('name')
    
    for rec in data:
        assert hasattr(rec,'keys') and rec.keys() == ['name','metadata','source']
        name = rec['name']
        assert name not in data_collections
        
        old_version = sCollection.find_one({'name':name})

        if old_version:
            old_version_number = old_version['version']
            new = all([v and v == old_version.get(k,None) for (k,v) in rec.items()])
            new_version_number = old_version_number + new
        else:
            new_version_number = 0
            new = True
            
        rec['isCollection'] = False
        rec['version'] = new_version_number
        rec['versionOffset'] = -1
        rec['timeStamp'] = td.Now()
 
        if new:
            sCollection.insert(rec,safe=True)
            sCollection.update({'name':name},{'$inc':{'versionOffset':1}})
   
    
