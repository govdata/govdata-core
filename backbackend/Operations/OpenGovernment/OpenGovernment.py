"""This module sets initializes directory structure at high level and has some very basic common parsing utilities. 
"""

import numpy as np
import tabular as tb
import backend.indexing as indexing
from System.Protocols import ApplyOperations2, activate
from System.Utils import MakeDir,MakeDirs
import os
import hashlib
import pymongo as pm
import gridfs as gfs
import cPickle as pickle
import tabular as tb
from common.utils import IsFile, listdir, is_string_like, ListUnion,createCertificate, uniqify, IsDir
from common.mongo import cleanCollection, SPECIAL_KEYS
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
    

def backendProtocol(collectionName,certdir = None, createCertDir = False, createPath = None, slicePath = None, indexPath = None, hashSlices=True, write = True,ID = None):
    """This protocol is the workflow for getting source data into the backend.  It sets up three steps:  1) add collection to DB
    2) makes the queryDB and 3) indexing the collection.    It writes certificates at each step to reflect completion in the filesystem. 
    """
    if ID == None:
        ID = collectionName
    if ID and not ID.endswith('_'):
        ID += '_'
    path = MONGOSOURCES_PATH + collectionName

    D = []
    
    if certdir == None and any([x == None for x in [createPath,slicePath,indexPath]]):
        certdir = CERT_ROOT
        
    if certdir:
        if createCertDir:
            D += [(ID + 'initialize',MakeDir,(certdir,))]
        if createPath == None:
            createPath = certdir + ID + 'createCertificate.txt'
        if slicePath == None:
            slicePath = certdir + ID + 'sliceCertificate.txt'
        if indexPath == None:
            indexPath = certdir + ID + 'indexCertificate.txt'
        
    D += [(ID + 'createCollection',createCollection,(path,createPath)),
    (ID + 'makeQueryDB',indexing.makeQueryDB,[(collectionName,createPath,slicePath),{'hashSlices':hashSlices}]),
    (ID + 'indexCollection',indexing.indexCollection,(collectionName,slicePath,indexPath))]

    if write:
        outfile = CERT_PROTOCOL_ROOT + collectionName + '.py'
        ApplyOperations2(outfile,D)
    
    return D
    
    
def incrementalBackendProtocol(collectionName,certdir = None, createCertDir = False, createPath = None, slicePath = None, indexPath = None, hashSlices=True, write = True,ID = None):
    """This protocol is the workflow for getting source data into the backend.  It sets up three steps:  1) add collection to DB
    2) makes the queryDB and 3) indexing the collection.    It writes certificates at each step to reflect completion in the filesystem. 
    """
    if ID == None:
        ID = collectionName
    if ID and not ID.endswith('_'):
        ID += '_'

    D = []
    
    if certdir == None and any([x == None for x in [createPath,slicePath,indexPath]]):
        certdir = CERT_ROOT
        
    if certdir:
        if createCertDir:
            D += [(ID + 'initialize',MakeDir,(certdir,))]
        if createPath == None:
            createPath = certdir + ID + 'createCertificate.txt'
        if slicePath == None:
            slicePath = certdir + ID + 'sliceCertificate.txt'
        if indexPath == None:
            indexPath = certdir + ID + 'indexCertificate.txt'
        
    D += [(ID + 'updateCollection',updateCollection,(collectionName,createPath)),
    (ID + 'updateQueryDB',indexing.updateQueryDB,[(collectionName,createPath,slicePath),{'hashSlices':hashSlices}]),
    (ID + 'updateCollectionIndex',indexing.updateCollectionIndex,(collectionName,slicePath,indexPath))]

    if write:
        outfile = CERT_PROTOCOL_ROOT + collectionName + '.py'
        ApplyOperations2(outfile,D)
    
    return D
    

@activate(lambda x : x[0],lambda x : x[1])
def createCollection(path,certpath):
    """adds data from text file format into mongo.   path is name of textfile folder path -- and containing directory name is used to create collection name

        This function is basically deprecated in favor of the incremental version, updateCollection. 
    """

    path += '/' if path[-1] != '/' else ''  

    #loads metadata
    M = pickle.load(open(path + '__metadata.pickle'))
    
    assert 'Subcollections' in M.keys(), 'No subcollection metadata found, aborting'
    assert '' in M['Subcollections'].keys(), 'No whole-collection metadata found, aborting'
    AllMeta = M['Subcollections']['']
    
    assert 'VARIABLES' in AllMeta.keys(), 'No variable information provided, aborting.'
    Variables = AllMeta['VARIABLES']
    VarMap = dict(zip(Variables,[str(x) for x in range(len(Variables))]))

    #establishes connection to "govdata" database
    connection =  pm.Connection()
    db = connection['govdata']
    collectionName = path.split('/')[-2]
    assert not collectionName.startswith('_'), 'Collection name must not start with "_", aborting.'
    metaCollectionName = '__' + collectionName + '__'
    collection = db[collectionName]
    metacollection = db[metaCollectionName]
    cleanCollection(collection)
    cleanCollection(metacollection)
    
    #DEAL WITH TIME COLUMN TRANSLATION in COLUMN NAMES
    if 'DateFormat' in AllMeta.keys():
        TimeFormatter = td.mongotimeformatter(AllMeta['DateFormat'])

    
    #ADD SUBCOLLECTION DOCUMENTS --  with id = '__' + name
    AllMeta['Source'] = pm.son.SON(AllMeta['Source'])
    for k in M['Subcollections'].keys():
        x = M['Subcollections'][k]
        x['_id'] = k
        id = metacollection.insert(x,safe=True)
        
    
    #SET UP INDEXES
    IndexCols = ['Subcollections']
    if 'ColumnGroups' in AllMeta.keys():
        for k in ['IndexColumns','LabelColumns','TimeColumns','SpaceColumns']:
            if k in AllMeta['ColumnGroups'].keys():
                IndexCols += AllMeta['ColumnGroups'][k]
    for col in IndexCols:
        if col in VarMap.keys():
            collection.ensure_index(VarMap[col])

    if 'UniqueIndexes' in AllMeta.keys():
        for colset in AllMeta['UniqueIndexes']: 
            cols = zip([VarMap[c] for c in colset],[pm.DESCENDING]*len(colset))
            collection.ensure_index(cols,unique=True,dropDups=True)
        
    
    #ADD ASSOCIATED FILES TO GRIDFS
    if IsFile(path + '__files.pickle'):
        Files = pickle.load(open(path + '__files.pickle'))
        G = gfs.GridFS(db,collection=collectionName)
        for F in Files:
            os.environ['PROTECTION'] = 'OFF'
            S = open(F['path'],'r').read()
            os.environ['PROTECTION'] = 'ON'
            G.put(S)
    
    #ADD RECORDS FROM CHUNKS -- CHECKING HASHES AS WE GO
    if 'Subcollections' in VarMap.keys():
        sc = VarMap['Subcollections']
    else:
        sc = None
    if 'TimeColumns' in AllMeta['ColumnGroups'].keys():
        tcs = [VarMap[tc] for tc in AllMeta['ColumnGroups']['TimeColumns']]
    else:
        tcs = []
    
    if 'SpaceColumns' in AllMeta['ColumnGroups'].keys():
        spcs = [VarMap[spc] for spc in AllMeta['ColumnGroups']['SpaceColumns']]
    else:
        spcs = []
        
    SpaceCache = {} 
    for k in M['Hashes'].keys():
        print 'Adding chunk', k
        hash = M['Hashes'][k]
        poss = [x for x in listdir(path) if x.startswith(str(k) + '.')]
        assert len(poss) == 1, 'Identification of chunk file ' + str(k) + ' in directory ' + path + ' failed, aborting.'
        fpath = poss[0]     
        #       assert hash == hashlib.sha1(str(pickle.load(open(path + fpath)))).hexdigest(), 'Hash of chunk file ' + str(k) + ' in directory ' + path + ' is incorrect, aborting.'
        
        if fpath.endswith(('.csv','.tsv')): #handles situation when source text file is csv

            X = tb.tabarray(SVfile = path + fpath,verbosity = 0)
            names = X.dtype.names
            for x in X:
                newx = [float(xx) if isinstance(xx,float) else int(xx) if isinstance(xx,int) else xx for xx in x]       
                if sc in X.dtype.names:
                    sci = X.dtype.names.index(sc)
                    newx[sci] = newx[sci].split(',')
                for tc in tcs: #handling time formatting
                    if tc in X.dtype.names:
                        tci = X.dtype.names.index(tc)
                        newx[tci] = TimeFormatter(newx[tci])
                for spc in spcs:  #space formatting and completiong
                    if spc in X.dtype.names:
                        spci = X.dtype.names.index(spc)
                        newx[spci] = eval(newx[spci])
                        t = getT(newx[spci])
                        if t in SpaceCache.keys():
                            newx[spci] = SpaceCache[t].copy()
                        else:
                            newx[spci] = loc.SpaceComplete(newx[spci])
                            SpaceCache[t] = newx[spci].copy()
                        #TODO: geocoding would be added here 
                collection.insert(dict(zip(names,newx)))
                
        elif fpath.endswith('.pickle'):   #this is used when the source data is a pickle, containing dictionaries of things to be added as mongo documents
            Chunk = pickle.load(open(path +  fpath))
            for c in Chunk:
                for tc in tcs:   #time handling 
                    if tc in c.keys():
                        c[tc] = TimeFormatter(c[tc])
                for spc in spcs:
                    if spc in c.keys():   #space
                        t = getT(c[spc])
                        if t in SpaceCache.keys():
                            c[spc] = SpaceCache[t].copy()
                        else:
                            c[spc] = loc.SpaceComplete(c[spc])
                            SpaceCache[t] = c[spc].copy()
                #TODO:  geocoding also inserted here 
                
                collection.insert(c)
        else:
            print 'Type of chunk file', fpath, 'not recognized.' 
        
    connection.disconnect()
    createCertificate(certpath,'Collection ' + collectionName + ' written to DB.')


@activate(lambda x : MONGOSOURCES_PATH + x[0] + '/',lambda x : x[1])
def updateCollection(collectionName,certpath):
    """incremental version of createCollection
        
        creates collection and associated metadata collection and version history collection
        
        The basis of the incremental approach is to assume each coollection has a uniqueIndexes list of columns which define a primary unique value.  Definition:  "Corresponding" records between versions are those with the same value in the uniqueIndexes columns.
        
        Each document (record) in each collection is endowed with special keys for tracking versions -- these are actually defined in common.mongo module. The SPECIAL_KEYS are specifically:
           __versionNumber__ = version number of a particular record (document)
           __retained__ =  whether a corresponding record was retained in the next version
           __originalVersion__ = last CONTIGUOUS version where corresponding record was first added.  By "contiguous" is meant that if uniqueIndex tuple value is added at version v1, deleted at v2,  and then added back at version v3, the __originalVersion__ key for corresponding records between with __versionNumber between v1 and v2  is v1, while it is v2 for corresponding records with __versionNumber__ >= v3  
           __addedKeys__  = in a record that is retained from version V to V + 1, this record is set in the version V record to be the list of (non-primary) keys present in the V+1 record that are NOT present in the original version V record
           
     
        See also:  comments on processRecords for information on how this is propgated as versions are added
                         comments on api.get for information on the versioning logic query retrieval
                         
        For each collection with name NAME, the metadata collection is an asssociated mongoDB collection with name __NAME__ .
        The metadata collection has two "special" keys, __name__ and __versionNumber__:
                the __name__ key describes the name of the special subcollection to which the metdata applies, e.g. so that __name__ = '' is top-level metadata
                The __versionNumber__ is the version Number to which the metadata applies (the same version scheme as the regular data)
       
        For each collection, there is also an associated versionHistory collection with name __NAME__VERSIONS__, which has two keys:
            __versionNumber__ : the versionNumber 
            __timeStamp__: the timestamp when this version was added, as a special-format date object with Y, m, d, H, M, S entries
                
        
    """
    path = MONGOSOURCES_PATH + collectionName + '/'
   
    #loads metadata
    metadata = pickle.load(open(path + '__metadata.pickle'))
    
    assert 'Subcollections' in metadata.keys(), 'No subcollection metadata found, aborting'
    assert '' in metadata['Subcollections'].keys(), 'No whole-collection metadata found, aborting'
    AllMeta = metadata['Subcollections']['']
    
    assert 'VARIABLES' in AllMeta.keys(), 'No variable information provided, aborting.'
    Variables = AllMeta['VARIABLES']
    oldVarMap = dict(zip(Variables,[str(x) for x in range(len(Variables))]))   

    connection =  pm.Connection()
    db = connection['govdata']
    assert not '__' in collectionName, 'collectionName must not contain consecutive underscores'
    metaCollectionName = '__' + collectionName + '__'
    versionName = '__' + collectionName + '__VERSIONS__'
    
    collection = db[collectionName]
    metacollection = db[metaCollectionName]
    versions = db[versionName]        

    assert 'UniqueIndexes' in AllMeta.keys(), 'No unique indexes specified'
    uniqueIndexes =  AllMeta['UniqueIndexes']

    if collectionName in db.collection_names():
        versionNumber = max(versions.distinct('__versionNumber__')) + 1
        storedAllMetadata = metacollection.find_one({'__name__':'','__versionNumber__':versionNumber-1})
        storedVariables = storedAllMetadata['totalVariables']
        oldVars = [x for x in Variables if x in storedVariables]
        newVars = [x for x in Variables if not x in storedVariables]
        totalVariables = storedVariables  + newVars
        varReMap = dict([(str(Variables.index(x)),str(totalVariables.index(x))) for x in oldVars] + [(str(Variables.index(x)),str(len(storedVariables) + i)) for (i,x) in enumerate(newVars)])  
        VarMap = dict(zip(totalVariables,[str(x) for x in range(len(totalVariables))]))   
        
        #check things and clean if wrong
        
    else:
        versionNumber = 0
        for k in SPECIAL_KEYS:
            assert k not in Variables, 'Special key ' + k + ' must not be in variable names.'
            
        totalVariables = SPECIAL_KEYS + Variables
        L = len(Variables)
        varReMap =dict([(str(x),str(x + len(SPECIAL_KEYS))) for x in range(L)])
        VarMap = dict(zip(totalVariables,[str(x) for x in range(len(totalVariables))]))   
        
        
        IndexCols = ['Subcollections']
        if 'ColumnGroups' in AllMeta.keys():
            for k in ['IndexColumns','LabelColumns','TimeColumns','SpaceColumns']:
                if k in AllMeta['ColumnGroups'].keys():
                    IndexCols += AllMeta['ColumnGroups'][k]
        IndexCols = uniqify(IndexCols + ListUnion(AllMeta['sliceCols']))

        for col in IndexCols:
            if col in VarMap.keys() and col not in uniqueIndexes:
                collection.ensure_index(VarMap[col])
        
        cols = zip([VarMap[c] for c in uniqueIndexes + ['__versionNumber__']],[pm.DESCENDING]*(len(uniqueIndexes) + 1))
        collection.ensure_index(cols,unique=True,dropDups=True)
    
        metacollection.ensure_index([('__name__',pm.DESCENDING),('__versionNumber__',pm.DESCENDING)], unique=True)

    if 'DateFormat' in AllMeta.keys():
        TimeFormatter = td.mongotimeformatter(AllMeta['DateFormat'])
    
    specialKeyInds = [VarMap[k] for k in SPECIAL_KEYS]
    
    AllMeta['totalVariables'] = totalVariables
    AllMeta['Source'] = pm.son.SON(AllMeta['Source'])
    for k in metadata['Subcollections'].keys():
        x = metadata['Subcollections'][k]
        x['__name__'] = k
        x['__versionNumber__'] = versionNumber
        id = metacollection.insert(x,safe=True)
   
   
    #ADD ASSOCIATED FILES TO GRIDFS
    if IsFile(path + '__files.pickle'):
        Files = pickle.load(open(path + '__files.pickle'))
        G = gfs.GridFS(db,collection=collectionName)
        for F in Files:
            os.environ['PROTECTION'] = 'OFF'
            S = open(F['path'],'r').read()
            os.environ['PROTECTION'] = 'ON'
            G.put(S)   
            
    if 'Subcollections' in oldVarMap.keys():
        sc = oldVarMap['Subcollections']
    else:
        sc = None

    if 'TimeColumns' in AllMeta['ColumnGroups'].keys():
        tcs = [oldVarMap[tc] for tc in AllMeta['ColumnGroups']['TimeColumns']]
    else:
        tcs = []
    
    if 'SpaceColumns' in AllMeta['ColumnGroups'].keys():
        spcs = [oldVarMap[spc] for spc in AllMeta['ColumnGroups']['SpaceColumns']]
    else:
        spcs = []
        
    SpaceCache = {}             
    for k in metadata['Hashes'].keys():
        print 'Adding chunk', k
        poss = [x for x in listdir(path) if x.startswith(str(k) + '.')]
        assert len(poss) == 1, 'Identification of chunk file ' + str(k) + ' in directory ' + path + ' failed, aborting.'
        fpath = poss[0]     
         
        if fpath.endswith(('.csv','.tsv')): #handles situation when source text file is csv
            X = tb.tabarray(SVfile = path + fpath,verbosity = 0)
            names = X.dtype.names
            for x in X:
                newx = [float(xx) if isinstance(xx,float) else int(xx) if isinstance(xx,int) else xx for xx in x]       
                if sc in X.dtype.names:
                    sci = X.dtype.names.index(sc)
                    newx[sci] = newx[sci].split(',')
                for tc in tcs: #handling time formatting
                    if tc in X.dtype.names:
                        tci = X.dtype.names.index(tc)
                        newx[tci] = TimeFormatter(newx[tci])
                for spc in spcs:  #space formatting and completiong
                    if spc in X.dtype.names:
                        spci = X.dtype.names.index(spc)
                        newx[spci] = eval(newx[spci])
                        t = getT(newx[spci])
                        if t in SpaceCache.keys():
                            newx[spci] = SpaceCache[t].copy()
                        else:
                            newx[spci] = loc.SpaceComplete(newx[spci])
                            SpaceCache[t] = newx[spci].copy()
                        #TODO: geocoding would be added here 
                c = dict(zip(names,newx))
              
                ID = processRecord(c,collection,VarMap,varReMap,uniqueIndexes,versionNumber,specialKeyInds)

        elif fpath.endswith('.pickle'):
            Chunk = pickle.load(open(path +  fpath))
            for c in Chunk:
                for tc in tcs:   #time handling 
                    if tc in c.keys():
                        c[tc] = TimeFormatter(c[tc])
                for spc in spcs:
                    if spc in c.keys():   #space
                        t = getT(c[spc])
                        if t in SpaceCache.keys():
                            c[spc] = SpaceCache[t].copy()
                        else:
                            c[spc] = loc.SpaceComplete(c[spc])
                            SpaceCache[t] = c[spc].copy()
                
                ID = processRecord(c,collection,VarMap,varReMap,uniqueIndexes,versionNumber,specialKeyInds)
        else:
            print 'Type of chunkfile: ', fpath, 'not recognized.'

    ts = td.Now()
    newVersion = {'__versionNumber__': versionNumber, '__timeStamp__':ts}
    versions.insert(newVersion)

    connection.disconnect()
    createCertificate(certpath,'Collection ' + collectionName + ' written to DB.')

                    
def processRecord(c,collection,VarMap,varReMap,uniqueIndexes,versionNumber,specialKeyInds):
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
    
    c = dict([(varReMap[k],c[k]) for k in c.keys()])
    c[vNInd] = versionNumber
    s = dict([(VarMap[k],c[VarMap[k]]) for k in uniqueIndexes])
    s[vNInd] = versionNumber - 1
    
    H = collection.find_one(s)
   
    if H:
        c[origInd] = H[origInd]
        diff = dict([(k,H[k]) for k in H.keys() if k != '_id' and k not in specialKeyInds and (k not in c.keys() or notEqual(H[k],c[k])) ])
        if diff:
            diff[retInd] = True
        newkeys = [k for k in c.keys() if k not in H.keys()]
        if newkeys:
            diff[aKInd] = newkeys
        
        if diff:
            diff.update(s)
            collection.update(s,diff)
            print 'Diff:' ,  diff
        else:
            c['_id'] = H['_id']
            collection.remove(s)
    else:

        print 'New:' , [c[VarMap[k]] for k in uniqueIndexes] 
        c[origInd] = versionNumber
            
    id = collection.insert(c)
    return id
            

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

def incrementalBackendProtocol2(collectionName,downloader = None, downloadProtocol= None,downloadArgs = None, parser=None,certdir = None, createCertDir = False, downloadPath = None , createPath = None, slicePath = None, indexPath = None, hashSlices=True, write = True,ID = None,incremental = False,uptostep=None):
    """This protocol is the workflow for getting source data into the backend.  It sets up three steps:  1) add collection to DB
    2) makes the queryDB and 3) indexing the collection.    It writes certificates at each step to reflect completion in the filesystem. 
    """
    if ID == None:
        ID = collectionName
    if ID and not ID.endswith('_'):
        ID += '_'
        
    assert parser, 'parser must be specified'

    StepList = []
    
    if certdir == None and any([x == None for x in [createPath,slicePath,indexPath]]):
        certdir = CERT_ROOT
        
    if certdir:
        if createCertDir:
            StepList += [(ID + 'initialize',MakeDir,(certdir,))]
        if downloadPath == None:
            downloadPath = certdir + ID + 'downloadCertificate.txt'
        if createPath == None:
            createPath = certdir + ID + 'createCertificate.txt'
        if slicePath == None:
            slicePath = certdir + ID + 'sliceCertificate.txt'
        if indexPath == None:
            indexPath = certdir + ID + 'indexCertificate.txt'   
    
    download_dir = DOWNLOAD_ROOT + collectionName + '/'

    if downloader:
        
        if isinstance(downloader,list):
            if downloadArgs == None:
                downloadArgs = [()]*len(downloader)
             
            (D,N) = zip(*downloader) ; A = downloadArgs
            downloadStep0 = (ID + 'download_'  + N[0],D[0],(download_dir + N[0] + '/',) + A[0])
            downloadSteps = [(ID + 'download_'  + N[i],D[i],(download_dir + N[i] + '/',dict([(N[j],download_dir + N[j] + '/') for j in range(i)]) ) + A[i]) for i in range(1,len(downloader))]
            downloadSteps = [downloadStep0] + downloadSteps
            
        else:
            assert hasattr(downloader,'__call__')
            if downloadArgs == None:
                downloadArgs = ()
            downloadSteps = [(ID + 'download',downloader,(download_dir,) + downloadArgs)]
    elif downloadProtocol:
        downloadSteps = downloadProtocol(download_dir,*downloadArgs)
        for (i,z) in downloadSteps:
            downloadSteps[i] = ((z[0] if z[0].startswith(ID) else ID + z[0]), z1,z[2])
            
    final_dir = downloadSteps[-1][2][0]
            
    if incremental:
        downloadSteps = [(name,incWrapper,(func, args,i),{'Fast':True}) for (i,(name,func,args)) in enumerate(downloadSteps)]

    StepList += downloadSteps
    
    StepList += [(ID + 'download_check',download_check,(final_dir,incremental,downloadPath))]
        
    StepList += [(ID + 'updateCollection',updateCollection2,[(final_dir,collectionName,parser,downloadPath,createPath),{'incremental':incremental}]),
    (ID + 'updateQueryDB',indexing.updateQueryDB,[(collectionName,createPath,slicePath),{'hashSlices':hashSlices}]),
    (ID + 'updateCollectionIndex',indexing.updateCollectionIndex,(collectionName,slicePath,indexPath))]

    if uptostep:
        for (i,d) in enumerate(StepList):
            if d[0] == ID + uptostep:
                StepList = StepList[:i+1]
                break

    if write:
        outfile = CERT_PROTOCOL_ROOT + collectionName + '.py'
        ApplyOperations2(outfile,StepList)
    
    return StepList
        


    
@activate(lambda x :  x[0].__dependor__(dict(enumerate(x[1]))),     lambda x :  x[0].__creator__(dict(enumerate(x[1]))))
def incWrapper(func,args,i):
    if not IsDir(args[0]):
            MakeDirs(args[0])
    if i == 0:
        args = (get_next_increment_path(args[0]) ,) + args[1:]
        return func(*args)  
    else:
        m = max([get_max_increment(v) for v in args[1].values()] + [get_max_increment(args[0])])

        args = (increment_format(args[0],m) , dict([(k,get_max_increment_path(v)) for (k,v) in args[1].items()])) + args[2:]
        return func(*args)  
        
 
        
def get_and_check_increments(download_dir):
    X = re.compile('__[\d]+__$')
    L = [x for x in listdir(download_dir) if not x.startswith('.')]
    assert all([X.match(x) for x in L])
    return [int(x[2:-2]) for x in L]
  
def get_increment_paths(download_dir):
    increments = get_and_check_increments(download_dir)
    return [increment_format(download_dir,i) for i in increments]
  
def increment_format(download_dir,i):
    return download_dir + '__' + str(i) + '__/'
  
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
    V = versions.find(fields=['__startIncrement__','__endIncrement__']).sort('__versionNumber__',pm.ASCENDING)
    V = [(v['__startIncrement__'],v['__endIncrement__']) for v in V]
    (S,E) = zip(*V)
    assert all([S[i] < S[i+1] and E[i] == S[i+1] - 1 for i in range(len(S)-1)]) and all([s <= e for (s,e) in V ]), 'Something\'s wrong with version history increments.'
    
    return (S,E)
    
def get_max_increment_fromDB(versions):
    (S,E) = get_and_check_increments_fromDB(versions)
    return max(E)
    
@activate(lambda x : x[0],lambda x : x[2])
def download_check(download_dir, incremental, certpath):

    if not incremental: 
        check_list = download_dir   
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
        self.Data = tb.tabarray(SVfile = file,verbosity = 0)
        self.__ind = 0
    
    def next(self):
        if self.__ind < len(self.Data):
            r = self.Data[self.__ind]
            r = [float(xx) if isinstance(xx,float) else int(xx) if isinstance(xx,int) else xx for xx in r]  
            r = dict(zip(self.Data.dtype.names,r))
            
            if 'Subcollections' in r.keys():
                r['Subcollections'] = r['Subcollections'].split(',')
            
            self.__ind += 1
                
            return r
            
        else:
            raise StopIteration
    
    

@activate(lambda x :  (x[0] + '/',x[3]),lambda x : x[4])
def updateCollection2(download_dir,collectionName,parserClass,checkpath,certpath,incremental=False):

    connection =  pm.Connection()
    db = connection['govdata']
    assert not '__' in collectionName, 'collectionName must not contain consecutive underscores'
    metaCollectionName = '__' + collectionName + '__'
    versionName = '__' + collectionName + '__VERSIONS__'
    
    collection = db[collectionName]
    metacollection = db[metaCollectionName]
    versions = db[versionName]        
            
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
        
    if sources:
		iterator = parserClass(sources[0])
	
		assert hasattr(iterator,'UniqueIndexes'),  'No unique indexes specified'
		uniqueIndexes = iterator.UniqueIndexes
	
		if collectionName in db.collection_names():
			versionNumber = max(versions.distinct('__versionNumber__')) + 1
			storedAllMetadata = metacollection.find_one({'__name__':'','__versionNumber__':versionNumber-1})
			totalVariables = storedAllMetadata['totalVariables']
			VarMap = dict(zip(totalVariables,[str(x) for x in range(len(totalVariables))]))   
			
			#check things are the same 
			
		else:
			versionNumber = 0
			IndexCols = [x for x in uniqify(['Subcollections'] + ListUnion(iterator.sliceCols) + ListUnion([iterator.ColumnGroups.get(k,[]) for k in ['IndexColumns','LabelColumns','TimeColumns','SpaceColumns']])) if x not in uniqueIndexes]
	
			totalVariables = SPECIAL_KEYS + uniqueIndexes + IndexCols
			VarMap = dict(zip(totalVariables,map(str,range(len(totalVariables)))))  
			
			cols = zip([VarMap[c] for c in uniqueIndexes + ['__versionNumber__']],[pm.DESCENDING]*(len(uniqueIndexes) + 1))
			collection.ensure_index(cols,unique=True,dropDups=True)
	
			for col in IndexCols:
				collection.ensure_index(VarMap[col])
				
				
		vNInd = VarMap['__versionNumber__']
		retInd = VarMap['__retained__']
		
		if hasattr(iterator,'DateFormat'):
			TimeFormatter = td.mongotimeformatter(iterator.DateFormat)
		
		specialKeyInds = [VarMap[k] for k in SPECIAL_KEYS]
	
		if 'TimeColumns' in iterator.ColumnGroups.keys():
			tcs = iterator.ColumnGroups['TimeColumns']
		else:
			tcs = []
		
		if 'SpaceColumns' in iterator.ColumnGroups.keys():
			spcs = iterator.ColumnGroups['SpaceColumns']
		else:
			spcs = []
	
	
		toParse = ListUnion([[source + '__PARSE__/' + x for x in listdir(source + '__PARSE__') if not x.startswith('.')] for source in sources])
	
		SpaceCache = {}    
		for file in toParse[:4]:
			iterator.refresh(file) 
			for c in iterator: 
				newVars = [x for x in c.keys() if not x in totalVariables]
				assert all(['__' not in x for x in newVars]) , '__ must not appear in key names.'     
				totalVariables += newVars
				VarMap.update(dict(zip(newVars,map(str,range(len(totalVariables) - len(newVars),len(totalVariables))))))
				for tc in tcs:   #time handling 
					if tc in c.keys():
						c[tc] = TimeFormatter(c[tc])
				for spc in spcs:
					if spc in c.keys():   #space
						t = getT(c[spc])
						if t in SpaceCache.keys():
							c[spc] = SpaceCache[t].copy()
						else:
							c[spc] = loc.SpaceComplete(c[spc])
							SpaceCache[t] = c[spc].copy()            
				processRecord2(c,collection,VarMap,uniqueIndexes,versionNumber,specialKeyInds,incremental)
		if incremental:
			collection.update({vNInd:versionNumber - 1, retInd : {'$exists':False}}, {'$set':{vNInd:versionNumber}})        
	
	
		updateMetacollection(iterator, metacollection,incremental,versionNumber,totalVariables)
		
		updateAssociatedFiles(sources,collection)
		
		updateVersionHistory(versionNumber,versions,startInc,endInc)
    
    connection.disconnect()
    createCertificate(certpath,'Collection ' + collectionName + ' written to DB.')



def updateVersionHistory(versionNumber, versions,startInc,endInc):

    ts = td.Now()
    if startInc != None and endInc != None:
        newVersion = {'__versionNumber__': versionNumber, '__timeStamp__':ts, '__startIncrement__':startInc, '__endIncrement__':endInc}
    else:
        newVersion = {'__versionNumber__': versionNumber, '__timeStamp__':ts}
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
                

def updateMetacollection(iterator, metacollection,incremental,versionNumber,totalVariables):
    
    metadata = iterator.metadata
    
    metadata['']['totalVariables'] = totalVariables
    metadata['']['Source'] = pm.son.SON(metadata['']['Source'])
       
    metacollection.ensure_index([('__name__',pm.DESCENDING),('__versionNumber__',pm.DESCENDING)], unique=True)   
    
    if incremental:
        previousMetadata = dict([(p["__name__"],p) for  p in metacollection.find({'__versionNumber__':versionNumber - 1})])
        if previousMetadata:
			for x in previousMetadata.values():
				x[1].pop('_id')
			
			for k in previousMetadata.keys():
				if k not in metadata.keys():
					metadata[k] = previousMetadata[k]
					
			for k in previousMetadata[''].keys():
				if k not in metadata[''].keys():
					metadata[''][k] = previousMetadata[''][k]
			
			for k in previousMetadata['']['ColumnGroups'].keys():
				if k not in metadata['']['ColumnGroups'].keys():
					metadata['']['ColumnGroups'][k] = previousMetadata['']['ColumnGroups'][k]
				else:
					metadata['']['ColumnGroups'][k] += previousMetadata['']['ColumnGroups'][k] 
                    

    for k in metadata.keys():
        x = metadata[k]
        x['__name__'] = k
        x['__versionNumber__'] = versionNumber
        id = metacollection.insert(x,safe=True)
            

def processRecord2(c,collection,VarMap,uniqueIndexes,versionNumber,specialKeyInds,incremental):
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
            diff.update(s)
            collection.update(s,diff)
            print 'Diff:' ,  diff
        else:
            c['_id'] = H['_id']
            collection.remove(s)
                
    else:
        print 'New:' , [c[VarMap[k]] for k in uniqueIndexes] 
        c[origInd] = versionNumber
            
    id = collection.insert(c)
    return id

