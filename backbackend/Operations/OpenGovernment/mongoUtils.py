import os
import hashlib
import pymongo as pm
import gridfs as gfs
import cPickle as pickle
import tabular as tb
from common.utils import IsFile, listdir, is_string_like, ListUnion,createCertificate, uniqify
from common.mongo import cleanCollection, SPECIAL_KEYS
import common.timedate as td
import common.location as loc
from System.Protocols import activate

MONGOSOURCES_PATH = '../Data/OpenGovernment/MongoSources/'

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
        
        The basis of the incremental approach is to assume each coollection has a uniqueIndexes list of columns which define a primary unique value.  "Corresponding" records between versions are those with the same value in the uniqueIndexes columns.
        
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

    ts = td.Now()
    newVersion = {'__versionNumber__': versionNumber, '__timeStamp__':ts}
    versions.insert(newVersion)
    
                    
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
        else:
            c['_id'] = H['_id']
            collection.remove(s)
    else:
        c[origInd] = versionNumber
            
    id = collection.insert(c)
    return id
            
import numpy
isnan = numpy.isnan
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
