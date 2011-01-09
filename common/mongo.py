"""
utilities for working with govdata in mongoDB format

"""
   
import pymongo as pm
from common.utils import Timer, is_string_like

SPECIAL_KEYS =  ['__versionNumber__','__retained__','__addedKeys__','__originalVersion__']  

class Collection(pm.collection.Collection):
    """Extends the pymongo collection object for the special case of govdata collections, meaning:
    
          1) attaching connections to various associated collections, including metadata, and versionHistory, as attributes, specifically:
               self.metaCollection = metadata collection connection
               self.versions = version history colleciton connection
           
          1a) The complete currentVersion metadata is read out into a dictionary, whose keys are names of subcollections.  That is:
          
                self.metadata[subcol] = metadata for that subcol -- from the current version of metadata
                
                so that e.g. "top level" metdata is self.metadata[''].  
                
          1b) The versionNumber parameter to __init__ serves to modify 1a) so that metadata from the specified version is attached as self.metadata
          
          2) This class also makes various attributes available from top-level metadata, by passing on through via intercepting __getattr__, i.e.
                IF attribute not otherwise found:
                    try:
                        X.attribute = X.metadata[''][attribute]
                        
                so e.g. X.ColumnGroups is defined.
          
    """
    
    def __init__(self,name,connection = None,versionNumber=None,attachMetadata = False):
        
        if connection == None:
            connection = pm.Connection(document_class=pm.son.SON)
        assert 'govdata' in connection.database_names(), 'govdata collection not found.'
        db = connection['govdata']
        assert name in db.collection_names(), 'collection ' + name + ' not found in govdata database.'
        pm.collection.Collection.__init__(self,db,name)
        metaname = '__' + name + '__'
        assert metaname in db.collection_names(), 'No metadata collection associated with ' + name + ' found.'
        self.metaCollection = db[metaname]              
                                
        versionsname = '__' + name + '__VERSIONS__'
        assert versionsname in db.collection_names()
        self.versions = db[versionsname]
        self.currentVersion = max(self.versions.distinct('versionNumber'))
        if versionNumber == None:
            versionNumber = self.currentVersion
        self.versionNumber = versionNumber
       
        vQuery = {"versionNumber":versionNumber} if self.versionNumber != 'ALL' else {}
        if not attachMetadata:
            vQuery["name"] = "" 
        self.metadata = dict([(l['name'],l) for l in self.metaCollection.find(vQuery)])
                
        self.valueProcessors = self.metadata[''].get('valueProcessors',{})
        self.nameProcessors = self.metadata[''].get('nameProcessors',{})
        
        slicesname =  '__' + name + '__SLICES__'
        if slicesname in db.collection_names():
            self.slices = db[slicesname]
        
    def subcollection_names(self):
        return self.metadata.keys()
        
    def __getattr__(self,name):
        try:
            V = self.metadata[''][name]
        except KeyError:
            raise AttributeError, "Can't find attribute " + name
        else:
            return V

def cleanCollection(collection):
    collection.remove()
    try:
        collection.drop_indexes()
    except:
        print 'couldnt delete index'
    else:
        pass
        
        
def processArg(arg,collection):
    """Translates the arg to human readable to collections"""
    V = collection.columns
    C = collection.columnGroups
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
        for (k,v) in T:
            if isinstance(k,list) or isinstance(k,tuple):
                S["$or"] = [{kk:v} for kk in k]
        if '$where' in arg:
            S['$where'] = arg['$where']
        return S
    else:
        return arg
        