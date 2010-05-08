"""
utilities for working with govdata in mongoDB format

"""
   
import pymongo as pm
    

SPECIAL_KEYS =  ['__versionNumber__','__retained__','__addedKeys__','__originalVersion__']  

class Collection(pm.collection.Collection):
    """Extends the pymongo collection object for the special case of govdata collections, meaning:
    
          1) attaching connections to various associated collections, including metadata, indexed slices, and versionHistory, as attributes, specifically:
               self.metaCollection = metadata collection connection
               self.slices = slice collection connection
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
    
    def __init__(self,name,connection = None,versionNumber=None):
        if connection == None:
            connection = pm.Connection()
        assert 'govdata' in connection.database_names(), 'govdata collection not found.'
        db = connection['govdata']
        assert name in db.collection_names(), 'collection ' + name + ' not found in govdata database.'
        pm.collection.Collection.__init__(self,db,name)
        metaname = '__' + name + '__'
        assert metaname in db.collection_names(), 'No metadata collection associated with ' + name + ' found.'
        self.metaCollection = db[metaname]      
        
        slicesname = '__' + name + '__SLICES__'
        if slicesname in db.collection_names():
            self.slices = db[slicesname]
            
        versionsname = '__' + name + '__VERSIONS__'
        if versionsname in db.collection_names() and versionNumber != 'ALL':
            self.versions = db[versionsname]
            currentVersion = max(self.versions.distinct('__versionNumber__'))
            self.currentVersion = currentVersion
            if versionNumber == None:
            	versionNumber = currentVersion
            self.versionNumber = versionNumber
            self.metadata = dict([(l['__name__'],l) for l in self.metaCollection.find({'__versionNumber__':versionNumber})])
        else:
            self.metadata = dict([(l['__name__'],l) for l in self.metaCollection.find()])
        
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