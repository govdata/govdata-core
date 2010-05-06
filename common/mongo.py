import pymongo as pm
    
    
SPECIAL_KEYS =  ['__versionNumber__','__retained__','__addedKeys__','__originalVersion__']  


class Collection(pm.collection.Collection):
    """Extends the pymongo collection object for the special case of govdata collections, meaning attaching stuff from 
    from the metadata collection as well as the slices from the query database. 
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
        if versionsname in db.collection_names():
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