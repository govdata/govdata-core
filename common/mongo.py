import pymongo as pm
	
class Collection(pm.collection.Collection):
    """Extends the pymongo collection object for the special case of govdata collections, meaning attaching stuff from 
    from the metadata collection as well as the slices from the query database. 
    """
	
	def __init__(self,name,connection = None):
		if connection == None:
			connection = pm.Connection()
		assert 'govdata' in connection.database_names(), 'govdata collection not found.'
		db = connection['govdata']
		assert name in db.collection_names(), 'collection ' + name + ' not found in govdata database.'
		pm.collection.Collection.__init__(self,db,name)
		metaname = '__' + name + '__'
		assert metaname in db.collection_names(), 'No metadata collection associated with ' + name + ' found.'
		self.metaCollection = db[metaname]		
		self.meta = dict([(l['_id'],l) for l in self.metaCollection.find()])
		
		slicesname = '__' + name + '__SLICES__'
		if slicesname in db.collection_names():
			self.slices = db[slicesname]
		
	def subcollection_names(self):
		return self.meta.keys()
		
	def __getattr__(self,name):
		try:
			V = self.meta[''][name]
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