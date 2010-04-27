import os
import hashlib
import pymongo as pm
import gridfs as gfs
import cPickle as pickle
import tabular as tb
from common.utils import IsFile, listdir, is_string_like, ListUnion,createCertificate
from common.mongo import cleanCollection
import common.timedate as td
import common.location as loc
from System.Protocols import activate

MONGOSOURCES_PATH = '../Data/OpenGovernment/MongoSources/'

@activate(lambda x : x[0],lambda x : x[1])
def createCollection(path,certpath):

	path += '/' if path[-1] != '/' else ''	
	
	M = pickle.load(open(path + '__metadata.pickle'))
	
	assert 'Subcollections' in M.keys(), 'No subcollection metadata found, aborting'
	assert '' in M['Subcollections'].keys(), 'No whole-collection metadata found, aborting'
	AllMeta = M['Subcollections']['']
	
	assert 'VARIABLES' in AllMeta.keys(), 'No variable information provided, aborting.'
	Variables = AllMeta['VARIABLES']
	VarMap = dict(zip(Variables,[str(x) for x in range(len(Variables))]))


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
		
	for k in M['Hashes'].keys():
		print 'Adding chunk', k
		hash = M['Hashes'][k]
		poss = [x for x in listdir(path) if x.startswith(str(k) + '.')]
		assert len(poss) == 1, 'Identification of chunk file ' + str(k) + ' in directory ' + path + ' failed, aborting.'
		fpath = poss[0]		
#		assert hash == hashlib.sha1(str(pickle.load(open(path + fpath)))).hexdigest(), 'Hash of chunk file ' + str(k) + ' in directory ' + path + ' is incorrect, aborting.'
		if fpath.endswith(('.csv','.tsv')):

			X = tb.tabarray(SVfile = path + fpath,verbosity = 0)
			names = X.dtype.names
			for x in X:
				newx = [float(xx) if isinstance(xx,float) else int(xx) if isinstance(xx,int) else xx for xx in x]		
				if sc in X.dtype.names:
					sci = X.dtype.names.index(sc)
					newx[sci] = newx[sci].split(',')
				for tc in tcs: 
					if tc in X.dtype.names:
						tci = X.dtype.names.index(tc)
						newx[tci] = TimeFormatter(newx[tci])
				for spc in spcs:
					if spc in X.dtype.names:
						spci = X.dtype.names.index(spc)
						newx[spci] = eval(newx[spci])
						loc.SpaceComplete(newx[spci])

				collection.insert(dict(zip(names,newx)))
				
		elif fpath.endswith('.pickle'):
			Chunk = pickle.load(open(path +  fpath))
			for c in Chunk:
				for tc in tcs:
					if tc in c.keys():
						c[tc] = TimeFormatter(c[tc])
				for spc in spcs:
					if spc in c.keys()
						loc.SpaceComplete(c[spc])
				
				collection.insert(c)
		else:
			print 'Type of chunk file', fpath, 'not recognized.' 
		
	connection.disconnect()
	createCertificate(certpath,'Collection ' + collectionName + ' written to DB.')
	
