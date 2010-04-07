#!/usr/bin/env python

from common.mongo import Collection, cleanCollection
from common.utils import IsFile, listdir, is_string_like, ListUnion, uniqify
import common.timedate as td
import backend.api as api
import solr
import itertools
import json
import pymongo as pm
import pymongo.json_util as ju
import pymongo.son as son
import os
import hashlib

def pathToSchema():
	plist = os.getcwd().split('/')
	assert 'govlove' in plist, "You're not in a filesystem with name 'govlove'."
	return '/'.join(plist[:plist.index('govlove') + 1]) + '/backend/solr-home/solr/conf/schema.xml'
	
def expand(r):
	L = [k for (k,v) in r if isinstance(v,list)]
	I = itertools.product(*tuple([v for (k,v) in r if isinstance(v,list)]))
	return [tuple([(k,v) for (k,v) in r if is_string_like(v)] + zip(L,x)) for x in I]
	
def getQueryList(collectionName,keys):
	if keys:
		collection = Collection(collectionName)
		R = api.get(collectionName,[('find',[(dict([(k,{'$exists':True}) for k in keys]),),{'fields':list(keys)}])])['data']
		colnames = [k for k in keys if k in collection.VARIABLES]
		colgroups = [k for k in keys if k in collection.ColumnGroups]
		T= ListUnion([collection.ColumnGroups[k] for k in colgroups])
		R = [son.SON([(collection.VARIABLES[int(k)],r[k]) for k in r.keys() if k.isdigit() and r[k]]) for r in R]
		R = [[(k,r[k]) for k in keys if k in r.keys() if k not in T] + [(g,[r[k] for k in collection.ColumnGroups[g] if k in r.keys() and r[k]]) for g in colgroups ] for r in R]
		return uniqify(ListUnion([expand(r) for r in R]))
	else:
		return [()]

def makeQueryDB(collectionName,hashSlices=True):
	
	collection = Collection(collectionName)
	sliceCols = collection.sliceCols
	if is_string_like(sliceCols[0]):
		sliceColList = [sliceCols]
	else:
		sliceColList = sliceCols
	sliceColU = uniqify(ListUnion(sliceColList))
	OK = dict([(x,x in collection.ColumnGroups.keys() or len(api.get(collectionName,[('distinct',(x,))])['data']) > 1) for x in sliceColU])
	sliceColList = [tuple([x for x in sliceColU if x in sc and OK[x]]) for sc in sliceColList]

	sliceColTuples = uniqify(ListUnion([subTuples(sc) for sc in sliceColList]))
	
	connection = pm.Connection()
	db = connection['govdata']
	col = db['__' + collectionName + '__SLICES__']
	cleanCollection(col)
	col.ensure_index('hash',unique=True,dropDups=True)
	
	for sliceCols in sliceColTuples:
		Q = getQueryList(collectionName,sliceCols)
		for (i,q) in enumerate(Q):
			q = son.SON(q)
			if hashSlices:
				print i ,'of', len(Q)
				R = api.get(collectionName,[('find',[(q,),{'fields':['_id']}])])['data']
				count = len(R)
				if count > 0:
					hash = hashlib.sha1(''.join([str(r['_id']) for r in R])).hexdigest()
					if col.find_one({'hash':hash}):
						col.update({'hash':hash},{'$push':{'queries':q}})
					else:
						col.insert({'hash':hash,'queries':[q],'count':count})
			else:
				col.insert({'hash':q,'queries':[q]})
				

def subqueries(q):
	K = q.keys()
	ind = itertools.product(*[[0,1]]*len(K))
	return [son.SON([(K[i],q[K[i]]) for (i,k) in enumerate(j) if k]) for  j in ind]
	
def subTuples(T):
	ind = itertools.product(*[[0,1]]*len(T))
	return [tuple([t for (t,k) in zip(T,I) if k]) for I in ind]
	
STANDARD_META = ['title','subject','description','author','keywords','content_type','last_modified','dateReleased','links']
STANDARD_META_FORMATS = {'keywords':'tplist','last_modified':'dt','dateReleased':'dt'}

def indexCollection(collectionName):

	ArgDict = {}
	
	collection = Collection(collectionName)
	sliceCols = collection.sliceCols
	if is_string_like(sliceCols[0]):
		sliceColList = [sliceCols]
	else:
		sliceColList = sliceCols
	sliceCols = uniqify(ListUnion(sliceColList))
	if hasattr(collection,'contentCols'):
		contentCols = collection.contentCols
	else:
		contentCols = sliceCols
	contentColNums = getNums(collection,contentCols)
	ArgDict['contentColNums'] = contentColNums
	if hasattr(collection,'phraseCols'):
		phraseCols = collection.phraseCols
	else:
		phraseCols = contentCols
	phraseColNums = getNums(collection,phraseCols)
	ArgDict['phraseCols'] = phraseCols
	ArgDict['phraseColNums'] = phraseColNums
	
	if hasattr(collection,'DateFormat'):
		DateFormat = collection.DateFormat
		ArgDict['DateFormat'] = DateFormat
		
		timeFormatter = td.mongotimeformatter(DateFormat)
		if 'TimeColNames' in collection.ColumnGroups.keys():
			TimeColNamesInd = getNums(collection,collection.ColumnGroups['TimeColNames'])
			tcs = [timeFormatter(t) for t in collection.ColumnGroups['TimeColNames']]
			ArgDict['timeCols'] = tcs
			timeColNameDivisions = [[td.TIME_DIVISIONS[x] for x in td.getLowest(tc)] for tc in tcs] 
			timeColPhrases = [td.phrase(t) for t in tcs]
			ArgDict['TimeColNamesInd'] = TimeColNamesInd
			ArgDict['timeColNameDivisions'] = timeColNameDivisions
			ArgDict['timeColPhrases'] = timeColPhrases
	
		if 'TimeColumns' in collection.ColumnGroups.keys():
			timeColInd = getNums(collection,collection.ColumnGroups['TimeColumns'])
			ArgDict['timeColInd'] = timeColInd
	
	d = {}
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
	
	if 'Subcollections' in collection.VARIABLES:
		ArgDict['subColInd'] = collection.VARIABLES.index('Subcollections')
		
	#solr_interface = sunburnt.SolrInterface("http://localhost:8983", pathToSchema())
	solr_interface = solr.SolrConnection("http://localhost:8983/solr")
	
	sliceDB = collection.slices
	numslices = sliceDB.count()
	i = 1
	for sliceData in sliceDB.find(timeout=False):
		queryText = min(sliceData['queries'])
		query = api.processArg(queryText,collection)
		print i , 'of', numslices , ': ' , queryText
		sliceCursor = collection.find(query,timeout=False)
		dd = d.copy()
		dd['mongoQuery'] = json.dumps(queryText,default=ju.default)
		dd['mongoText'] = ', '.join([key + '=' + value for (key,value) in queryText.items()])
		addToIndex(sliceCursor,dd,collection,solr_interface,**ArgDict)
		i += 1
			
		
	solr_interface.commit()
	
	
def getNums(collection,namelist):
	numlist = []
	for n in namelist:
		if n in collection.VARIABLES:
			numlist.append(collection.VARIABLES.index(n))
		else:
			numlist.append([collection.VARIABLES.index(m) for m in collection.ColumnGroups[n]])
	return numlist
	
	
def addToIndex(R,d,collection,solr_interface,contentColNums = None, phraseCols = None, phraseColNums = None,DateFormat = None,TimeColNamesInd = None,timeColNameDivisions = None,timeColPhrases=None,timeColInd=None,timeCols=None,subColInd = None):
	
	d['sliceContents'] = []
	d['slicePhrases'] = []
	colnames = []
	d['volume'] = 0
	mindate = None
	maxdate = None
	Subcollections = []
	
	for (i,r) in enumerate(R):

		if i/10000 == i/float(10000):
			print '. . . at', i
			
		d['sliceContents'].append( ' '.join(ListUnion([([str(r[str(x)])] if str(x) in r.keys() else []) if isinstance(x,int) else [str(r[str(xx)]) for xx in x if str(xx) in r.keys()] for x in contentColNums])))
		
		sP = ListUnion([([s + ':' + str(r[str(x)])] if str(x) in r.keys() else []) if isinstance(x,int) else [s + ':' +  str(r[str(xx)]) for xx in x if str(xx) in r.keys()] for (s,x) in zip(phraseCols,phraseColNums)])
		for ssP in sP:
			if ssP not in d['slicePhrases']:
				d['slicePhrases'].append(ssP)
		
		colnames  = uniqify(colnames + r.keys())
		d['volume'] += 1
		
		if subColInd:
			Subcollections += r[str(subColInd)]
				
		if timeColInd:
			for (k,x) in zip(collection.ColumnGroups['TimeColumns'],timeColInd):
				if str(x) in r.keys():
					dateDivisions += td.getLowest(r[str(x)])
					datePhrases.append(td.phrase(r[str(x)]))		
					if mindate:
						mindate = min(mindate,r[str(x)])
						maxdate = max(maxdate,r[str(x)])
					else:
						mindate = r[str(x)]
						maxdate = r[str(x)]
	
	d['sliceContents'] = ' '.join(d['sliceContents'])
	Subcollections = uniqify(Subcollections)
	d['columnNames'] = [collection.VARIABLES[int(x)] for x in colnames if x.isdigit()]
	d['dimension'] = len(d['columnNames'])
	#time/date
		
	if hasattr(collection,'DateFormat'):
		d['dateFormat'] = DateFormat
		
		dateDivisions = []
		datePhrases = []
		if 'TimeColNames' in collection.ColumnGroups.keys():
			K = [k for (k,j) in enumerate(TimeColNamesInd) if str(j) in colnames]
			dateDivisions += uniqify(ListUnion([timeColNameDivisions[k] for k in K]))
			d['begin_date'] = td.convertToDT(min([timeCols[k] for k in K]))
			d['end_date'] = td.convertToDT(max([timeCols[k] for k in K]),convertMode='High')
			datePhrases += uniqify([timeColPhrases[k] for k in K])
		
		if 'TimeColumns' in collection.ColumnGroups.keys():
			d['begin_date'] = td.convertToDT(mindate)
			d['end_date'] = td.convertToDT(maxdate,convertMode='High')
		
		d['dateDivisions'] = ' '.join(uniqify(dateDivisions))
		d['datePhrases'] = '|||'.join(datePhrases)
	
	
	#metadata
	metadata = collection.meta['']
	for sc in Subcollections:
		metadata.update(collection.meta[sc])

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
	#solr_interface.add(d)

def coerceToFormat(md,format):
	if format == 'tplist':
		return '|||'.join(md)	