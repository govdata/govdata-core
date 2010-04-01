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
	L = [k for k in r.keys() if isinstance(r[k],list)]
	NL = [k for k in r.keys() if is_string_like(r[k])]
	I = itertools.product(*tuple([r[k] for k in L]))
	return uniqify([son.SON([(k,r[k]) for k in NL] + zip(L,x)) for x in I])
	
def getQueryList(collectionName,keys):
	collection = Collection(collectionName)
	R = api.get(collectionName,[('find',{'fields':keys})])['data']
	colnames = [k for k in keys if k in collection.VARIABLES]
	colgroups = [k for k in keys if k in collection.ColumnGroups]
	T= ListUnion([collection.ColumnGroups[k] for k in colgroups])
	R = [son.SON([(collection.VARIABLES[int(i)],r[i]) for i in r.keys() if i.isdigit() and r[i]]) for r in R]
	R = [son.SON([(k,v) for (k,v) in r.items() if k not in T] + [(g,[r[k] for k in collection.ColumnGroups[g] if k in r.keys() and r[k]]) for g in colgroups ]  ) for r in R]
	return ListUnion([expand(r) for r in R])


def makeQueryDB(collectionName):
	
	collection = Collection(collectionName)
	sliceCols = collection.sliceCols
	Q = getQueryList(collectionName,sliceCols)
	connection = pm.Connection()
	db = connection['govdata']
	col = db['__' + collectionName + '__SLICES__']
	cleanCollection(col)
	col.ensure_index('hash',unique=True,dropDups=True)
	
	for (i,q) in enumerate(Q):
		if not col.find_one({'queries':q}):
			R = api.get(collectionName,[('find',[(q,),{'fields':['_id']}])])['data']
			count = len(R)
			if count > 1:
				print 'Preprocessing query', i ,'of', len(Q)
				hash = hashlib.sha1(''.join([str(r['_id']) for r in R])).hexdigest()
				if col.find_one({'hash':hash}):
					col.update({'hash':hash},{'$push':{'queries':q}})
				else:
					col.insert({'hash':hash,'queries':[q],'count':count})


STANDARD_META = ['title','subject','description','author','keywords','content_type','last_modified','dateReleased','links']
STANDARD_META_FORMATS = {'keywords':'tplist','last_modified':'dt','dateReleased':'dt'}

def indexCollection(collectionName):

	ArgDict = {}
	collection = Collection(collectionName)
	sliceCols = collection.sliceCols
	
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

	#solr_interface = sunburnt.SolrInterface("http://localhost:8983", pathToSchema())
	solr_interface = solr.SolrConnection("http://localhost:8983/solr")

	R = collection.find()
	for (i,r) in enumerate(R):
		print i
		addToIndex([r],collection,solr_interface,**ArgDict)	
		
	solr_interface.commit()
	
	
def getNums(collection,namelist):
	numlist = []
	for n in namelist:
		if n in collection.VARIABLES:
			numlist.append(collection.VARIABLES.index(n))
		else:
			numlist.append([collection.VARIABLES.index(m) for m in collection.ColumnGroups[n]])
	return numlist
	
	
def addToIndex(R,collection,solr_interface,contentColNums = None, phraseCols = None, phraseColNums = None,DateFormat = None,TimeColNamesInd = None,timeColNameDivisions = None,timeColPhrases=None,timeColInd=None,timeCols=None):
	
	for r in R:
		d = {}
		#database slice things
		d['mongoQuery'] = json.dumps(r['_id'],default=ju.default)
		
		d['sliceContents'] = ' '.join(ListUnion([([str(r[str(x)])] if str(x) in r.keys() else []) if isinstance(x,int) else [str(r[str(xx)]) for xx in x if str(xx) in r.keys()] for x in contentColNums]))
		d['slicePhrases'] = '|||'.join(ListUnion([([s + ':' + str(r[str(x)])] if str(x) in r.keys() else []) if isinstance(x,int) else [s + ':' +  str(r[str(xx)]) for xx in x if str(xx) in r.keys()] for (s,x) in zip(phraseCols,phraseColNums)]))
		d['columnNames'] = '|||'.join(r.keys())
		
		#slice stats
		d['volume'] = 1
		d['dimension'] = len(d['columnNames'])
		
		#source and source acronyms
		Source = collection.Source
		SourceNameDict = dict([(k,Source[k]['Name'] if isinstance(Source[k],dict) else Source[k]) for k in Source.keys()])
		d['SourceSpec'] = json.dumps(SourceNameDict,default=ju.default)
		SourceAbbrevDict = dict([(k,Source[k]['ShortName']) for k in Source.keys() if isinstance(Source[k],dict) and 'ShortName' in Source[k].keys() ])
		d['agency'] = SourceNameDict['Agency']
		d['subagency'] = SourceNameDict['Subagency']
		d['dataset'] = SourceNameDict['Dataset']
		for k in set(SourceNameDict.keys()).difference(['Agency','Subagency','Dataset']):
			d['source_' + str(k).lower()] = SourceNameDict[k]
		for k in SourceAbbrevDict.keys():
			d['source_' + str(k).lower() + '_acronym'] = SourceAbbrevDict[k]
		d['source'] = ' '.join(SourceNameDict.values() + SourceAbbrevDict.values())
		
		#metadata
		metadata = collection.meta['']
		if 'Subcollections' in collection.VARIABLES:
			for l in r[str(collection.VARIABLES.index('Subcollections'))]:
				metadata.update(collection.meta[l])

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
				
		
		#time/date
		if hasattr(collection,'DateFormat'):
			d['dateFormat'] = DateFormat
			
			dateDivisions = []
			datePhrases = []
			if 'TimeColNames' in collection.ColumnGroups.keys():
				K = [k for (k,j) in enumerate(TimeColNamesInd) if str(j) in r.keys()]
				dateDivisions += uniqify(ListUnion([timeColNameDivisions[k] for k in K]))
				d['begin_date'] = td.convertToDT(min([timeCols[k] for k in K]))
				d['end_date'] = td.convertToDT(max([timeCols[k] for k in K]),convertMode='High')
				datePhrases += uniqify([timeColPhrases[k] for k in K])

				
			if 'TimeColumns' in collection.ColumnGroups.keys():
				for (k,x) in zip(collection.ColumnGroups['TimeColumns'],timeColInd):
					if str(x) in r.keys():
						dateDivisions += td.getLowest(r[str(x)])
						datePhrases.append(td.phrase(r[str(x)]))
					
				d['begin_date'] = td.convertToDT(min([r[str(x)] for x in timeColInd]))
				d['end_date'] = td.convertToDT(max([r[str(x)] for x in timeColInd]),convertMode='High')
			
			
			d['dateDivisions'] = ' '.join(uniqify(dateDivisions))
			d['datePhrases'] = '|||'.join(datePhrases)
		
		solr_interface.add(**d)
		#solr_interface.add(d)
