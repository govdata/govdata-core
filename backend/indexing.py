#!/usr/bin/env python

from common.mongo import Collection
from common.utils import IsFile, listdir, is_string_like, ListUnion, uniqify
import common.timedate as td
import backend.api as api
import sunburnt
import solr
import itertools
import json
import pymongo.json_util as ju
import pymongo.son as son
import os

def pathToSchema():
	plist = os.getcwd().split('/')
	assert 'govlove' in plist, "You're not in a filesystem with name 'govlove'."
	return '/'.join(plist[:plist.index('govlove') + 1]) + '/backend/solr-home/solr/conf/schema.xml'
	
def expand(r):
	L = [k for k in r.keys() if isinstance(r[k],list)]
	NL = [k for k in r.keys() if is_string_like(r[k])]
	I = itertools.product(*tuple([r[k] for k in L]))
	return [dict([(k,r[k]) for k in NL] + zip(L,x)) for x in I]
	
def getQueryList(collectionName,keys):
	collection = Collection(collectionName)
	R = api.get(collectionName,[('find',{'fields':keys})])['data']
	colnames = [k for k in keys if k in collection.VARIABLES]
	colgroups = [k for k in keys if k in collection.ColumnGroups]
	T= ListUnion([collection.ColumnGroups[k] for k in colgroups])
	R = [dict([(collection.VARIABLES[int(i)],r[i]) for i in r.keys() if i.isdigit() and r[i]]) for r in R]
	R = [dict(  [(k,v) for (k,v) in r.items() if k not in T] + [(g,[r[k] for k in collection.ColumnGroups[g] if k in r.keys() and r[k]]) for g in colgroups ]  ) for r in R]
	return ListUnion([expand(r) for r in R])

def makeQueryDB(collectionName):
	pass

def indexCollection(collectionName):

	collection = Collection(collectionName)
	sliceCols = collection.sliceCols
	if hasattr(collection,'contentCols'):
		contentCols = collection.contentCols
	else:
		contentCols = sliceCols
	contentColNums = getNums(collection,contentCols)
		
	if hasattr(collection,'phraseCols'):
		phraseCols = collection.phraseCols
	else:
		phraseCols = contentCols
	phraseColNums = getNums(collection,phraseCols)
	
	#index all indiviudal records with query by _id
	R = collection.find()
	standard_meta = ['title','subject','description','author','keywords','content_type','last_modified','dateReleased','links']
	standard_meta_formats = {'keywords':'tplist','last_modified':'dt','dateReleased':'dt'}
	
	if hasattr(collection,'DateFormat'):
		DateFormat = collection.DateFormat
		timeFormatter = td.mongotimeformatter(DateFormat)
		if 'TimeColNames' in collection.ColumnGroups.keys():
			TimeColNamesInd = getNums(collection,collection.ColumnGroups['TimeColNames'])
			tcs = [timeFormatter(t) for t in collection.ColumnGroups['TimeColNames']]
			timeColNameDivisions = [[td.TIME_DIVISIONS[x] for x in td.getLowest(tc)] for tc in tcs] 
			timeColPhrases = [td.phrase(t) for t in tcs]

			
		if 'TimeColumns' in collection.ColumnGroups.keys():
			timeColInd = getNums(collection,collection.ColumnGroups['TimeColumns'])

	#solr_interface = sunburnt.SolrInterface("http://localhost:8983", pathToSchema())
	solr_interface = solr.SolrConnection("http://localhost:8983/solr")
	
	for (i,r) in enumerate(R):

		print i
		if i > 100:
			break
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
		d['sourceSpec'] = json.dumps(Source,default=ju.default)
		SourceNameDict = dict([(k,Source[k]['Name'] if isinstance(Source[k],dict) else Source[k]) for k in Source.keys()])
		SourceAbbrevDict = dict([(k,Source[k]['ShortName']) for k in Source.keys() if isinstance(Source[k],dict) and 'ShortName' in Source[k].keys() ])
		d['agency'] = SourceNameDict['Agency']
		d['subagency'] = SourceNameDict['Subagency']
		d['dataset'] = SourceNameDict['Dataset']
		for k in set(SourceNameDict.keys()).difference(['Agency','Subagency','Dataset']):
			d['source_' + str(k).lower()] = SourceNameDict[k]
		for k in SourceAbbrevDict.keys():
			d['source_' + str(k).lower() + '_acronym'] = SourceAbbrevDict[k]
		
		#metadata
		metadata = collection.meta['']
		if 'Subcollections' in collection.VARIABLES:
			for l in r[str(collection.VARIABLES.index('Subcollections'))]:
				metadata.update(collection.meta[l])

		for k in metadata.keys():
			if k in standard_meta:
				if k in standard_meta_formats.keys():
					val = coerceToFormat(metadata[k],standard_meta_formats[k])
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
				d['begin_date'] = td.convertToDT(min([tcs[k] for k in K]))
				d['end_date'] = td.convertToDT(max([tcs[k] for k in K]),convertMode='High')
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
	
	solr_interface.commit()
	
	#getQueryList and go through, for all things that are more than 1, index with query description
 
def getNums(collection,namelist):
	numlist = []
	for n in namelist:
		if n in collection.VARIABLES:
			numlist.append(collection.VARIABLES.index(n))
		else:
			numlist.append([collection.VARIABLES.index(m) for m in collection.ColumnGroups[n]])
	return numlist