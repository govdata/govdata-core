#!/usr/bin/env python

from common.mongo import Collection
from common.utils import IsFile, listdir, is_string_like, ListUnion
import backend.api as api
import sunburnt
import itertools
import json
import pymongo.json_util as ju
import pymongo.son as son



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


def indexCollection(collectionName):

	collection = Collection(collectionName)
	sliceCols = collection.sliceCols
	if hasttr(collection,'contentCols'):
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
	standard_meta_formats = {'keywords':'tplist','last_mo
	
	for r in R:
		d = {}
		d['mongoQuery'] = json.dumps(son.SON([('_id',r['_id'])]),default=ju.default)
		d['sliceContents'] = ' '.join(ListUnion([[str(r[str(x)])] if isinstance(x,int) else [str(r[str(xx)]) for xx in x] for x in contentColNames]))
		d['slicePhrases'] = '|||'.join(ListUnion([[s + ':' + str(r[str(x)])] if isinstance(x,int) else [s + ':' +  str(r[str(xx)]) for xx in x] for (s,x) in zip(phraseCols,contentColNames)]))
		d['columnNames'] = '|||'.join(r.keys())
		d['volume'] = 1
		d['dimensions'] = len(columnNames)
		
		d['sourceSpec'] = json.dumps(R.Source,default=ju.default)
		SourceNameDict = dict([(k,R.Source[k]['Name'] if isinstance(R.Source[k],dict) else R.Source[k]) for k in R.Source.keys()])
		SourceAbbrevDict = dict([(k,R.Source[k]['ShortName']) for k in R.Source.keys() if isinstance(R.Source[k],dict) and 'ShortName' in R.Source[k].keys() ])
		d['agency'] = SourceNameDict['Agency']
		d['subagency'] = SourceNameDict['Subagency']
		d['dataset'] = SourceNameDict['Dataset']
		for k in set(SourceNameDict.keys()).difference(['Agency','Subagency','Dataset']):
			d['source_' + k.lower()] = SourceNameDict[k]
		for k in SourceAbbrevDict.keys():
			d['source_' + k.lower() + '_acronym'] = SourceAbbrevDict[k]
			
		metadata = dict([(l,collection.meta[l]) for l in r['Subcollections']])
		for k in standard_meta:
			if k in metadata.keys():
				d[k] = metadata[k]
		#other metadata
		
		#time/date fields
	
	
	
	#getQueryList and go through, for all things that are more than 1, index with query description
 
def getNums(collection,namelist):
	numlist = []
	for n in namelist:
		if n in collection.VARIABLES
			numlist.append(collection.VARIABLES.index(n))
		else:
			numlist.append([collection.VARIABLES.index(m) for m in collection.ColumnGroups[n]])
	return numlist