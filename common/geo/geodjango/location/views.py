# Create your views here.

from django.http import HttpResponse
import location.models as models
import common.location as loc
import json
from common.utils import Rgetattr, Rhasattr, uniqify, ListUnion, dictListUniqify

def main(request):
	return HttpResponse(str(request))
	
def geodb(request,level_code):

	if request.method == 'GET':
		g = request.GET
	elif request.method == 'POST':
		g = request.POST
	
	return HttpResponse(json.dumps(geodbGuts(g,level_code)))
	
	
def geodbGuts(g,level_code):

	level = loc.SPACE_DB_TABLES[level_code]
	
	methodstr = g['method']
	
	method = getattr(getattr(models,level).objects,methodstr)	
	
	if 'querylist' in g:
		querylist = g['querylist']
	else:
		keylist =['field','query','type','pattern','radius','units']
		querylist = [dict( [(k,g[k]) for k in keylist if k in g])]	

	argdict = {}
	for d in querylist:
		field = d['field']
		type = d['type'] if 'type' in d.keys() else ''			
		key = field + ('__' + type if type else '')
			
		if type == 'relate':
			geom = d['query']
			pattern = d['pattern']
			results = (geom,pattern)
		elif type in ['distance_lt','distance_lte','distance_gt','distance_gte','dwithin']:
			geom = d['query']
			radius = float(d['radius'])
			
			if 'units' in d.keys():
				units = d['units']
				from django.contrib.gis.measure import D
				dobj = D(**{units:radius})
			else:
				dobj = radius
				
			args = (geom,dobj)
		else:
			args = d['query']
		
		argdict[key] = args
 	
 	results = method(**argdict)
 	
 	if 'return' in g:
 		returnVals = g['return'].split(',')
 		if 'name' in returnVals:
 			returnVals[returnVals.index('name')] = loc.LEVEL_NAMES[level_code]
 		if 'code' in returnVals:
 			returnVals[returnVals.index('code')] = loc.LEVEL_CODES[level_code]
 	else:
	 	name = loc.LEVEL_NAMES[level_code]
 		code = loc.LEVEL_CODES[level_code]
 		returnVals = [name,code]

	results = [dict([(a,str(Rgetattr(x,a.split('.')))) for a in returnVals if Rhasattr(x,a.split('.'))]) for x in results]
 
	return results
	

def fips(request):

	g = request.GET
	
	h = dict(g.items())

	return HttpResponse(json.dumps(fipsHierarchy(h)))

	
def fipsGuts(g):
	
	keys = set([k for k in g])
	
	lowest = loc.getLowest(keys)					
	assert len(lowest) == 1	
	level_code = lowest[0]
	
	D = {'method':'filter'}
	codes = keys.intersection(loc.SPACE_HIERARCHY_R[level_code])
	querylist =  [{'field': loc.LEVEL_CODES[code],  'query' : g[code]} for code in codes]
	D['querylist'] = querylist	
	D['return'] = ','.join([loc.LEVEL_CODES[code] for code in loc.SPACE_HIERARCHY_R[level_code] if loc.LEVEL_CODES[code]] + [loc.LEVEL_NAMES[level_code]])
	
	R = uniqify([tuple(x.items()) for x in geodbGuts(D,level_code)])
	R = [dict([(k,v) for (k,v) in r if v != 'None']) for r in R]
	
	
	newkeys = set(loc.SPACE_HIERARCHY_R[level_code]).difference([level_code])
	keydict = dict([(code,loc.LEVEL_CODES[code]) for code in newkeys])
	
	if newkeys:
		for (i,r) in enumerate(R):
			newg = dict([(k,r[keydict[k]]) for k in newkeys if keydict[k] in r.keys()])			
			if newg:
				res = fipsGuts(newg)
				R[i] = [dict(R[i].items() + rr.items()) for rr in res]
			else:
				R[i] = [R[i]]

		R = ListUnion(R)
		
	return R
	
	
def fipsHierarchy(g,action = 'contains'):

	if 'action' in g:
		action = g['action']
		g.pop('action')
		
	keys = set([k for k in g])
	lowest = loc.getLowest(keys)
	assert len(lowest) == 1	
	level_code = lowest[0]

	H = [x for x in loc.SPACE_HIERARCHY_R[level_code] if loc.LEVEL_CODES[x]]

	D = {'method':'filter'}
	codes = keys.intersection(H)
	querylist =  [{'field': loc.LEVEL_CODES[code],  'query' : g[code]} for code in codes]
	D['querylist'] = querylist	
	D['return'] = ','.join([loc.LEVEL_CODES[code] for code in H] + [loc.LEVEL_NAMES[level_code],'geom'])
	
	R = dictListUniqify(geodbGuts(D,level_code))
	
	Gdict = {}
	for r in R:
		rmg = tuple([(k,v) for (k,v) in r.items() if k != 'geom'])
		if rmg not in Gdict.keys():
			Gdict[rmg] = [r['geom']]
		else:
			if r['geom'] not in Gdict[rmg]:
				Gdict[rmg].append(r['geom'])
	
	U = [h for h in H if h != level_code]
	
	H = []
	for x in Gdict.keys():
		for y in Gdict[x]:
			V = [dict(x + (('geom',{level_code:y}),))]
			for u in U:
				newV = []
				for v in V:
					d = {'method':'filter', 'querylist': [{'field':'geom','type':action,'query':gg} for gg in v['geom'].values()], 'return':'name,code,geom'} 
					R = dictListUniqify(geodbGuts(d,u))
		
					for r in R:
						h = v.copy()
						h['geom'] = {level_code:y}
						for k in r.keys():
							if k != 'geom':	
								h[k] = r[k]
							else:
								h['geom'][u] = r['geom']
						newV.append(h)
				V = newV
			
					
			H += V		

	for h in H:
		h.pop('geom')
		
	H = dictListUniqify(H)
	
	return H
	
def regions(request,level_code):
	g = request.GET
	return HttpResponse(json.dumps(regionsGuts(g,level_code)))
	
def regionsGuts(g,level_code):
	
	D  = {'method':'filter'}
	if 'return' in g:
		D['return'] = g['return']
	D['field'] = 'geom'
	
	if 'bounds' in g:
		w,s,e,n = g['bounds'].split(',')
		if 'type' in g:
			D['type'] = g['type']
		else:
			D['type'] = 'intersects'
		
		D['query'] = 'POLYGON((' + ', '.join([w + ' ' + n , w + ' ' + s, e + ' ' + s , e + ' ' + n, w + ' ' + n]) + ' ))'
		
	elif 'radius' in g and 'center' in g:
		D['radius'] = g['radius']
		x,y = g['center'].split(',')
		D['type'] = 'distance_lte'
		D['query'] = 'POINT(' + x + ' ' + y + ')'
		if 'units' in g:
			D['units'] = g['units']
	
	R = uniqify([tuple(x.items()) for x in geodbGuts(D,level_code)])
	R = [dict([(k,v) for (k,v) in r if v != 'None']) for r in R]
	
	return R
	
	
	
	