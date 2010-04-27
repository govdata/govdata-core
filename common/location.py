import pymongo.son as son
from common.utils import is_string_like, ListUnion, uniqify, Flatten, rhasattr,rgetattr
import urllib2

#see http://www.census.gov/population/www/metroareas/metrodef.html
#see http://www.census.gov/geo/www/ansi/ansi.html for FIPS
#see http://www.census.gov/geo/www/cob/bdy_files.html for boundary files
#see http://www2.census.gov/cgi-bin/shapefiles2009/national-files for more boundary files
#see http://wiki.openstreetmap.org/wiki/Karlsruhe_Schema
#see http://wiki.openstreetmap.org/wiki/Map_Features#Places

SPACE_CODE_MAP = [('a','Address',None,None,None),
('A','Area Code',None,None,None),
('c','County','USCounties','county_code','county_name'),
('C','country',None,None,None),
('d','Public Use Microdata Area -- 1%',None,None,None),
('e','Public Use Microdata Area -- 5%',None,None,None),
('f','FIPS',None,None,None),
('g','Congressional District',None,None,None),
('I','Incoporated Place',None,None,None),
('i','Island',None,None,None),
('j','County Subdivision',None,None,None),
('k','School District -- Unified',None,None,None),
('l','State Legislative District -- Lower',None,None,None),
('L','State Legislative District -- Upper',None,None,None),
('m','Metropolitan/Micropolitan Statistical Area','CBSA','cbsa_code','cbsa_name'),
('n','New England City and Town Area',None,None,None),
('O','Continent',None,None,None),
('p','Postal Code',None,None,None),
('q','School District -- Elementary',None,None,None),
('Q','School District -- Secondary',None,None,None),
('r','Region',None,None,None),
('S','State Abbreviation',None,None,None),
('s','State','USStates','state_code','state_name'),
('t','Town',None,None,None),
('T','Census Tract','CensusTracts','census_tract_code','census_tract_name'),
('u','Urban Area',None,None,None),
('v','Voting District',None,None,None),
('V','Village',None,None,None),
('W','City',None,None,None),
('X','Undefined',None,None,None),
('Z','ZCTA5','FiveDigitZCTAs','zcta5_code','zcta5_name'),
('z','ZCTA3',None,None,None),
]

(SPACE_CODES,SPACE_DIVISIONS,SPACE_DB_TABLES,LEVEL_CODES,LEVEL_NAMES) = zip(*SPACE_CODE_MAP)
SPACE_DIVISIONS = dict(zip(SPACE_CODES,SPACE_DIVISIONS))
SPACE_DB_TABLES = dict(zip(SPACE_CODES,SPACE_DB_TABLES))
LEVEL_CODES = dict(zip(SPACE_CODES,LEVEL_CODES))
LEVEL_NAMES = dict(zip(SPACE_CODES,LEVEL_NAMES))
iLEVEL_CODES = dict([(y,x) for (x,y) in LEVEL_CODES.items()])
iLEVEL_NAMES = dict([(y,x) for (x,y) in LEVEL_NAMES.items()])

def convertToCodes(x):
	n = [(iLEVEL_NAMES[l],x[l]) for l in x.keys() if l in iLEVEL_NAMES.keys()]
	m =  dict([(iLEVEL_CODES[l],x[l]) for l in x.keys() if l in iLEVEL_CODES.keys()])
	return dict(n + ([('f',m)]  if m else []))

SPACE_HIERARCHY_RELATIONS = [('O','C'),
('C','s'),
('s',('S','=')),
('s','c'),
('s','g'),
('c','j'),
('c','i'),
('s','z'),
('z','Z'),
('c','T'),
('s','l'),
('s','L'),
('s','A')
]

import networkx as nx
SPACE_HIERARCHY_GRAPH = nx.DiGraph()
for g in SPACE_CODES:
	SPACE_HIERARCHY_GRAPH.add_node(g)
for g in SPACE_HIERARCHY_RELATIONS:
	if is_string_like(g[1]):
		SPACE_HIERARCHY_GRAPH.add_edge(*g)
	else:
		SPACE_HIERARCHY_GRAPH.add_edge(g[0],g[1][0])
SPACE_HIERARCHY = dict([(o,v.keys()) for (o,v) in nx.all_pairs_shortest_path(SPACE_HIERARCHY_GRAPH).items()])

SPACE_HIERARCHY_GRAPH_R = nx.DiGraph()
for g in SPACE_CODES:
	SPACE_HIERARCHY_GRAPH_R.add_node(g)
for g in SPACE_HIERARCHY_RELATIONS:
	if is_string_like(g[1]):
		SPACE_HIERARCHY_GRAPH_R.add_edge(g[1],g[0])
	else:
		SPACE_HIERARCHY_GRAPH_R.add_edge(g[1][0],g[0])		
SPACE_HIERARCHY_R = dict([(o,v.keys()) for (o,v) in nx.all_pairs_shortest_path(SPACE_HIERARCHY_GRAPH_R).items()])
		
def divisions(l):
	assert set(l.keys()) <= set(SPACE_DIVISIONS.keys()), 'Unrecognized spatial key codes.'
	return [SPACE_DIVISIONS[x] for x in l.keys() if x != 'f'] + ([SPACE_DIVISIONS[x] +  ' FIPS' for x in l['f']] if 'f' in l.keys() else [])
	
def phrase(l):
	return ', '.join([SPACE_DIVISIONS[x] + '=' + y  for (x,y) in l.items() if x != 'f'] + ([SPACE_DIVISIONS[x] + ' FIPS=' + y for (x,y) in l['f'].items()] if 'f' in l.keys() else []))
	
	
def integrate(l1,l2):
	if not l1:
		return l2
	elif not l2:
		return l1
	else:
		D = l1.copy()
		for (k,v) in l2.items():
			if is_string_like(v):
				D[k] = v
			else:
				assert is_instance(v,dict)
				D[k].update(v)
		return D
		
		
def intersect(l1,l2):
	I = dict([(k,l1[k]) for k in set(l1.keys()).intersection(l2.keys())])
	
	for (k,v) in l2.items():
		if k in I.keys() and v != I[k]:
			if is_string_like(v):
				for j in SPACE_HIERARCHY[k]:
					if j in I.keys():
						I.pop(j)
			else:
				D = intersect(I[k],v)
				if D:
					I[k] = D
				else:
					I.pop(k)
		
	return I
	
	
def generateQueries(spaceQuery):	

	Q = {}
	if isinstance(spaceQuery,list) or isinstance(spaceQuery,tuple):
		for x in uniqify(spaceQuery):
			Q[tuple(x.split('.'))] = {'$exists':True}
	
	elif hasattr(spaceQuery,'keys'):
		spaceQuery = convertSQ(spaceQuery)
		for x in spaceQuery.keys():
			Q[tuple(x.split('.'))] = spaceQuery[x]
			
	return Q

def checkQuery(spaceQuery,ol):
	if isinstance(spaceQuery,list) or isinstance(spaceQuery,tuple):
		for k in spaceQuery:
			if rhasattr(ol,k.split('.')):
				spaceQuery.pop(k)
				
		return True
	elif hasattr(spaceQuery,'keys'):
		spaceQuery = convertSQ(spaceQuery)
		OK = True
		for k in spaceQuery.keys():
			sq = spaceQuery[k]
			if rhasattr(ol,k.split('.')):
				spaceQuery.pop(k)
				if rgetattr(ol,k.split('.')) != sq:
					OK = False
		return OK
			
def convertSQ(sq):
	return dict([(k,v) for (k,v) in sq.items() if k != 'f'] + ([('f.' + k,v) for (k,v) in sq['f'].items() ]  if 'f' in sq.keys() else []))
	
def convertQS(sq):
	nFIPS = [k for k  in sq if not k.startswith('f.')]
	FIPS = [k[2:] for k in sq if k.startswith('f.')]

	return dict(zip(nFIPS,['']*len(nFIPS)) + ([('f',dict(zip(FIPS,['']*len(FIPS))))] if FIPS else []))


def SpaceComplete(x):
	if 'f' in x.keys():
		iFIPS = [c for c in x['f'].keys() if c not in x.keys()]
		if iFIPS:
			X = convertToCodes(eval(urllib2.urlopen('http://localhost:8000/fips/?' + '&'.join([c + '=' + x['f'][c] for c in iFIPS])).read())[0])
			x['f'] = X['f']
			for c in iFIPS:
				x[c] = X[c]

def queryToSolr(spaceQuery):

	if isinstance(spaceQuery,list) or isinstance(spaceQuery,tuple):
		div = ['"' + x + '"' for x in  divisions(convertQS(spaceQuery))]
		fq = 'spatialDivisions:' + (div[0] if len(div) == 1 else '(' + ' AND '.join(div) + ')')
		return fq
		
	elif hasattr(spaceQuery,'keys'):
		if 'bounds' in spaceQuery.keys():
			level = spaceQuery['level']
			phrases = [phrase(convertToCodes(x)) for x in eval(urllib2.urlopen('http://localhost:8000/regions/' + level + '/?bounds=' + spaceQuery['bounds']).read())]
		
		elif 'radius' in spaceQuery.keys():
			level = spaceQuery['level']
			phrases = [phrase(convertToCodes(x)) for x in eval(urllib2.urlopen('http://localhost:8000/regions/' + level + '/?radius=' + spaceQuery['radius'] + '&center=' + spaceQuery['center'] + ('&units=' + spaceQuery['units'] if 'units' in spaceQuery.keys() else '')).read())]
		else:
			spaceQuery = spaceQuery.copy()
			SpaceComplete(spaceQuery)
			phrases = [phrase(spaceQuery)]
					
		return 'commonLocation:' + '(' + ' OR '.join(phrases) + ')' 

