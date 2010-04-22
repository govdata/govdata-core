import pymongo.son as son
from common.utils import is_string_like, ListUnion, uniqify, Flatten, rhasattr,rgetattr

#see http://www.census.gov/population/www/metroareas/metrodef.html
#see http://www.census.gov/geo/www/ansi/ansi.html for FIPS
#see http://www.census.gov/geo/www/cob/bdy_files.html for boundary files
#see http://www2.census.gov/cgi-bin/shapefiles2009/national-files for more boundary files
#see http://wiki.openstreetmap.org/wiki/Karlsruhe_Schema
#see http://wiki.openstreetmap.org/wiki/Map_Features#Places

SPACE_CODE_MAP = [('a','Address',),
('A','Area Code'),
('c','County'),
('C','country'),
('d','Public Use Microdata Area -- 1%'),
('e','Public Use Microdata Area -- 5%'),
('f','FIPS'),
('g','Congressional District'),
('I','Incoporated Place'),
('i','Island'),
('j','County Subdivision'),
('k','School District -- Unified'),
('l','State Legislative District -- Lower'),
('L','State Legislative District -- Upper'),
('m','Metropolitan/Micropolitan Statistical Area'),
('n','New England City and Town Area'),
('O','Continent'),
('p','Postal Code'),
('q','School District -- Elementary'),
('Q','School District -- Secondary'),
('r','Region'),
('S','State Code'),
('s','State'),
('t','Town'),
('T','Census Tract'),
('u','Urban Area'),
('v','Voting District'),
('V','Village'),
('W','City'),
('X','Undefined'),
('Z','5-Digit ZCTA'),
('z','3-Digit ZCTA'),
]

(SPACE_CODES,SPACE_DIVISIONS) = zip(*SPACE_CODE_MAP)
SPACE_DIVISIONS = dict(zip(SPACE_CODES,SPACE_DIVISIONS))

SPACE_HIERARCHY_RELATIONS = [('O','C'),
('C','s'),
('s',('S','=')),
('s','c'),
('s','g'),
('c','j'),
('c','i'),
('s','z'),
('z','Z'),
('z','T'),
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

def queryToSolr(spaceQuery):

	if isinstance(spaceQuery,list) or isinstance(spaceQuery,tuple):
		div = ['"' + x + '"' for x in  divisions(convertQS(spaceQuery))]
		fq = 'spatialDivisions:' + (div[0] if len(div) == 1 else '(' + ' AND '.join(div) + ')')
		return fq
		
	elif hasattr(spaceQuery,'keys'):
	
		return 'commonLocation:' + phrase(spaceQuery)
	
		
			
	
