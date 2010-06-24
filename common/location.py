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
('b','Combined Statistical Area','CSA','csa_code','csa_name'),
('B','Metropolitan Division','METDIV','metdiv_code','metdic_name'),
('c','County','USCounties','county_code','county_name'),
('C','country',None,None,None),
('d','Public Use Microdata Area -- 1%',None,None,None),
('D','Census Division','CensusDivisions','census_division_code','census_division_name'),
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
('r','Census Region','CensusRegions','census_region_code','census_region_name'),
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

SPACE_RELATIONS = [('O','C'),
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
('s','A'),
('r','D'),
('D','s'),
('C','r'),
('s','b'),
('s','B'),
('s','m'),
]

import networkx as nx
def makeHierarchy(V,E):
    """Convert hierarchy into a network graph and creates a dictionary of ordered list to describe the hiearchy"""
    G = nx.DiGraph()
    for v in V:
        G.add_node(v)
    for e in E:
        if is_string_like(e[0]):
            s = e[0]
        else:
            s = e[0][0]
        if is_string_like(e[1]):
            t = e[1]
        else:
            t = e[1][0]
        G.add_edge(s,t)
            
    H = dict([(o,v.keys()) for (o,v) in nx.all_pairs_shortest_path(G).items()]) 
    T = nx.topological_sort(G)
    for k in H.keys():
        H[k] = [j for j in T if j in H[k]]
    
    return [G,H]
        

[SPACE_HIERARCHY_GRAPH,SPACE_HIERARCHY] = makeHierarchy(SPACE_CODES,SPACE_RELATIONS)
[SPACE_HIERARCHY_GRAPH_R,SPACE_HIERARCHY_R] = makeHierarchy(SPACE_CODES,[(y,x) for (x,y) in SPACE_RELATIONS])

        
def divisions(l):
    """Converts space object (dict) into the levels auto expands FIPS codes"""
    return [SPACE_DIVISIONS[x] for x in l.keys() if x != 'f' and SPACE_DIVISIONS.has_key(x)] + ([SPACE_DIVISIONS[x] +  ' FIPS' for x in l['f'] if SPACE_DIVISIONS.has_key(x)] if 'f' in l.keys() else [])
    
    
def divisions2(l):
    """Converts space object (dict) into the levels auto expands FIPS codes"""
    return [SPACE_DIVISIONS[x] for x in l.keys() if x != 'f' and SPACE_DIVISIONS.has_key(x)] + ([SPACE_DIVISIONS[x]  for x in l['f'] if x not in l.keys() and SPACE_DIVISIONS.has_key(x)] if 'f' in l.keys() else [])
    
def phrase(l):
    """Human readable phrase"""
    return ', '.join([SPACE_DIVISIONS[x] + '=' + y  for (x,y) in l.items() if x != 'f' and SPACE_DIVISIONS.has_key(x)] + ([SPACE_DIVISIONS[x] + ' FIPS=' + y for (x,y) in l['f'].items() if SPACE_DIVISIONS.has_key(x)] if 'f' in l.keys() else []))


def phrase2(l):
    """Human readable phrase"""
    return ', '.join([y  for (x,y) in l.items() if x != 'f' and SPACE_DIVISIONS.has_key(x)] + ([SPACE_DIVISIONS[x] + ' FIPS=' + y for (x,y) in l['f'].items() if SPACE_DIVISIONS.has_key(x)] if 'f' in l.keys() else []))


def modPhrase(l):
    """phrase for phrase matching indexing"""
    return ' '.join(['"' + SPACE_DIVISIONS[x] + '=' + y + '"' for (x,y) in l.items() if x != 'f' and SPACE_DIVISIONS.has_key(x)] + (['"' + SPACE_DIVISIONS[x] + ' FIPS=' + y + '"' for (x,y) in l['f'].items() if SPACE_DIVISIONS.has_key(x)] if 'f' in l.keys() else []))

def integrate(l1,l2):
    """Combines two space objects"""
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
    """Intersects two space objects"""
    if l1 and l2 :
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
    """Generates queries for get in mongo"""
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
    """compares spaceQuery to ol and if they match then returns nil
    Otherwise it removes the unnessary spaceQuery properites to match to the correct remaining format"""
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

def getLowest(keys):
    return [k for k in keys if len(keys.intersection(SPACE_HIERARCHY[k])) == 1]
    
def SpaceComplete(x):
    """FIPS -> names and upwards when possible"""
    if 'f' in x.keys():
        x = x.copy()
        iFIPS = ListUnion([SPACE_HIERARCHY_R[c] for c in x['f'].keys()])
        iFIPS = [c for c in iFIPS if c not in x.keys()]
        Cset = [c + '=' + x['f'][c] for c in x['f'].keys() if uniqify(x['f'][c]) != ['0']]
        if iFIPS and Cset:
      
            X = eval(urllib2.urlopen('http://localhost:8000/geo/fips/?' + '&'.join(Cset)).read())
            if len(X) == 1:
                X = convertToCodes(X[0])
                x['f'] = X['f']
                for c in X.keys():
                    if c not in x.keys():
                        x[c] = X[c]
            
    return x
                    

def queryToSolr(spaceQuery):
    """Make query to solr"""
    if isinstance(spaceQuery,list) or isinstance(spaceQuery,tuple):
        div = ['"' + x + '"' for x in  divisions(convertQS(spaceQuery))]
        fq = 'spatialDivisions:' + (div[0] if len(div) == 1 else '(' + ' AND '.join(div) + ')')
        return fq
        
    elif hasattr(spaceQuery,'keys'):
        if 'bounds' in spaceQuery.keys():
            level = spaceQuery['level']
            phrases = [modPhrase(convertToCodes(x)) for x in eval(urllib2.urlopen('http://localhost:8000/geo/regions/' + level + '/?bounds=' + spaceQuery['bounds']).read())]
        
        elif 'radius' in spaceQuery.keys():
            level = spaceQuery['level']
            phrases = [modPhrase(convertToCodes(x)) for x in eval(urllib2.urlopen('http://localhost:8000/geo/regions/' + level + '/?radius=' + spaceQuery['radius'] + '&center=' + spaceQuery['center'] + ('&units=' + spaceQuery['units'] if 'units' in spaceQuery.keys() else '')).read())]
        else:
            spaceQuery = spaceQuery.copy()
            SpaceComplete(spaceQuery)
            phrases = [modPhrase(spaceQuery)]
                    
        return 'commonLocation:' + '(' + ' OR '.join(phrases) + ')' 

