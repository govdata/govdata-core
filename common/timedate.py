import pymongo as pm
from common.utils import is_string_like, ListUnion, uniqify

TIME_CODE_MAP = [('Y','Year',None),('h','Half',range(1,3)),('q','Quarter',range(1,5)),('m','Month',range(1,13)),('d','DayOfMonth',range(1,32)),('U','WeekOfYear',range(1,53)),('w','DayOfWeek',range(1,8)),('j','DayOfYear',range(1,366)),('H','HourOfDay',range(1,25)),('M','MinuteOfHour',range(0,60)),('S','Second',range(0,60)),('Z','TimeZone',None)]
(TIME_CODES,TIME_DIVISIONS,TIME_RANGES) = zip(*TIME_CODE_MAP)
TIME_HIERARCHY_RELATIONS = dict([('Y',['h','q','m','U','j']),('m',['d']),('d',['H']),('H',['M']),('M',['S']),('U',['w']),('w',['H']),('j',['H'])])

def getHierarchy(f):
	H = []
	f = list(f)
	assert set(f) <= set(TIME_CODES), 'Some codes are not recognized.'
	while f:
		for tc in TIME_CODES:
			if tc in f:
				f.remove(tc)
				break
		h = getHierarchyBelow(tc,f)
		H.append(h)
		f = [ff for ff in f if ff not in Flatten(h)]
	return H
	
	
def getHierarchyBelow(tc,f):
	if tc not in TIME_HIERARCHY_RELATIONS.keys():
		return (tc,)
	else:
		h = [hh for hh in TIME_HIERARCHY_RELATIONS[tc] if hh in f]
		if h:
			f = [ff for ff in f if ff not in h]
			return (tc,[getHierarchyBelow(hh,f) for hh in h])
		else:
			return (tc,)
	

def mongotimeformatter(format):
	fbreaks = [0] + [i for i in range(1,len(format)) if format[i] != format[i-1]] + [len(format)]
	fblocks = zip(fbreaks[:-1],fbreaks[1:])
	fs = [format[b[0]] for b in fblocks]
	H = getHierarchy(fs)
	return getFunc(H,fs,fblocks)
						
						
def getFunc(H,fs,fblocks):
	def F(x):
		Bdict = dict([(f,x[a:b]) for (f,(a,b)) in zip(fs,fblocks)])
		return applyHierarchy(H,Bdict)
	return F
	
	
def applyHierarchy(H,bdict):
	if isinstance(H,list):
		L = [applyHierarchy(h,bdict) for h in H]
		return pm.son.SON([l for l in L if l[1]])
	elif isinstance(H,tuple):
		assert 1 <= len(H) <= 2
		assert is_string_like(H[0])
		F = {}
		if not 'X' in bdict[H[0]]:
			F[''] = int(bdict[H[0]])
		if len(H) == 2:
			assert isinstance(H[1],list)
			F.update(applyHierarchy(H[1],bdict))
		return (H[0],F)
	
def Flatten(L):
	S = []
	for l in L:
		if isinstance(l,list) or isinstance(l,tuple):
			S += Flatten(l)
		else:
			S.append(l)
	return S



def generateQueries(DateFormat,timeQuery):	
	timeQueryFormat = timeQuery['format'] if 'format' in timeQuery.keys() else DateFormat
	tQFset = set(timeQueryFormat)
	tFset = set(DateFormat)

	
	if tQFset <= tFset:
		tQHier = getHierarchy(tQFset)
		Hier = getHierarchy(DateFormat)
		
		mergedTimeFormat = ''.join(tFset.difference(tQFset)) + timeQueryFormat
		timeFormatter = mongotimeformatter(mergedTimeFormat)
		zeroLen = len(tFset.difference(tQFset))
		
		tQHier0 = [x[0] for x in tQHier]
		Hier0 = [x[0] for x in Hier]
		basePathDict = dict([(m,getPathsTo(m,Hier)) for m in tQHier0])
		
		Q = {}
		for (k,op) in [('begin','$gte'),('end','$lt')]:
			if k in timeQuery.keys():
				timeObj = timeFormatter('X'*zeroLen + timeQuery[k])
				for m in basePathDict.keys():
					for p in basePathDict[m]:
						if p in Q.keys():
							Q[p][op] = rgetattr(timeObj,p)
						else:
							Q[p] = {op: rgetattr(timeObj,p)}
												
		if 'on' in timeQuery.keys():
			timeObj = timeFormatter('X'*zeroLen + timeQuery['on'])
			paths = uniqify(ListUnion([getPathsTo(m,Hier) for m in tQFset]))
			for p in paths:
				p = p + ('',)
				Q[p] = rgetattr(timeObj,p)
		
		if not set(timeQuery.keys()).intersection(['begin','end','on']):
			for m in set(tQHier0):
				for p in basePathDict[m]:
					p = p + ('',)
					Q[p] = {'$exists':True}

		
		return Q

def rgetattr(r,a):
	for aa in a:
		if aa in r.keys():
			r = r[aa]
		else:
			return None
	return r
	
def getPathsTo(m,H):
	if isinstance(H,list):
		L =  ListUnion([getPathsTo(m,h) for h in H])
		return [l for l in L if l]
	elif isinstance(H,tuple):
		
		if H[0] == m:
			return [(m,)]
		else:
			if len(H) == 2:
				return [(H[0],) + y for y in getPathsTo(m,H[1])]
			else:
				return [()]
				