import pymongo.son as son
from common.utils import is_string_like, ListUnion, uniqify, Flatten, rgetattr

TIME_CODE_MAP = [('Y','Year',None),('h','Half',range(1,3)),('q','Quarter',range(1,5)),('m','Month',range(1,13)),('d','DayOfMonth',range(1,32)),('U','WeekOfYear',range(1,53)),('w','DayOfWeek',range(1,8)),('j','DayOfYear',range(1,366)),('H','HourOfDay',range(1,25)),('M','MinuteOfHour',range(0,60)),('S','Second',range(0,60)),('Z','TimeZone',None)]
(TIME_CODES,TIME_DIVISIONS,TIME_RANGES) = zip(*TIME_CODE_MAP)
TIME_DIVISIONS = dict(zip(TIME_CODES,TIME_DIVISIONS))
TIME_RANGES = dict(zip(TIME_CODES,TIME_RANGES))
# For each code it gives you a hierarchical list of direct descendents in order
TIME_HIERARCHY_RELATIONS = dict([('Y',['h','q','m','U','j']),('m',['d']),('d',['H']),('H',['M']),('M',['S']),('U',['w']),('w',['H']),('j',['H'])])

def getHierarchy(f):
	"""Converts a string format into a time hierarchy object template"""
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
	"""Recursive Helper function for getHierarchy"""
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
	"""Returns the anonymous format function"""
	fbreaks = [0] + [i for i in range(1,len(format)) if format[i] != format[i-1]] + [len(format)]
	fblocks = zip(fbreaks[:-1],fbreaks[1:])
	fs = [format[b[0]] for b in fblocks]
	H = getHierarchy(fs)
	return getFunc(H,fs,fblocks)
						
						
def getFunc(H,fs,fblocks):
	"""Helper for mongotimeformatter"""
	def F(x):
		Bdict = dict([(f,x[a:b]) for (f,(a,b)) in zip(fs,fblocks)])
		return applyHierarchy(H,Bdict)
	return F
	
	
def applyHierarchy(H,bdict):
	"""Helper for getFunc"""
	if isinstance(H,list):
		L = [applyHierarchy(h,bdict) for h in H]
		return son.SON([l for l in L if l[1]])
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
	
def tObjFlatten(tObj):
	"""Flattens a Time object from hierachical to flat"""
	S = {}
	for l in tObj.keys():
		if hasattr(tObj[l],'keys'):
			S[l] = tObj[l]['']
			S.update(tObjFlatten(tObj[l]))
	return S

def generateQueries(DateFormat,timeQuery):
	"""Converts nice DateFormat string and simple query format for time and generate mongo icky
	DateFormat : String e.g. YYYYmmdd
	timeQuery : Dict keys = format, begin, end, on
	These are not necessary"""
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
	
def getPathsTo(m,H):
	"""Helper for generateQueries"""
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

def getLowest(tObj):
	"""Helper which for any given time object gets the lowest relevant level(s)"""
	lowest =[]
	for k in tObj.keys():
		if hasattr(tObj[k],'keys'):
			if tObj[k].keys() == ['']:
				lowest.append(k)
			else:
				lowest += getLowest(tObj[k])
	return lowest

#import mx	
#import mx.DateTime as DateTime
import datetime

def convertToDT(tObj,convertMode = 'Low'):
	"""Convert to python DateTime format"""
	assert convertMode in ['Low','High']
	ftObj = tObjFlatten(tObj)
	tlist = []
	default_val = {'Y':0,'m':1,'d':1,'H':0,'M':0,'S':0,'MS':0,'Z':None}
	for k in ('Y','m','d'):
		if k in ftObj.keys():
			tlist.append(ftObj[k])
		else:
			tlist.append(default_val[k])
	if 'q' in ftObj.keys() and 'm' not in ftObj.keys():
		if convertMode == 'Low':
			tlist[1] = 3*(ftObj['q']-1) + 1
		elif convertMode == 'High':
			tlist[1] = 3*ftObj['q']

	return datetime.date(*tlist)
	
def convertToSolrDT(tObj,convertMode = 'Low'):
	"""Convert to solr DateTime format
	it only handles year month day not hour minute second"""
	DT = convertToDT(tObj,convertMode=convertMode)
	return DT.strftime('%Y-%m-%d') + ('T00:00:00.999Z' if convertMode == 'Low' else 'T23:59:59.999Z')


def queryToSolr(timeQuery):
	"""converts timeQuery for use in find ie solr"""
	#TODO: query between march and december fails handle things that don't start with year
	F = mongotimeformatter(timeQuery['format'])
	for k in timeQuery.keys():
		if k != 'format':
			timeQuery[k] = F(timeQuery[k])
		
	if timeQuery.keys() == ['format']:
		divisions = [TIME_DIVISIONS[x] for x in uniqify(timeQuery['format'])]
		
		fq = 'dateDivisions:' + (divisions[0] if len(divisions) == 1 else '(' + ' AND '.join(divisions) + ')')
	
	else:
		if 'on' in timeQuery.keys():
			start = timeQuery['on']
			end = timeQuery['on']
		else:
			start = timeQuery['start'] if 'start' in timeQuery.keys() else None
			end = timeQuery['end'] if 'end' in timeQuery.keys() else None
			
		start = convertToSolrDT(start,convertMode='High') if start else None
		end = convertToSolrDT(end) if end else None

		fq = []
		if start:
			fq.append('begin_date:[* TO ' + start + ']')
		if end:
			fq.append('end_date:[' + end + ' TO *]')
			
	return fq
	
	
def makemin(old,new):
	if old:
		return min(new,old)
	else:
		return new
		
def makemax(old,new):
	if old:
		return max(new,old)
	else:
		return new

	
def phrase(tObj,convertMode = 'Low'):
	"""Constructs human readable phrase from mongo dateObject"""
	dateObj = convertToDT(tObj,convertMode = convertMode)
	ftObj = tObjFlatten(tObj)
	X =  [('w', '%A' ), ('m','%B') , ('d', '%d'), ('Y','%Y')]
	s = ' '.join([x for (m,x) in X if m in ftObj.keys()])
	H = dateObj.strftime(s)
	if 'q' in ftObj.keys():
		H = 'Q' + str(ftObj['q']) + ' ' + H
	return H

def checkQuery(timeQuery,OverallTime):
	"""compares timeQuery to OverallTime and if they match then returns nil
	Otherwise it removes the unnessary timeQuery properites to match to the correct remaining format"""
	ot = OverallTime['date']
	otf = OverallTime['format']
	
	if 'format' in timeQuery.keys():
		tQF = timeQuery['format']
	else:
		tQF = otf
	
	rtF = [i for i in range(len(tQF)) if tQF[i] in otf]
	if rtF:
		rtQ = {}
		ktF = [i for i in range(len(tQF)) if tQF[i] not in otf]
		for k in timeQuery.keys():
			if rtF:
				rtQ[k] = ''.join([timeQuery[k][i] for i in rtF])
			if ktF:
				timeQuery[k] = ''.join([timeQuery[k][i] for i in ktF])
			else:
				timeQuery.pop(k)
				
		if 'on' in rtQ.keys():
			rtQ['begin'] = rtQ['on']
			rtQ['end'] = rtQ['on']
		
		F1 = mongotimeformatter(rtQ['format'])
		for k in rtQ:
			if k != 'format':
				rtQ[k] = F1(rtQ[k])
		F2 = mongotimeformatter(otf)
		OT = F2(ot)
		if 'begin' in rtQ.keys() and 'end' not in rtQ.keys():
			OK = OT >= rtQ['begin']
		elif 'end' in rtQ.keys() and 'begin' not in rtQ.keys():
			OK = OT <= rtQ['end']
		elif 'end' in rtQ.keys() and 'begin' in rtQ.keys():
			OK = rtQ['begin'] <= OT <= rtQ['end']
			
	else:
		OK = True
		
	return OK
	
def reverse(format):
	fbreaks = [0] + [i for i in range(1,len(format)) if format[i] != format[i-1]] + [len(format)]
	fblocks = zip(fbreaks[:-1],fbreaks[1:])
	fs = [format[b[0]] for b in fblocks]
	ls = [b[1] - b[0] for b in fblocks]

	def F(tObj):
		t = tObjFlatten(tObj)
		return ''.join([('0'*( len(l) - len(str(t[f])) ) + str(t[f])) if f in t.keys() else len(l)*'X' for (l,f) in zip(ls,fs)])
			
	return F