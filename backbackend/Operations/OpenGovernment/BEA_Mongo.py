import os
import numpy as np
import tabular as tb
import hashlib
import cPickle as pickle
import re
from BeautifulSoup import BeautifulSoup, BeautifulStoneSoup, NavigableString
from System.Utils import MakeDir, Contents, listdir, IsDir, wget, uniqify, PathExists, ListUnion,RecursiveFileList


sourceroot = '../Data/OpenGovernment/BEA/'
root = '../Data/OpenGovernment/MongoSources/'	

def NumberVars(Z,V):
	newnames = []
	for n in Z.dtype.names:
		if n in V:
			newnames.append(str(V.index(n)))
		else:
			V.append(n)
			newnames.append(str(len(V) - 1))
	Z.dtype.names = newnames

###NIPA###

def MakeNIPACollectionObject(depends_on = sourceroot + 'NEA_NIPA/ParsedFiles/',creates = root + 'BEA_NIPA/'):
	NEACollectionsObjects(depends_on,creates)

def MakeNIPAUnpublishedCollectionObject(depends_on = sourceroot + 'NEA_NIPA_Unpublished/ParsedFiles/',creates = root + 'BEA_NIPA_Unpublished/'):
	NEACollectionsObjects(depends_on,creates)	

def MakeNIPAUnderlyingDetailCollectionObject(depends_on = sourceroot + 'NEA_NIPA_UnderlyingDetail/ParsedFiles/',creates = root + 'BEA_NIPA_UnderlyingDetail/'):
	inpath = depends_on
	L = []
	for l in listdir(inpath):
		if l.endswith('.tsv'):
			if '_Year' in l:
				L.append(inpath + l)
			else:
				M = tb.io.getmetadata(inpath + l)[0]
				if M['DateDivisions'] != 'Years':
					L.append(inpath + l)

	NEACollectionsObjects(depends_on,creates,L = L)	

def nea_dateparse(x):
	mmap = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']
	d = x.split('-')
	if len(d) == 1:
		return d[0] + 'X' + 'XX' 
	elif set(d[1].lower()) <= set(['i','v']):
		D = {'i':'1','ii':'2','iii':'3','iv':'4'}
		return d[0] + D[d[1].lower()] + 'XX'
	elif d[1].lower() in mmap:
		ind = str(mmap.index(d[1].lower())+1)
		ind = ind if len(ind) == 2 else '0' + ind
		return d[0] + 'X' + ind
		
def PI_dateparse(x):
	d = x.split('.')
	if len(d) == 1:
		return d[0] + 'X' + 'XX' 
	else:
		return d[0] + str(int(d[1]))  + 'XX'

def NEACollectionsObjects(inpath,outpath,L = None):

	MakeDir(outpath)
	
	if L == None:
		L = [inpath + x for x in listdir(inpath) if x.endswith('.tsv')]
	T = [x.split('/')[-1].split('_')[0].strip('.') for x in L]
	R = tb.tabarray(columns=[L,T],names = ['Path','Table']).aggregate(On=['Table'],AggFunc=lambda x : '|'.join(x))
	
	ColGroups = {}
	Metadict = {}
	Hashes = {}
	VARS = []
	for (j,r) in enumerate(R):
		ps = r['Path'].split('|')
		t = r['Table']
		print t
		assert t != ''
		X = [tb.tabarray(SVfile = p) for p in ps]
		X1 = [x[x['Line'] != ''].deletecols(['Category','Label','DisplayLevel']) for x in X]
		for i in range(len(X)):
			X1[i].metadata = X[i].metadata
			X1[i].coloring['Topics'] = X1[i].coloring['Categories']
			X1[i].coloring.pop('Categories')
			X1[i].coloring['TimeColNames'] = X1[i].coloring['Data']
			X1[i].coloring.pop('Data')
			for k in range(len(X1[i].coloring['TimeColNames'])):
				name = X1[i].coloring['TimeColNames'][k]
				X1[i].renamecol(name,nea_dateparse(name))
				
			
		if len(X1) > 1:		
			keycols = [x for x in X1[0].dtype.names if x not in X1[0].coloring['TimeColNames']]
			Z = tb.tab_join(X1,keycols=keycols)
		else:
			Z = X1[0]
			
		K = ['Category','Section','Units','Notes','DownloadedOn','LastRevised','Table','Footer']
		Z.metadata = {}
		for k in K:
			h = [x.metadata[k] for x in X if k in x.metadata.keys()]
			if h:
				if isinstance(h[0],str):
					Z.metadata[k] = ', '.join(uniqify(h))
				elif isinstance(h[0],list) or isinstance(h[0],tuple):
					Z.metadata[k] = uniqify(ListUnion(h))
				else:
					print 'metadata type for key', k , 'in table', t, 'not recognized.'
					
		Section = Z.metadata['Section']
		Z.metadata.pop('Section')
		Table = Z.metadata['Table'].strip().split('.')[-1].strip()
		Z.metadata.pop('Table')
		
		Z.coloring['LabelColumns'] = ['Table'] + Z.coloring['Topics']
		for k in Z.coloring.keys():
			if k in ColGroups.keys():
				ColGroups[k] = uniqify(ColGroups[k] + Z.coloring[k])
			else:
				ColGroups[k] = Z.coloring[k]
		
		Metadict[t] = Z.metadata

		Z = Z.addcols([[t]*len(Z),[Table]*len(Z),[Section]*len(Z),[t]*len(Z)],names=['TableNo','Table','Section','Subcollections'])
		NumberVars(Z,VARS)
		Z.saveSV(outpath + str(j) + '.tsv',metadata=['dialect','formats','names'])
		Hashes[j] = hashlib.sha1(open(outpath+ str(j) + '.tsv').read()).digest()
	
	AllKeys = uniqify(ListUnion([k.keys() for k in Metadict.values()]))
	AllMeta = {}
	for k in AllKeys:
		if all([k in Metadict[l].keys() for l in Metadict.keys()]) and len(uniqify([Metadict[l][k] for l in Metadict.keys()])) == 1:
			AllMeta[k] = Metadict[Metadict.keys()[0]][k]
			for l in Metadict.keys():
				Metadict[l].pop(k)
	
	Category = AllMeta['Category']
	AllMeta.pop('Category')
	AllMeta['Source'] = [('Agency',{'Name':'Department of Commerce','ShortName':'DOC'}),('Subagency',{'Name':'Bureau of Economic Analysis','ShortName':'BEA'}),('Program','National Economic Accounts'), ('Dataset',Category)]
	AllMeta['TopicHierarchy'] = ('Agency','Subagency','Program','Dataset','Section','Table')
	AllMeta['UniqueIndexes'] = [['TableNo','Line']]
	AllMeta['ColumnGroups'] = ColGroups
	AllMeta['DateFormat'] = 'YYYYqmm'
	AllMeta['VARIABLES'] = VARS
	AllMeta['sliceCols'] = ['Section','Table','Topics']
	AllMeta['phraseCols'] = ['Section','Table','Topics','Line','TableNo']

	
	Subcollections = Metadict
	Subcollections[''] = AllMeta
		
	F = open(outpath + '__metadata.pickle','w')
	pickle.dump({'Subcollections':Subcollections, 'Hashes':Hashes},F)
	F.close()
	
	
def MakeFATCollectionObject(depends_on = sourceroot + 'NEA_FixedAssetTables/ParsedFiles/',creates = root + 'BEA_FixedAsset/'):
	inpath = depends_on
	outpath = creates
	MakeDir(outpath)
	
	L = [inpath + x for x in listdir(inpath) if x.endswith('.tsv')]
	T = ['.'.join(x.split('/')[-1].split('_Table')[1].split('.')[:-2]) if 'Survey' not in x else 'S' + (x.split('/')[-1].split('_')[1].split('.')[0].split('Table')[1]) for x in L]

	GoodKeys = ['Category', 'Section', 'Units', 'Table', 'Footer']
	
	Metadict = {}
	ColGroups = {}
	Hashes={}
	VARS = []
	for i in range(len(L)):
		l = L[i]
		t = T[i]
		print t
		X = tb.tabarray(SVfile = l)
		X1 = X[X['Line'] != ''].deletecols(['Category','Label','DisplayLevel'])
		X1.metadata = X.metadata
		X = X1
		X.coloring['Topics'] = X.coloring['Categories']
		X.coloring.pop('Categories')
		X.coloring['TimeColNames'] = X.coloring['Data']
		X.coloring.pop('Data')
		for j in range(len(X.coloring['TimeColNames'])):
			name = X.coloring['TimeColNames'][j]
			X.renamecol(name,nea_dateparse(name))
			
		Section = X.metadata['Section'].split('-')[-1].strip()
		X.metadata.pop('Section')
		Table = X.metadata['Table']
		X.metadata.pop('Table')
		X.metadata['ColumnGroups'] = X.coloring
		X.metadata['LabelColumns'] = ['Table','Topics']
		
		X.coloring['LabelColumns'] = ['Table'] + X.coloring['Topics']
		for k in X.coloring.keys():
			if k in ColGroups.keys():
				ColGroups[k] = uniqify(ColGroups[k] + X.coloring[k])
			else:
				ColGroups[k] = X.coloring[k]
		
		Metadict[t] = dict([(k,X.metadata[k]) for k in GoodKeys if k in X.metadata.keys()])
		
		X = X.addcols([[t]*len(X),[Table]*len(X),[Section]*len(X),[t]*len(X)],names=['TableNo','Table','Section','Subcollections'])
		NumberVars(X,VARS)
		X.saveSV(outpath + str(i) + '.tsv',metadata=['dialect','names','formats'])
		Hashes[i] = hashlib.sha1(open(outpath+ str(i) + '.tsv').read()).digest()
	
	AllKeys = uniqify(ListUnion([k.keys() for k in Metadict.values()]))
	AllMeta = {}
	for k in AllKeys:
		if all([k in Metadict[l].keys() for l in Metadict.keys()]) and len(uniqify([Metadict[l][k] for l in Metadict.keys()])) == 1:
			AllMeta[k] = Metadict[Metadict.keys()[0]][k]
			for l in Metadict.keys():
				Metadict[l].pop(k)
				
	Category = AllMeta['Category']
	AllMeta.pop('Category')
	AllMeta['Source'] = [('Agency',{'Name':'Department of Commerce','ShortName':'DOC'}),('Subagency',{'Name':'Bureau of Economic Analysis','ShortName':'BEA'}),('Program','National Economic Accounts'), ('Dataset',Category)]
	AllMeta['TopicHierarchy'] =  ('Agency','Subagency','Program','Dataset','Section','Table')
	AllMeta['UniqueIndexes'] = [['TableNo','Line']]
	AllMeta['ColumnGroups'] = ColGroups
	AllMeta['DateFormat'] = 'YYYYqmm'
	AllMeta['VARIABLES'] = VARS
	AllMeta['sliceCols'] = ['Section','Table','Topics']
	AllMeta['phraseCols'] = ['Section','Table','Topics','Line','TableNo']
	
	Subcollections = Metadict
	Subcollections[''] = AllMeta

	F = open(outpath+'__metadata.pickle','w')
	pickle.dump({'Subcollections':Subcollections,'Hashes':Hashes},F)
	F.close()
	
	
def GDPByStateAreaCollectionsObjects(depends_on = (sourceroot + 'REA_GDPByState/ParsedFiles/',sourceroot + 'REA_GDPByMetropolitanArea/ParsedFiles/'),creates = root + 'BEA_RegionalGDP/'):
	inpath = depends_on[0]
	outpath = creates
	MakeDir(outpath)

	L = [x for x in listdir(inpath) if x.endswith('.tsv')]
	State = [x.split('/')[-1].split('_')[2] for x in L]
	Ind = [x.split('/')[-1].split('_')[1] for x in L]
	R = tb.tabarray(columns = [State,Ind,L],names =['State','IndClass','Path']).aggregate(On=['State','IndClass'],AggFunc = lambda x : '|'.join(x))
	
	GoodKeys = ['Category', 'description','footer', 'LastRevised']
	
	VARS = []
	Hashes = {}
	Metadict = {}
	LenR = len(R)
	ColGroups = {}
	for (i,r) in enumerate(R):
		state = r['State']
		indclass = r['IndClass']
		ps = r['Path'].split('|')
		print state,indclass
		
		X = [tb.tabarray(SVfile = inpath + p) for p in ps]
		for (j,x) in enumerate(X):
			x1 = x.deletecols(['Component'])
			x1.renamecol('Component Code','ComponentCode')
			x1.renamecol('Industry Code','IndustryCode')
			x1.renamecol('ParsedComponent','Component')
			x1.metadata = x.metadata
			x1.metadata['description'] = '.'.join(x1.metadata['description'].split('.')[1:]).strip()
			X[j] = x1
	
		if len(X) > 1:
			Z = tb.tab_join(X)
		else:
			Z = X[0]
		Z.coloring['IndustryHierarchy'] = Z.coloring['Categories']
		Z.coloring.pop('Categories')
		Z.coloring['TimeColNames'] = Z.coloring['Data']
		Z.coloring.pop('Data')
		for j in range(len(Z.coloring['TimeColNames'])):
			name = Z.coloring['TimeColNames'][j]
			Z.renamecol(name,nea_dateparse(name))
			
		Z.metadata = {}
		for k in GoodKeys:
			h = [x.metadata[k] for x in X if k in x.metadata.keys()]
			if h:
				if isinstance(h[0],str):
					Z.metadata[k] = uniqify(h)
				elif isinstance(h[0],list) or isinstance(h[0],tuple):
					Z.metadata[k] = uniqify(ListUnion(h))
				else:
					print 'metadata type for key', k , 'in table', t, 'not recognized.'

		Z.coloring['LabelColumns'] =  ['State','Industry','Component']		
		for k in Z.coloring.keys():
			if k in ColGroups.keys():
				ColGroups[k] = uniqify(ColGroups[k] + Z.coloring[k])
			else:
				ColGroups[k] = Z.coloring[k]		
		
		Metadict[state] = Z.metadata
		
		Z = Z.addcols([len(Z)*[indclass], len(Z)*['S']],names=['IndClass','Subcollections'])
		NumberVars(Z,VARS)
		Z.saveSV(outpath + str(i) + '.tsv',metadata=['dialect','names','formats'])
		Hashes[i] = hashlib.sha1(open(outpath+ str(i) + '.tsv').read()).digest()
		
	
	AllKeys = uniqify(ListUnion([k.keys() for k in Metadict.values()]))
	AllMeta = {}
	for k in AllKeys:
		if all([k in Metadict[l].keys() for l in Metadict.keys()]) and len(uniqify([Metadict[l][k] for l in Metadict.keys()])) == 1:
			AllMeta[k] = Metadict[Metadict.keys()[0]][k]
	
	Subcollections = {'S':AllMeta}
	Subcollections['S']['Title'] = 'GDP by State'
	
	del(Z)
		
	inpath = depends_on[1]
	L = ['gmpGDP.tsv', 'gmpPCGDP.tsv', 'gmpQI.tsv', 'gmpRGDP.tsv']

	Metadict = {}
	for (i,l) in enumerate(L):
		print l
		X = tb.tabarray(SVfile = inpath + l)
		X.renamecol('industry_id','IndustryCode')
		X.renamecol('component_id','ComponentCode')
		X1 = X.deletecols('component_name')
		X1.metadata = X.metadata
		X = X1	
		X.renamecol('ParsedComponent','Component')
		X.renamecol('industry_name','Industry')
		X.renamecol('area_name','Metropolitan Area')
		X.metadata['description'] = '--'.join(X.metadata['description'].split('--')[2:]).strip()

		for k in X.metadata.keys():
			if k not in GoodKeys: 
				X.metadata.pop(k)
		
		if 'Categories' in X.coloring.keys():
			X.coloring['IndustryHierarchy'] = X.coloring['Categories']
			X.coloring.pop('Categories')
		X.coloring['TimeColNames'] = X.coloring['Data']
		X.coloring.pop('Data')
		for j in range(len(X.coloring['TimeColNames'])):
			name = X.coloring['TimeColNames'][j]
			X.renamecol(name,nea_dateparse(name))

		X.coloring['LabelColumns'] = ['Metropolitan Area','Industry','Component']	
		for k in X.coloring.keys():
			if k in ColGroups.keys():
				ColGroups[k] = uniqify(ColGroups[k] + X.coloring[k])
			else:
				ColGroups[k] = X.coloring[k]				
		
		Metadict[l] = X.metadata
		X = X.addcols([['NAICS']*len(X),['M']*len(X)],names=['IndClass','Subcollections'])
		NumberVars(X,VARS)
		X.saveSV(outpath + str(i+LenR) + '.tsv',metadata=['dialect','names','formats'])
		Hashes[i+LenR] = hashlib.sha1(open(outpath+ str(i + LenR) + '.tsv').read()).digest()
	
	AllKeys = uniqify(ListUnion([k.keys() for k in Metadict.values()]))
	AllMeta = {}
	for k in AllKeys:
		if all([k in Metadict[l].keys() for l in Metadict.keys()]) and len(uniqify([Metadict[l][k] for l in Metadict.keys()])) == 1:
			AllMeta[k] = Metadict[Metadict.keys()[0]][k]
		
	Subcollections['M'] = AllMeta
	Subcollections['M']['Title'] = 'GDP by Metropolitan Area'

	AllMeta = {}
	AllMeta['Source'] = [('Agency',{'Name':'Department of Commerce','ShortName':'DOC'}),('Subagency',{'Name':'Bureau of Economic Analysis','ShortName':'BEA'}),('Program','Regional Economic Accounts'), ('Dataset','Regional GDP Data')]
	AllMeta['TopicHierarchy']  = ('Agency','Subagency','Program','Dataset','Category')
	AllMeta['UniqueIndexes'] = [['FIPS','IndustryCode','ComponentCode','IndClass']]
	AllMeta['ColumnGroups'] = ColGroups
	AllMeta['DateFormat'] = 'YYYYqmm'
	AllMeta['VARIABLES'] = VARS
	
	AllMeta['sliceCols'] = ['State', 'Component', 'IndClass', 'Metropolitan Area', 'IndustryHierarchy']	
	AllMeta['phraseCols'] = ['State', 'Component', 'IndClass', 'Metropolitan Area', 'IndustryHierarchy','Industry','Units','FIPS','Units']	 

	
	Subcollections[''] = AllMeta
		
	F = open(outpath+'__metadata.pickle','w')
	pickle.dump({'Subcollections':Subcollections,'Hashes':Hashes},F)
	F.close()	
	
	
def PersonalIncomeCollectionObject(depends_on = (sourceroot + 'REA_LocalAreaPersonalIncome/ParsedFiles/',sourceroot + 'REA_StateAnnualPersonalIncome/ParsedFiles/',sourceroot + 'REA_StateQuarterlyPersonalIncome/ParsedFiles/'),creates = root + 'BEA_PersonalIncome/'):


	inpath = depends_on[0]
	outpath = creates
	
	MakeDir(outpath)
	GoodKeys = ['Category', 'TableFootnotes','LastRevised']
	
	L = [x for x in listdir(inpath) if x.endswith('.tsv')]
	Metadict = {}
	Subcollections = {}
	
	ltpairs = []
	Hashes = {}
	VARS = []
	ColGroups = {}
	
	for (i,l) in enumerate(L):
		print l
		
		X = tb.tabarray(SVfile = inpath + l)	
		X.renamecol('AreaName','County')
		X.renamecol('Line','LineCode')
	
		linecode = X.metadata['Subject'].split('(')[-1].strip(' )')
		line =  '('.join(X.metadata['Subject'].split('(')[:-1]).strip(' (')
		table = X.metadata['Table'].split('(')[1].strip(' )')
		tabledescr =  X.metadata['Table'].split('(')[0].strip()

		subj = [x for x in eval(X.metadata['SubjectHierarchy']) if x]
		subjcols = [len(X)*[sub] for sub in subj]
		subjnames = ['Level_' + str(j) for j in range(len(subjcols))]
		
		X1 = X.addcols([[table]*len(X),[line]*len(X),[table + ',C']*len(X)] + subjcols,names=['Table','Line','Subcollections'] + subjnames)
		X1.metadata = X.metadata
		X = X1
		X.coloring['SubjectHierarchy'] = subjnames
		X.coloring['TimeColNames'] = X.coloring['Data']
		X.coloring.pop('Data')
		for j in range(len(X.coloring['TimeColNames'])):
			name = X.coloring['TimeColNames'][j]
			X.renamecol(name,PI_dateparse(name))
		X.coloring['LabelColumns'] = ['County','Table','Line']	
	
		for k in X.coloring.keys():
			if k in ColGroups.keys():
				ColGroups[k] = uniqify(ColGroups[k] + X.coloring[k])
			else:
				ColGroups[k] = X.coloring[k]				
		
		id = table + '_' + linecode
	
		ltpairs.append((linecode,table))
		
		Metadict[id] = dict([(k,v) for (k,v) in X.metadata.items() if k in GoodKeys])
		Metadict[id]['Description'] = tabledescr
		
		NumberVars(X,VARS)
		X.saveSV(outpath + str(i) + '.tsv',metadata=['dialect','names','formats'])
		Hashes[i] = hashlib.sha1(open(outpath+ str(i) + '.tsv').read()).digest()
	LAST = i
			
	LT = tb.tabarray(records = ltpairs,names = ['linecode','table']).aggregate(On=['table'],AggFunc = lambda x : ','.join(x))

	for r in LT:
		table = r['table']
		Lines = [table + '_' + linecode for linecode in r['linecode'].split(',')]
		AllKeys = uniqify(ListUnion([Metadict[l].keys() for l in Lines]))
		AllMeta = {}
		for k in AllKeys:
			if all([k in Metadict[l].keys() for l in Lines]) and len(uniqify([Metadict[l][k] for l in Lines])) == 1:
				AllMeta[k] = Metadict[Lines[0]][k]
										
		Subcollections[table] = AllMeta

	AllMeta = {}
	AllKeys = uniqify(ListUnion([Subcollections[l].keys() for l in LT['table']]))	
	for k in AllKeys:
		if all([k in Subcollections[l].keys() for l in LT['table']]) and len(uniqify([Subcollections[l][k] for l in LT['table']])) == 1:
			AllMeta[k] = Subcollections[LT['table'][0]][k]
			for l in LT['table']:
				Subcollections[l].pop(k)	
				
	AllMeta['Description'] = 'Local Area Personal Income data for all US counties, from the <a href="http://www.bea.gov/regional/reis/default.cfm?selTable=Single%20Line">Local Area Personal Income "Single Line of data for all counties"</a> data set under the <a href="http://www.bea.gov/regional/index.htm">Regional Economic Accounts</a> section of the <a href="http://www.bea.gov/">Bureau of Economic Accounts (BEA)</a> website.  For more information on the industry classifications, see the BEA web pages on  <a href="http://www.bea.gov/regional/definitions/nextpage.cfm?key=NAICS">NAICS (1997-2008)</a> and <a href="http://www.bea.gov/regional/definitions/nextpage.cfm?key=SIC">SIC (1963-1997)</a>.  With the exception of per capita estimates, all dollar estimates are in thousands of dollars. All employment estimates are number of jobs. All dollar estimates are in current dollars.'
	AllMeta['Title'] = 'Local Area Personal Income'				
	Subcollections['C'] = AllMeta

		

	#####State
	
	#state quarterly
	
	GoodKeys = ['Category',  'Units', 'TableFootnotes', 'footer', 'Table', 'LastRevised']
	
	Metadict = {}
	L1 = np.array(listdir(depends_on[2]))
	
	LIMIT = len(L1)
	for (i,l) in enumerate(L1[:LIMIT]):
		print l
		X = tb.tabarray(SVfile = depends_on[2] + l)
		t = X['Table'][0]
		X1 = X.deletecols(['First Year']).addcols(len(X)*[t + ',SQ'],names=['Subcollections'])
		X1.metadata = X.metadata
		X = X1
		X.renamecol('State FIPS','FIPS')
		X.renamecol('Line Code','LineCode')
		X.renamecol('Line Title','Line')
		X.renamecol('State Name','State')
		X.renamecol('Line Title Footnotes', 'Line Footnotes')
		
		
		if 'Categories' in X.coloring.keys():
			X.coloring['SubjectHierarchy'] = X.coloring['Categories']
			X.coloring.pop('Categories')		
		X.coloring['TimeColNames'] = X.coloring['Data']
		X.coloring.pop('Data')
		for j in range(len(X.coloring['TimeColNames'])):
			name = X.coloring['TimeColNames'][j]
			X.renamecol(name,PI_dateparse(name))
		X.coloring['LabelColumns'] = ['State','Table','Line']			
	
		for k in X.coloring.keys():
			if k in ColGroups.keys():
				ColGroups[k] = uniqify(ColGroups[k] + X.coloring[k])
			else:
				ColGroups[k] = X.coloring[k]	
		
		for k in X.metadata.keys():
			if k not in GoodKeys:
				X.metadata.pop(k)
		X.metadata['TableDescription'] = X.metadata['Table']
		X.metadata.pop('Table')
		NumberVars(X,VARS)		
		X.saveSV(outpath + str(i + LAST) + '.tsv',metadata=['dialect','names','formats'])
		Hashes[i+LAST] = hashlib.sha1(open(outpath+ str(i+LAST) + '.tsv').read()).digest()
		Metadict[l] = X.metadata
	LAST = i + LAST
	
	A1 = tb.tabarray(records = [x.split('_') for x in L1[:LIMIT]],names = ['Table','Start','End','State'])
	for t in uniqify(A1['Table']):
		tlist = L1[(A1['Table'] == t).nonzero()[0]]
		AllKeys = uniqify(ListUnion([Metadict[l].keys() for l in tlist]))
		AllMeta = {}
		for k in AllKeys:
			if all([k in Metadict[l].keys() for l in tlist]) and len(uniqify([Metadict[l][k] for l in tlist])) == 1:
				AllMeta[k] = Metadict[tlist[0]][k]
										
		Subcollections[t] = AllMeta
		Subcollections[t]['__query__'] = {'Table':t}		
			
	tlist = uniqify(A1['Table'])
	AllKeys = uniqify(ListUnion([Subcollections[l].keys() for l in tlist]))
	AllMeta = {}
	for k in AllKeys:
		if all([k in Subcollections[l].keys() for l in tlist]) and len(uniqify([Subcollections[l][k] for l in tlist])) == 1:
			AllMeta[k] = Subcollections[tlist[0]][k]
	AllMeta['Description'] = '<a href="http://www.bea.gov/regional/sqpi/">State Quarterly Personal Income</a> data set under the <a href="http://www.bea.gov/regional/index.htm">Regional Economic Accounts</a> section of the <a href="http://www.bea.gov/">Bureau of Economic Accounts (BEA)</a> website.'
	AllMeta['Title'] = 'State Quarterly Personal Income'
	Subcollections['SQ'] = AllMeta		


	#state annual 
	Metadict = {}
	GoodKeys = ['Category', 'TableFootnotes', 'Footnotes' 'Subcategory',  'footer',  'Table', 'LastRevised']

	L2 = [(x,x.split('/')[-1].split('.')[0].upper()) for x in RecursiveFileList(depends_on[1]) if x.endswith('.tsv')]
	
	L2a  = [x for x in L2 if len(x[1].split('_')) == 4]
	A2 = tb.tabarray(records = [[x[0]] + x[1].split('_') for x in L2a],names = ['Path','Table','Start','End','State'])
	g = lambda x,v : [y[v] for y in x if not any([y['Start'] >= z['Start'] and y['End'] <= z['End'] and z != y for z in x])]
	A2 = A2.aggregate(On = ['Table','State'], AggList = [('Start',lambda x : g(x,'Start'),['Start','End','Path']),('End',lambda x : g(x,'End'),['Start','End','Path']),('Path',lambda x : g(x,'Path'),['Start','End','Path'])])
	
	for (i,l) in enumerate(A2['Path']):
		print l
		X = tb.tabarray(SVfile = l)
		t = X['Table'][0]
		X1 = X.deletecols(['First Year']).addcols(len(X)*[t + ',SA'],names=['Subcollections'])
		X1.metadata = X.metadata
		X = X1
		X.renamecol('State FIPS','FIPS')
		X.renamecol('Line Code','LineCode')
		X.renamecol('Line Title','Line')
		X.renamecol('State Name','State')
		X.renamecol('Line Title Footnotes', 'Line Footnotes')
		
		if 'Categories' in X.coloring.keys():
			X.coloring['SubjectHierarchy'] = X.coloring['Categories']
			X.coloring.pop('Categories')		
		X.coloring['TimeColNames'] = X.coloring['Data']
		X.coloring.pop('Data')
		for j in range(len(X.coloring['TimeColNames'])):
			name = X.coloring['TimeColNames'][j]
			X.renamecol(name,PI_dateparse(name))
		X.coloring['LabelColumns'] = ['State','Table','Line']			
		
		for k in X.coloring.keys():
			if k in ColGroups.keys():
				ColGroups[k] = uniqify(ColGroups[k] + X.coloring[k])
			else:
				ColGroups[k] = X.coloring[k]	
		
		for k in X.metadata.keys():
			if k not in GoodKeys:
				X.metadata.pop(k)
		X.metadata['TableDescription'] = X.metadata['Table']
		X.metadata.pop('Table')		
		
		for k in X.metadata.keys():
			if k not in GoodKeys:
				X.metadata.pop(k)
				
		Metadict[l] = X.metadata
		NumberVars(X,VARS)
		X.saveSV(outpath + str(i + LAST) + '.tsv',metadata=['dialect','names','formats'])
		Hashes[i+LAST] = hashlib.sha1(open(outpath+ str(i+LAST) + '.tsv').read()).digest()
		Metadict[l] = X.metadata
	LAST = i + LAST
	
	for t in uniqify(A2['Table']):
		tlist = A2[A2['Table'] == t]['Path']
		AllKeys = uniqify(ListUnion([Metadict[l].keys() for l in tlist]))
		AllMeta = {}
		for k in AllKeys:
			if all([k in Metadict[l].keys() for l in tlist]) and len(uniqify([Metadict[l][k] for l in tlist])) == 1:
				AllMeta[k] = Metadict[tlist[0]][k]
										
		Subcollections[t] = AllMeta
		Subcollections[t]['__query__'] = {'Table':t}		
			
	tlist = uniqify(A2['Table'])
	AllKeys = uniqify(ListUnion([Subcollections[l].keys() for l in tlist]))
	AllMeta = {}
	for k in AllKeys:
		if all([k in Subcollections[l].keys() for l in tlist]) and len(uniqify([Subcollections[l][k] for l in tlist])) == 1:
			AllMeta[k] = Subcollections[tlist[0]][k]
			
	AllMeta['Description'] = '<a href="http://www.bea.gov/regional/spi/">State Annual Personal Income</a> data set under the <a href="http://www.bea.gov/regional/index.htm">Regional Economic Accounts</a> section of the <a href="http://www.bea.gov/">Bureau of Economic Accounts (BEA)</a> website.  U.S. DEPARTMENT OF COMMERCE--ECONOMICS AND STATISTICS ADMINISTRATION BUREAU OF ECONOMIC ANALYSIS--REGIONAL ECONOMIC INFORMATION SYSTEM STATE ANNUAL TABLES 1969 - 2008 for the states and regions of the nation September 2009 These files are provided by the Regional Economic Measurement Division of the Bureau of Economic Analysis. They contain tables of annual estimates (see below) for 1969-2008 for all States, regions, and the nation. State personal income estimates, released September 18, 2009, have been revised for 1969-2008 to reflect the results of the comprehensive revision to the national income and product accounts released in July 2009 and to incorporate newly available state-level source data. For the year 2001 in the tables SA05, SA06, SA07, SA25, and SA27, the industry detail is available by division-level SIC only. Tables based upon the North American Industry Classification System (NAICS) are available for 2001-07. Newly available earnings by NAICS industry back to 1990 were released on September 26, 2006.   For more information on the industry classifications, see the BEA web pages on  <a href="http://www.bea.gov/regional/definitions/nextpage.cfm?key=NAICS">NAICS (1997-2008)</a> and <a href="http://www.bea.gov/regional/definitions/nextpage.cfm?key=SIC">SIC (1963-1997)</a>.   Historical estimates 1929-68 will be updated in the next several months. TABLES The estimates are organized by table. The name of the downloaded file indicates the table. For example, any filename beginning with "SA05" contains information from the SA05 table. With the exception of per capita estimates, all dollar estimates are in thousands of dollars. All employment estimates are number of jobs. All dollar estimates are in current dollars. SA04 - State income and employment summary (1969-2008) SA05 - Personal income by major source and earnings by industry (1969-2001, 1990-2008) SA06 - Compensation of employees by industry (1998-2001, 2001-08) SA07 - Wage and salary disbursements by industry (1969-01, 2001-08) SA25 - Total full-time and part-time employment by industry (1969-2001, 2001-08) SA27 - Full-time and part-time wage and salary employment by industry (1969-2001, 2001-08) SA30 - State economic profile (1969-08) SA35 - Personal current transfer receipts (1969-08) SA40 - State property income (1969-2008) SA45 - Farm income and expenses (1969-2008) SA50 - Personal current taxes (this table includes the disposable personal income estimate) (1969-08) DATA (*.CSV) FILES The files containing the estimates (data files) are in comma-separated-value text format with textual information enclosed in quotes.  (L) Less than $50,000 or less than 10 jobs, as appropriate, but the estimates for this item are included in the total. (T) SA05N=Less than 10 million dollars, but the estimates for this item are included in the total. SA25N=Estimate for employment suppressed to cover corresponding estimate for earnings. Estimates for this item are included in the total. (N) Data not available for this year. If you have any problems or comments on the use of these data files call or write: Regional Economic Information System Bureau of Economic Analysis (BE-55) U.S. Department of Commerce Washington, D.C. 20230 Phone (202) 606-5360 FAX (202) 606-5322 E-Mail: reis@bea.gov'
	
	Subcollections['SA'] = AllMeta

	#state annual summary
	Metadict = {}
	L2b  = [x for x in L2 if len(x[1].split('_')) == 1]	
	for (p,t) in L2b:
		print p
		X = tb.tabarray(SVfile = p)
		t = X['Table'][0]
		X1 = X.deletecols(['First Year']).addcols(len(X)*[t + ',SA_S,SA'],names=['Subcollections'])
		X1.metadata = X.metadata
		X = X1
		X.renamecol('State FIPS','FIPS')
		X.renamecol('Line Code','LineCode')
		X.renamecol('Line Title','Line')
		X.renamecol('State Name','State')
		X.renamecol('Line Title Footnotes', 'Line Footnotes')

		if 'Categories' in X.coloring.keys():
			X.coloring['SubjectHierarchy'] = X.coloring['Categories']
			X.coloring.pop('Categories')		
		X.coloring['TimeColNames'] = X.coloring['Data']
		X.coloring.pop('Data')
		for j in range(len(X.coloring['TimeColNames'])):
			name = X.coloring['TimeColNames'][j]
			X.renamecol(name,PI_dateparse(name))
		X.coloring['LabelColumns'] = ['State','Table','Line']			
		
		for k in X.coloring.keys():
			if k in ColGroups.keys():
				ColGroups[k] = uniqify(ColGroups[k] + X.coloring[k])
			else:
				ColGroups[k] = X.coloring[k]			
		
		for k in X.metadata.keys():
			if k not in GoodKeys:
				X.metadata.pop(k)
				
		X.metadata['TableDescription'] = X.metadata['Table']
		X.metadata.pop('Table')		
		
		Subcollections[t] = X.metadata

		Metadict[p] = X.metadata
		NumberVars(X,VARS)
		X.saveSV(outpath + str(i + LAST) + '.tsv',metadata=['dialect','names','formats'])
		Hashes[i+LAST] = hashlib.sha1(open(outpath+ str(i+LAST) + '.tsv').read()).digest()

	LAST = i + LAST
	tlist = Metadict.keys()
	AllKeys = uniqify(ListUnion([Metadict[l].keys() for l in tlist]))
	AllMeta = {}
	for k in AllKeys:
		if all([k in Metadict[l].keys() for l in tlist]) and len(uniqify([Metadict[l][k] for l in tlist])) == 1:
			AllMeta[k] = Metadict[tlist[0]][k]
									
	Subcollections['SA_S'] = AllMeta
	
				
	AllMeta = {}
	AllMeta['Source'] = [('Agency',{'Name':'Department of Commerce','ShortName':'DOC'}),('Subagency',{'Name':'Bureau of Economic Analysis','ShortName':'BEA'}),('Program','Regional Economic Accounts'), ('Dataset','Personal Income')]
	AllMeta['TopicHierarchy']  = ('Agency','Subagency','Dataset','Category','Subcategory','SubjectHierarchy')
	AllMeta['UniqueIndexes'] = [['FIPS','Table','LineCode']]
	AllMeta['ColumnGroups'] = ColGroups
	AllMeta['DateFormat'] = 'YYYYqmm'
	AllMeta['VARIABLES'] = VARS
		
	AllMeta['sliceCols'] = ['County','State','Table','SubjectHierarchy']	
	AllMeta['phraseCols'] = ['County','State','Table','SubjectHierarchy','Line','FIPS','LineCode']	
	
	Subcollections[''] = AllMeta
	
	F = open(outpath+'__metadata.pickle','w')
	pickle.dump({'Subcollections':Subcollections,'Hashes':Hashes},F)
	F.close()	
		