import os
from System.Utils import MakeDir,Contents,listdir,wget,PathExists, strongcopy,uniqify,ListUnion
import Operations.htools as htools
from BeautifulSoup import BeautifulSoup,BeautifulStoneSoup
import tabular as tb
import numpy as np
from System.MetaData import AttachMetaData,loadmetadata
from System.Protocols import activate,ApplyOperations2
import time,re
import cPickle as pickle
import hashlib

root = '../Data/OpenGovernment/BLS/'
MongoRoot = '../Data/OpenGovernment/MongoSources/'
protocol_root = '../Protocol_Instances/OpenGovernment/BLS/'


#MAIN_SPLITS = ['cu', 'cw', 'su', 'ap', 'li', 'pc', 'wp', 'ei', 'ce', 'sm', 'jt', 'bd', 'oe', 'lu', 'la', 'ml', 'nw', 'ci', 'cm', 'eb', 'ws', 'le', 'cx', 'pr', 'mp', 'ip', 'in', 'fi', 'ch', 'ii']
MAIN_SPLITS = ['cu', 'ap', 'pr','sm','bd','lu']

def WgetMultiple(dd, fname, maxtries=10):
	link = dd['URL']
	opstring = '--user-agent="Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.5; en-US; rv:1.9.1.7) Gecko/20091221 Firefox/3.5.7"'
	time.sleep(5)
	for i in range(maxtries):
		wget(link, fname, opstring)
		F = open(fname,'r').read().strip()
		if F.startswith('<!DOCTYPE HTML'):
			return
		else:
			print 'download of ' + link + ' failed: ' + F[:20]
			time.sleep(15)
			
	print 'download of ' + link + ' failed after ' + str(maxtries) + ' attempts'
	return
	
def BLS_mainparse1(page,x):
	Soup = BeautifulSoup(open(page),convertEntities=BeautifulStoneSoup.HTML_ENTITIES)
	c1 = lambda x : x.name == 'h1' and 'id' in dict(x.attrs).keys()
	c2 = lambda x : x.name == 'td' and 'colspan' in dict(x.attrs).keys() and dict(x.attrs)['colspan'] == "8"
	c3 = lambda x : x.name == 'tr' and 'class' in dict(x.findParent().findParent().attrs).keys() and dict(x.findParent().findParent().attrs)['class'] == 'matrix-table' and x.findAll('td') and x.findAll('td')[-1].findAll('a') and 'ftp://' in str(x.findAll('td')[-1].findAll('a')[0])
	p1 = Contents
	p2 = Contents
	p3 = lambda x : (Contents(x.findAll('td')[0]).replace('\t','').strip().replace('\r\n',': '),str(dict(x.findAll('td')[-1].findAll('a')[0].attrs)['href']).strip(' /') + '/')
	N = ['Level1','Level2',['Level3','URL']]
	T = htools.MakeTable(Soup,[c1,c2,c3],[p1,p2,p3],N)
	T.coloring['Categories']=['Level1','Level2','Level3']
	return T


def Ogetter(x):
	
	L = [(i,y) for (i,y) in enumerate(x['File']) if 'Current' not in y.split('.')]
	La = [(i,y) for (i,y) in L if 'AllData' in y]
	if La:
		L = La + [(i,y) for (i,y) in L if not '.data.' in y]
	if len(x.dtype.names) == 1:
		return list(zip(*L)[1])
	else:
		name = [n for n in x.dtype.names if n != 'File'][0]
		return [x[name][i] for i in zip(*L)[0]]

def BLS_mainparse2(page,x):	
	if x['URL'].strip('/').split('/')[-2] == 'time.series' and x['Level1'] != 'International' and not (x['URL'].strip('/').split('/')[-1] in ['oe','ce'] and x['Level1'] == 'Pay & Benefits') and x['URL'].strip('/').split('/')[-1] in MAIN_SPLITS:
		print 'parsing', page
		Soup = BeautifulSoup(open(page))
		A = Soup.findAll('a')
		Records = [(Contents(a),str(dict(a.attrs)['href']),'txt') for a in A]
		Records = [r for r in Records if 'Current' not in r[0].split('.')]
		RecordsR = [r for r in Records if 'AllData' in r[0]]
		if RecordsR:
			Records = RecordsR + [r for r in Records if not '.data.' in r[0]]
		T = tb.tabarray(records = Records,names = ['File','URL','Extension'])
		T.coloring['Categories'] = ['File']
		return T

def CleanOpen(path):

	try:
		[recs,md] = tb.io.loadSVrecs(path,delimiter = '\t',formats='str',headerlines=1,linefixer = lambda x : x.replace('\x00',''))
	except:
		print 'Problem reading records from', path
	else:
		ln = len(md['names'])
		recs = [r[:ln] for r in recs if r != []]
		
		if len(recs) > 0:
			X = tb.tabarray(recs,names = md['names'])
			return X
		
def main_splitfunc(x):
	return x['URL'].strip(' /').split('/')[-2]


	
def identifybasepath(base,datadir):
	L = listdir(datadir)
	L1 = [x.split('.')[-2] for x in L]
	L2 = [x.split('.')[-2].replace('_','') for x in L]
	if base in L1:
		return datadir + L[L1.index(base)]
	elif base in L2:
		return datadir + L[L2.index(base)]
	elif base.replace('_','') in L1:
		return datadir + L[L1.index(base.replace('_',''))]
	elif base.replace('_','') in L2:
		return datadir + L[L2.index(base.replace('_',''))]

		
def identifycode(name,names):
	
	tries = [name + '_code', name + '_codes',name.replace('_','') + '_code',name.replace('_','') + '_codes',name,name.replace('_','')]

	for t in tries:
		if t in names:
			return t
	
def identifybase(name):
	if name.endswith('_code'):
		return name[:-5]
	elif name.endswith('_codes'):
		return name[:-6]
	else:	
		return name


@activate(lambda x : x[0], lambda x :x[1])		
def parse_series(datadir,outpath,units=''):
	SPs = [datadir + l for l in listdir(datadir) if l.endswith('.series.txt')]
	assert len(SPs) == 1, 'Wrong number of series paths.'
	serpath = SPs[0]
	F = open(serpath,'rU')
	names = F.readline().rstrip('\n').split('\t')

	codefiles = {}
	codenames = {}
	bases = {}
	for name in names:
		base = identifybase(name)
		basepath = identifybasepath(base,datadir)
		if basepath != None:
			print name, basepath
			codefile = CleanOpen(basepath)
			if codefile != None:
				codename = identifycode(base,codefile.dtype.names)
				if codename != None:		
					codenames[name] = codename
					codefiles[name] = codefile[[n for n in codefile.dtype.names if n.startswith(base)]]
					bases[name] = base
				else:
					print '\n\nWARNING: Problem with code for' , name , 'in file', basepath, '\n\n'
			else:
				print '\n\nWARNING: Can\'t seem to open', basepath
		else:
			print '\n\nWARNING: Problem with finding basepath for ', name , 'in', datadir					
	
	blocksize = 750000


	done = False

	while not done:
		lines = [F.readline().rstrip('\n').split('\t') for i in range(blocksize)]
		lines = [l for l in lines if l != ['']]
		if len(lines) > 0:
			X = tb.tabarray(records = lines,names = names)
			NewCols = []
			NewNames = []
			for name in names:
				if name in codenames.keys():
					codefile = codefiles[name]
					base = bases[name]
					codename = codenames[name]
					Xn = np.array([xx.strip() for xx in X[name]])
					Cn = np.array([xx.strip() for xx in codefile[codename]])
					[S1,S2] = tb.fast.equalspairs(Xn,Cn)
		
					NewCols += [codefile[n][S1] for n in codefile.dtype.names if n != codename]
					NewNames += [n for n in codefile.dtype.names if n != codename]
			X = X.addcols(NewCols,	names = NewNames)
			X.coloring['NewNames'] = NewNames
			
			if units != '':
				if ' ' not in units:
					if units:
						X.coloring['Units'] = [units]
				elif not units.startswith('if '):
					X = X.addcols([[units]*len(X)], names=['Units'])
				else:
					X = X.addcols([[rec['earn_text'] if rec['tdata_text'] == 'Person counts (number in thousands)' else rec['pcts_text'] for rec in X]], names=['Units'])

			tb.io.appendSV(outpath,X,metadata=True)
		else:
			done = True
	

@activate(lambda x : tuple([x[1],x[2],x[3]]), lambda x : (x[4],x[5]))
def makemetadata(code,manifest,keywords,datadir,outfile1,outfile2,depends_on = ('../Data/OpenGovernment/BLS/ProcessedManifest_2_HandAdditions.tsv',)):
	Z = getcategorydata(code,manifest,keywords)

	dirl = np.array(listdir(datadir))
	
	pr = lambda x : x.split('!')[-1][:-4]
	p=re.compile('\([^\)]*\)')
	
	tps = [l for l in dirl if l.endswith('.txt.txt')]
	if tps:
		textpath = datadir + tps[0]
		[SD,things] = ParseTexts(textpath,code)
		FNs = [p.sub('',things[pr(y).lower()]).replace(' ,',',').replace(',,',',') if pr(y).lower() in things.keys() else '' for y in dirl]
		FNs = [z.split('=')[1] if '=' in z and not ' =' in z else z for z in FNs]
	else:
		SD = ''
		FNs = len(dirl)*['']
		
	Z['description'] = SD
	
	cfs = [l for l in dirl if l.endswith('.contacts.txt')]
	if cfs:
		contactfile = datadir + cfs[0]
		ctext = open(contactfile,'rU').read().strip()
		if '<html>' in ctext.lower():
			clines = ctext.split('\n')
			fb = [i for i in range(len(clines)) if clines[i].strip() == ''][0]
			ctext = '\n'.join(clines[fb+1:])
		ctext = ctext.strip(' *\n').replace('\n\n','\n')	
	else:
		ctext = ''
		
	Z['ContactInfo'] = ctext
	f = open(outfile1,'w')
	pickle.dump(Z,f)
	f.close()

	Y = tb.tabarray(SVfile = depends_on[0])
	Y.sort(order = ['File'])

	
	dirlp = np.array([pr(y) for y in dirl])
	[A,B] = tb.fast.equalspairs(dirlp,Y['File'])
	if (B>A).any():
		print 'adding hand-made content to', dirlp[B>A]
		for k in (B>A).nonzero()[0]:
			FNs[k] = Y['FileName'][A[k]]	
	
	D = tb.tabarray(columns=[dirl,FNs], names = ['Path','FileName'])
	
	D.saveSV(outfile2,metadata = True)	
	
	

def getcategorydata(code,manifest,keywords):

	X = tb.tabarray(SVfile = manifest)
	Y = tb.tabarray(SVfile = keywords)[['Code','Keywords']]
	
	Codes = np.array([x.split('/')[-2] for x in X['URL']])
		
	x = X[Codes == code][0]	
	topic = str(x['Level1'])
	subtopic = str(x['Level2'])
	xx = str(x['Level3'])
	if len(xx.split(':')) > 1 and '-' in xx.split(':')[1]:
		Dataset = xx.split(':')[0].strip()		
		y = ':'.join(xx.split(':')[1:]).strip('() ')
		ProgramName = y.split('-')[0].strip()
		ProgramAbbr = y.split('-')[1]
	elif xx.strip().endswith(')'):
		Dataset = xx[:xx.find('(')].strip()
		ProgramAbbr = xx[xx.find('('):].strip(' ()')
		if not ProgramAbbr.isupper():
			ProgramAbbr = ''
		ProgramName = ''
	else:
		Dataset = xx
		ProgramName = ''
		ProgramAbbr = ''
	
	y = Y[Y['Code'] == code]
	keywords = str(y['Keywords'][0])
		
	return {'Topic':topic,'Subtopic':subtopic,'Dataset':Dataset,'ProgramName':ProgramName,'ProgramAbbr':ProgramAbbr,'keywords':keywords,'DatasetCode':code}
	
	
	
def ParseTexts(textpath,code):

	F = open(textpath,'rU').read().strip(' \n*').split('\n')
	bs = [i for i in range(len(F)) if 'Survey Description:' in F[i] or 'Program Description:' in F[i]]
	if bs:
		b = bs[0]
		if F[b].split(':')[1].strip() == '':
			bb = b + 2
		else:
			bb = b

		e = [i for i in range(bb,len(F)) if F[i].strip() == ''][0]
		SD  = ' '.join(F[b:e+1])
	else:
		SD = ''
		print 'No description in ', textpath.split('/')[-1]

	pb = [i for i in range(len(F)) if 'Section 2' in F[i]][0] 
	b = [i for i in range(pb,len(F)) if F[i].strip().startswith(code)][0]
	e = [i for i in range(b,len(F)) if F[i].strip() == '' or F[i].startswith('==')][0]
	things = {}
	p = re.compile('- ')
	for x in F[b:e]:
		if x.strip().startswith(code):
			v = p.split(x.strip())[0].strip().replace(' ','').lower()
			k = '-'.join(x.strip().split('-')[1:]).strip().replace('\t',' ')
			things[v] = k
		else:
			things[v] += ' ' + x.strip().replace('\t',' ')

			
	return [SD,things]
	

@activate(lambda x : x[0], lambda x : x[1])	
def processtextfile(datadir,outfile):
	dirl = listdir(datadir)
	tps = [l for l in dirl if l.endswith('.txt.txt')]
	if tps:
		textpath = datadir + tps[0]
		strongcopy(textpath,outfile)

	else:
		F = open(outfile,'w')
		F.write('No documentation file found in.')
		F.close()



def BLS_Initialize2(creates = protocol_root):
	MakeDir(creates)

def BLS_mainInstantiator(creates = protocol_root + 'main.py'):
	L = [{'Parser':BLS_mainparse1,'Getter':WgetMultiple},{'Parser':BLS_mainparse2,'Splitter':(main_splitfunc,MAIN_SPLITS)},None]
	D = htools.hsuck('http://www.bls.gov/data/', root + 'MainData/', L, ipath=creates,write=False)
	D += [('initialize_finalparsed',MakeDir,(root + 'MainData/final_parsed/',))]
	D += [('initialize_finalparsed_' + v ,MakeDir, (root + 'MainData/final_parsed/' + v + '/',)) for v in MAIN_SPLITS]
	D += [('metadata_' + v ,makemetadata, (v,root + 'MainData/Manifest_1.tsv', root + 'Keywords.txt', root + 'MainData/Downloads_2/' + v + '/',root + 'MainData/final_parsed/' + v + '/overall_metadata.pickle',root + 'MainData/final_parsed/' + v + '/filenames.tsv')) for v in MAIN_SPLITS]
	D += [('textfileprocess_' + v ,processtextfile, (root + 'MainData/Downloads_2/' + v + '/',root + 'MainData/final_parsed/' + v + '/' + v + '_documentation.txt')) for v in MAIN_SPLITS]
	D += [('parse_series_' + v , parse_series, (root + 'MainData/Downloads_2/' + v + '/',root + 'MainData/final_parsed/' + v + '/' + v + '_series.tsv')) for v in MAIN_SPLITS]
	D += [('MakeMongoSource_' + v, MakeMongoSource,(root + 'MainData/final_parsed/' + v + '/overall_metadata.pickle',root + 'MainData/final_parsed/' + v + '/' + v + '_documentation.txt',root + 'MainData/final_parsed/' + v + '/' + v + '_series.tsv',root + 'MainData/final_parsed/' + v + '/filenames.tsv',root + 'MainData/Downloads_2/' + v + '/',MongoRoot + 'BLS_' + v + '/')) for v in MAIN_SPLITS]
	D += [('MakeMongoSourceFlat_' + v, MakeMongoSourceFlat,(root + 'MainData/final_parsed/' + v + '/overall_metadata.pickle',root + 'MainData/final_parsed/' + v + '/' + v + '_documentation.txt',root + 'MainData/final_parsed/' + v + '/' + v + '_series.tsv',root + 'MainData/final_parsed/' + v + '/filenames.tsv',root + 'MainData/Downloads_2/' + v + '/',MongoRoot + 'BLS_' + v + '_Flat/')) for v in MAIN_SPLITS[:1]]
	
	ApplyOperations2(creates,D)


import Operations.OpenGovernment.OpenGovernment as OG		
def addBLS_ap(creates = OG.CERT_PROTOCOL_ROOT + 'BLS_ap.py'):
	OG.backendProtocol('BLS_ap')
	
def addBLS_bd(creates = OG.CERT_PROTOCOL_ROOT + 'BLS_bd.py'):
	OG.backendProtocol('BLS_bd')
		

@activate(lambda x : (x[0],x[1],x[2],x[3],x[4]),lambda x : x[5])
def MakeMongoSource(metafile, docfile, seriesfile, filelistfile, sourcedir, outdir):
	MakeDir(outdir)
	
	
	M = pickle.load(open(metafile))
	D = {}
	for x in ['ContactInfo','description','keywords']:
		D[x] = M[x]
	D['Source'] = [('Agency',{'Name':'Department of Labor','ShortName':'DOL'}),('Subagency',{'Name':'Bureau of Labor Statistics','ShortName':'BLS'}),('Topic',M['Topic']),('Subtopic',M['Subtopic']),('Program',{'Name':M['ProgramName'],'ShortName':M['ProgramAbbr']}),('Dataset',{'Name':M['Dataset'],'ShortName':M['DatasetCode']})]
	D['DateFormat'] = 'YYYYhqmm'
	
	Filestuff = [{'path':docfile,'Name':M['DatasetCode'] + '.txt', 'description': 'Text documentation for ' + M['Dataset'], 'source' : D['Source']}]
	F = open(outdir + '__files.pickle','w')
	pickle.dump(Filestuff,F)
	F.close()
	
	L = [x for x in listdir(sourcedir) if 'data' in x.split('!')[-1].split('.')]	

	ColNumbers = [x.split('!')[-1].split('.')[x.split('!')[-1].split('.').index('data') + 1] for x in L]
	FLF = tb.tabarray(SVfile = filelistfile)
	Paths = FLF['Path'].tolist()
	SubCols = dict([('Col' + c,{'Title':FLF['FileName'][Paths.index(f)]}) for (c,f) in zip(ColNumbers,L)])
	
	M = tb.io.getmetadata(seriesfile)[0]
	getnames = M['coloring']['NewNames']
	names = M['names']
	spaceCodes = [inferSpaceCode(n) for n in names]
	
	headerlines = M['headerlines']
	getcols = [names.index(x) for (x,y) in zip(names,spaceCodes) if y == None and x in getnames]
	spacecols = [(names.index(x),y) for (x,y) in zip(names,spaceCodes) if y != None]
	fipscols = [(j,y) for (j,y) in spacecols  if y.startswith('f.')]
	nonfipscols = [(j,y) for (j,y) in spacecols  if not y.startswith('f.')]
	
	goodNames = [nameProcessor(x) for x in names]
	NAMES = ['Subcollections', 'Series'] + [goodNames[i] for i in getcols] + (['Location'] if spacecols else [])
	
	labelcols = [goodNames[i] for i in getcols] + (['Location'] if spacecols else [])
	
	TIMECOLS = []

	getinds = range(2,2+len(getcols) + (1 if spacecols else 0))
	
	BLOCKSIZE = 2500
	blocknum = 0
	recs = []
	Hashes = {}
	
	for (ColNo,l) in zip(ColNumbers,L):
		print 'Parsing', SubCols['Col' + ColNo]['Title']
		G = open(seriesfile,'rU')
		for i in range(headerlines):
			G.readline()
		sline = G.readline().strip('\n')
			
		F = open(sourcedir + l,'rU')
		dnames = F.readline().strip().split('\t')
		dline = F.readline().strip('\n')		
		
		ser = dline.split('\t')[0].strip()

		while dline:
			found = False
			while not found:
				if sline.split('\t')[0].strip() == ser:
					found = True
				else:
					sline = G.readline().strip('\n')
			slinesplit = sline.split('\t')
			servals = [('0',[ColNo]),('1',ser)] + zip([str(x)  for x in getinds], [slinesplit[j].strip() for j in getcols] + ([dict(([(y,slinesplit[j])  for (j,y) in nonfipscols] if nonfipscols else []) + ([('f',dict([(y.split('.')[1],slinesplit[j])  for (j,y) in fipscols]))]  if fipscols else []))] if spacecols else []))
			while dline:
				dlinesplit = [x.strip() for x in dline.split('\t')]
				if dlinesplit[0] == ser:
					if dlinesplit[3]:
						t = tval(dlinesplit[1],dlinesplit[2])
						if t in NAMES:
							tind = NAMES.index(t)
						else:
							tind = len(NAMES)
							NAMES.append(t)
							TIMECOLS.append(t)
						servals.append((str(tind),float(dlinesplit[3])))
						
					dline = F.readline().strip('\n')
				else:
					ser = dlinesplit[0]
					break
					
							
			servals = dict(servals)
			recs.append(servals)
			del(servals)		
			if len(recs) >= BLOCKSIZE:
				print ser
				print '. . . writing block', blocknum
				OUT = open(outdir + str(blocknum) + '.pickle','w')
				pickle.dump(recs,OUT)
				Hashes[blocknum] =  hashlib.sha1(str(recs)).hexdigest()
				OUT.close()
				blocknum += 1
				recs = []

	if recs:
		print ser
		print '. . . writing block', blocknum
		OUT = open(outdir + str(blocknum) + '.pickle','w')
		pickle.dump(recs,OUT)
		Hashes[blocknum] =  hashlib.sha1(str(recs)).hexdigest()
		OUT.close()
		blocknum += 1
		recs = []
	
	D['ColumnGroups'] = {'TimeColNames': TIMECOLS, 'LabelColumns': labelcols }
	if spacecols:
		D['ColumnGroups']['SpaceColumns'] = ['Location']
	D['UniqueIndexes'] = [['Series']]
	D['VARIABLES'] = NAMES
	D['sliceCols'] = [g for g in labelcols if g.lower().split('.')[0] not in ['footnote','seasonal','periodicity','location']] + (['Location.' + x for x in dict(spacecols).values() if not x.startswith('f.')] if spacecols else [])
		

	SubCols[''] = D
	
	OUT = open(outdir+'__metadata.pickle','w')
	pickle.dump({'Subcollections':SubCols,'Hashes':Hashes},OUT)
	OUT.close()	
				
			
def tval(year,per):
	if per.startswith('M'):
		num = int(per[1:])
		assert num <= 13
		if num < 13:
			return year + 'X' + 'X' + per[1:]
		else:
			return year + 'X' + 'X' + 'XX'
	elif per.startswith('Q'):
		num = int(per[1:])
		assert len(str(num)) == 1 and num <= 5
		if num < 5:
			return year + 'X' + str(num) + 'XX'
		else:
			return year + 'X' + 'X' + 'XX' 
	elif per.startswith('S'):
		num = int(per[1:])
		assert len(str(num)) == 1 and num <= 3
		if num < 3:
			return year + str(num) + 'X' + 'XX'
		else:
			return year + 'X' + 'X' + 'XX' 
	elif per.startswith('A'):
		return  year + 'X' + 'X' + 'XX' 
	else:
		raise ValueError, 'Time period format of ' + per + ' not recognized.'

def nameProcessor(g):

	g = g.split('_name')[0]
	g = g.split('_text')[0]
	g = g[0].upper() + g[1:]
	return g


#test=-=-=-=-=
@activate(lambda x : (x[0],x[1],x[2],x[3],x[4]),lambda x : x[5])
def MakeMongoSourceFlat(metafile, docfile, seriesfile, filelistfile, sourcedir, outdir):
	MakeDir(outdir)
	
	
	M = pickle.load(open(metafile))
	D = {}
	for x in ['ContactInfo','description','keywords']:
		D[x] = M[x]
	D['Source'] = [('Agency',{'Name':'Department of Labor','ShortName':'DOL'}),('Subagency',{'Name':'Bureau of Labor Statistics','ShortName':'BLS'}),('Topic',M['Topic']),('Subtopic',M['Subtopic']),('Program',{'Name':M['ProgramName'],'ShortName':M['ProgramAbbr']}),('Dataset',{'Name':M['Dataset'],'ShortName':M['DatasetCode']})]
	D['DateFormat'] = 'YYYYhqmm'
	
	Filestuff = [{'path':docfile,'Name':M['DatasetCode'] + '.txt', 'description': 'Text documentation for ' + M['Dataset'], 'source' : D['Source']}]
	F = open(outdir + '__files.pickle','w')
	pickle.dump(Filestuff,F)
	F.close()
	
	L = [x for x in listdir(sourcedir) if 'data' in x.split('!')[-1].split('.')]	

	ColNumbers = [x.split('!')[-1].split('.')[x.split('!')[-1].split('.').index('data') + 1] for x in L]
	FLF = tb.tabarray(SVfile = filelistfile)
	Paths = FLF['Path'].tolist()
	SubCols = dict([('Col' + c,{'Title':FLF['FileName'][Paths.index(f)],'__query__' : {'Subcollections':[c]}}) for (c,f) in zip(ColNumbers,L)])
	
	M = tb.io.getmetadata(seriesfile)[0]
	getnames = M['coloring']['NewNames']
	names = M['names']
	
	headerlines = M['headerlines']
	getcols = [names.index(x) for x in getnames]
	
	for (i,g) in enumerate(getnames):
		g = g.split('_name')[0]
		g = g.split('_text')[0]
		g = g[0].upper() + g[1:]
		getnames[i] = g
		
	NAMES = ['Subcollections', 'Series'] + getnames + ['Date','Value']

	getinds = range(2,2+len(getnames))
	
	BLOCKSIZE = 500000
	blocknum = 0
	recs = []
	Hashes = {}
	
	for (ColNo,l) in zip(ColNumbers,L):
		print 'Parsing', SubCols['Col' + ColNo]['Title']
		G = open(seriesfile,'rU')
		for i in range(headerlines):
			G.readline()
		sline = G.readline().strip('\n')
			
		F = open(sourcedir + l,'rU')
		dnames = F.readline().strip().split('\t')
		dline = F.readline().strip('\n')		
		
		ser = dline.split('\t')[0].strip()

		while dline:
			found = False
			while not found:
				if sline.split('\t')[0].strip() == ser:
					found = True
				else:
					sline = G.readline().strip('\n')
			slinesplit = sline.split('\t')
			servals = [ColNo,ser] + [slinesplit[j].strip() for j in getcols]
			while dline:
				dlinesplit = [x.strip() for x in dline.split('\t')]
				if dlinesplit[0] == ser:
					if dlinesplit[3]:
						t = tval(dlinesplit[1],dlinesplit[2])
						recs.append(tuple(servals + [t,dlinesplit[3]]))
						
						if len(recs) >= BLOCKSIZE:
							print ser
							print '. . . writing block', blocknum
							X = tb.tabarray(records = recs,names = [str(x) for x in range(len(NAMES))])
							X.saveSV(outdir + str(blocknum) + '.tsv',metadata=['dialect','formats','names'])
							Hashes[blocknum] =  hashlib.sha1(open(outdir + str(blocknum) + '.tsv').read()).digest()
							blocknum += 1
							recs = []						
											
					dline = F.readline().strip('\n')
				else:
					ser = dlinesplit[0]
					break
					
			del(servals)		


	if recs:
		print ser
		print '. . . writing block', blocknum
		X = tb.tabarray(records = recs,names = [str(x) for x in range(len(NAMES))])
		X.saveSV(outdir + str(blocknum) + '.tsv',metadata=['dialect','formats','names'])
		Hashes[blocknum] =  hashlib.sha1(open(outdir + str(blocknum) + '.tsv').read()).digest()
		blocknum += 1
		recs = []		

	D['ColumnGroups'] = {'TimeColumns':['Date'], 'LabelColumns': getnames }
	D['UniqueIndexes'] = [['Series','Date']]
	D['VARIABLES'] = NAMES
	
	SubCols[''] = D
	
	OUT = open(outdir+'__metadata.pickle','w')
	pickle.dump({'Subcollections':SubCols,'Hashes':Hashes},OUT)
	OUT.close()	

def inferSpaceCode(name):
	parts = uniqify(name.lower().split('_') + name.lower().split(' '))
	if 'msa' in parts and not 'code' in parts:
		return 'm'
	elif 'state' in parts and not 'code' in parts:
		return 's'
	elif 'county' in parts and not 'code' in parts:
		return 'c'
	elif 'fips' in parts and 'text' in parts:
		return 'X'
	elif 'area' in parts and 'code' not in parts:
		return 'X'
	elif 'fips' in parts and 'state' not in parts and 'county' not in parts:
		return 'f.X'
	elif 'state' in parts and ('code' in parts or 'fips' in parts):
		return 'f.s'
	elif 'county' in parts and ('code' in parts or 'fips' in parts):
		return 'f.c'
	
