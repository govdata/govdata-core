'''
Contains functions I use for configuring the  Data Environment.
'''
import System.LinkManagement as LM
import tabular as tb
from System.Utils import RecursiveFileList, CheckInOutFormulae, getpathalong, uniqify, ListUnion, IsPythonFile, IsDotPath, IsDir,listdir,IsFile,is_string_like,PathExists
from System.Storage import FindMtime,StoredDocstring
from System.MetaData import CombineSources,ProcessResources,ChooseImage,DEFAULT_GenerateAutomaticMetaData,metadatapath,opmetadatapath,ProcessMetaData
import cPickle as pickle
import Words.WordFrequency as WF
from traceback import print_exc
import os

DE_NAME = 'govlove'

def GetLiveModules(LiveModuleFilters):
	'''
	Function for filtering live modules that is fast by avoiding looking through directories i know will be irrelevant.
	'''
	#for each thing in live modulefilter
	FilteredModuleFiles = []
	for x in LiveModuleFilters.keys():
		RawModuleFiles = [y for y in RecursiveFileList(x,Avoid=['^RawData$','^Data$','^.svn$','^ZipCodeMaps$','.data$','^scrap$']) if y.split('.')[-1] == 'py']
		FilteredModuleFiles += [y for y in RawModuleFiles if CheckInOutFormulae(LiveModuleFilters[x],y)]
	return FilteredModuleFiles

def GenerateAutomaticMetaData(objname,forced=False,use=100):	

	metapath = opmetadatapath(objname) if IsDotPath(objname) else metadatapath(objname)
	if IsDotPath(objname):
		path = '../' + '/'.join(objname.split('.')[:-1]) + '.py'
		objectname = objname.split('.')[-1]
	else:
		path = objname
		objectname =''
		
	if forced or not PathExists(metapath) or os.path.getmtime(metapath) <= FindMtime(path,objectname=objectname,Simple=False):
		if IsDir(objname):
			if objname[-1] != '/': objname += '/'
			if is_hsv_dir(objname):
				pass
			else:
				D = {}
				L = [objname + ll for ll in listdir(objname) if not ll.startswith('.')]
				for l in L:
					D.update(GenerateAutomaticMetaData(l,forced=forced))
				LL = set(L).intersection(D.keys())
				D[objname] = IntegrateDirMetaData([D[l] for l in LL])
				return D
	
		else:
			if IsPythonFile(objname) or IsDotPath(objname):
				d = StoredDocstring(objname)
				if d:
					return {objname:{'description':d,'signature': 'python'}}		
		
			elif objname.endswith(('.csv','.tsv')):
				if IsFile(objname):
					try:
						x = tabularmetadata(objname,use=use)
					except:
						x = DEFAULT_GenerateAutomaticMetaData(objname)
						print 'Failed to tabular metadata for', objname
						print_exc()
					else:
						x['signature'] = 'tabular'
					return {objname : x}
	
			elif objname.endswith(('.html','.htm')):
				
				if IsFile(objname):
					try:
						w = WF.HiAssocWords(objname)
					except:
						print 'Failed to compute word associations on', objname
						print_exc()
						x = DEFAULT_GenerateAutomaticMetaData(objname)
						return {objname : x}
					else:
						common = list(w[:100]['Word'])
						common.sort()
						if w is not None:
							return {objname:{'wordassoc':w.tolist(),'frequentwords':common, 'signature':'html'}}
						
		return {}
	else:
		try:
			return {objname:pickle.load(open(metapath+'/AutomaticMetaData.pickle','r'))}
		except:
			return GenerateAutomaticMetaData(objname,forced=True)

def is_hsv_dir(x):
	return hasattr(x,'endswith') and x.endswith('.hsv')


def IntegrateDirMetaData(D):
	DD = {'signature' : 'directory'}
	DD['npaths'] = len(D) ; DD['nfiles'] = sum([d['nfiles'] if 'nfiles' in d.keys() else 1 for d in D])
	if len(D) > 0:
		C = D[0].items()
		for d in D[1:]:
			C = [(k,v) for (k,v) in C if (k,v) in d.items()]
			if len(C) == 0:
				break
		if len(C) > 0:
			for (k,v) in C:
				DD['DIR_' + k] = v
				
		if all(['frequentwords' in d.keys() for d in D]):
			C = set(D[0]['frequentwords'])
			for d in D[1:]:
				C = C.intersection(set(d['frequentwords']))
				if len(C) == 0:
					break
			if len(C) > 0:
				common = list(C)
				common.sort()
				DD['frequentwords'] = common
			
	return DD	

def tabularmetadata(path,use=10000):


	MetaData = {}
	if os.path.getsize(path) > 500000000:
		MD = tb.io.getmetadata(path)
		headerlines = MD[0]['headerlines']
		F = open(path,'rU')
		r = 0
		for l in F:
			r += 1
		MetaData['nrows'] = r - headerlines
		print 'Making approximation to number of rows in ', path 
		
	else:
		data1 = tb.tabarray(SVfile=path,verbosity = 0, usecols = [0])
		MetaData['nrows'] = len(data1)


	data1 = tb.tabarray(SVfile=path,verbosity = 0, uselines = (0,use))
	MetaData['colnames'] = data1.dtype.names
	MetaData['colformats'] = tb.io.parseformats(data1.dtype)
	MetaData['coltypes'] = tb.io.parsetypes(data1.dtype)
	MetaData['ncols'] = len(data1.dtype.names)	
	MetaData['coloring'] = data1.coloring
	MetaData['delimiter'] = repr(data1.metadata['dialect'].delimiter)
	if 'coldescrs' in data1.metadata.keys():
		MetaData['coldescrs'] =  eval(data1.metadata['coldescrs'])
		
	
	othermetadata = data1.metadata
	
	badlist = ['dialect','coloring','formats','types','names','metametadata','headerlines','delimiter','coldescrs']
	for b in badlist:
		if b in othermetadata.keys():
			othermetadata.pop(b)
	
	othermetadata.update(MetaData)

	del data1

	return othermetadata


def MetaDataProcessor(metapath,objname = None, extensions = None):

	if objname is None:
		objname = metapath
			
	X = ConsolidateSources(metapath)
	if 'image' in X.keys():
		if isinstance(X['image'],str):
			X['image'] = X['image'].split(',')
		imgres = [(x,'image' + ('.' + x.split('.')[-1]) if '.' in x else '') for x in X['image']]
		if 'Resources' in X.keys():
			X['Resources'] += imgres
		else:
			X['Resources'] = imgres
		
	ProcessResources(metapath,X,objname)
	image = ChooseImage(metapath)
	if image:
		X['image'] = image
	elif 'image' in X.keys():
		X.pop('image')
	F = open(metapath + '/ProcessedMetaData.pickle','w')
	pickle.dump(X,F)
	F.close()

	text = SummarizeMetaData(X)
	F = open(metapath + '/MetaDataSummary.html','w')
	try:
		F.write(text)
	except UnicodeEncodeError:
		F.write(text.encode('utf-8'))
	
	F.close()


def ConsolidateSources(metapath,objname=None,extensions=None):
	
	consolidated = {}
	if extensions is None:
		extensions = ['Attached','Associated','Automatic','Inherited']
	combined = CombineSources(metapath,extensions=extensions)

	if 'Resources' in combined.keys():
		consolidated['Resources'] = uniqify(ListUnion(combined['Resources'].values()))
			
	if 'image' in combined.keys():
		consolidated['image'] = ListUnion([x.split(',') if is_string_like(x) else x for x in combined['image'].values()])
	
	if 'author' in combined.keys():
		consolidated['author'] = '; '.join(combined['author'].values())
	
	if 'title' in combined.keys():
		consolidated['title'] = '; '.join(combined['title'].values())
	
	if 'description' in combined.keys():
		descrs = combined['description'].items()
		if len(descrs) == 1:
			consolidated['description'] = descrs[0][1]
		else:
			consolidated['description'] = '\n\n'.join([e + ': ' + d for (e,d) in descrs])
			
	elif 'Verbose' in combined.keys():
		descrs = combined['Verbose'].items()
		if len(descrs) == 1:
			consolidated['description'] = descrs[0][1]
		else:
			consolidated['description'] = '\n\n'.join([e + ': ' + d for (e,d) in descrs])
	
	if 'keywords' in combined.keys():
		for k in combined['keywords'].keys():
			if not is_string_like(combined['keywords'][k]):
				combined['keywords'][k] = ','.join(combined['keywords'][k])
				
		consolidated['keywords'] = ','.join([x.strip() for x in uniqify((','.join(combined['keywords'].values())).split(','))])
				
				
	if 'signature' in combined.keys():
		s = uniqify(combined['signature'].values())
		if len(s) == 1:
			consolidated['signature'] = s[0]
		else:
			consolidated['signature'] = ''
	
	L = ['nrows','ncols','coloring','wordassoc','colformats','coltypes','colnames','wordassoc','frequentwords','nfiles','npaths']
	LL = L + [x for x in combined.keys() if x.startswith('DIR_')]
	for x in LL:
		if x in combined.keys() and 'Automatic' in combined[x].keys():
			consolidated[x] = combined[x]['Automatic']
		elif x in combined.keys() and 'Attached' in combined[x].keys():
			consolidated[x] = combined[x]['Automatic']
		elif x in combined.keys() and 'Associated' in combined[x].keys():
			consolidated[x] = combined[x]['Associated']		
		elif x in combined.keys() and 'Inherited' in combined[x].keys():
			consolidated[x] = combined[x]['Inherited']		


	if 'coldescrs' in combined.keys():
		coldescrs = {}
		for c in combined['coldescrs'].values():
			if isinstance(c,dict):
				for k in c.keys():
					if k in coldescrs.keys():
						coldescrs[k] += (c[k],)
					else:
						coldescrs[k] = (c[k],)
	
		for k in coldescrs.keys():
			coldescrs[k] = '\n'.join(coldescrs[k])
		
		consolidated['coldescrs'] = coldescrs
				
	OtherKeys = set(combined.keys()).difference(consolidated.keys())

	for k in OtherKeys:
		consolidated[k] = ' '.join([x if is_string_like(x) else repr(x) for x in combined[k].values()])

	return consolidated
	

	
def SummarizeMetaData(X):

	if 'image' in X.keys():
		from PIL import Image	
		K = 125
		try:
			x = Image.open('..' + X['image'])
		except:
			print 'Importing image', X['image'], 'failed.  here is the error:'
			print_exc()
			sizetag = ''
		else:
			(w,h) = x.size
			if w > K or h > K:
				r = float(max(w,h))
				w = int(w * K/r)
				h = int(h * K/r)
			sizetag = 'width="' + str(w) + '" height="' + str(h) + '"'

		image = '<img src="' + X['image'] + '" ' + sizetag + '/><br/>'
	else:
		image = ''

	if 'description' in X.keys():
		description = '<strong>Description: </strong>' + X['description'].replace('\n','<br/>')
	else:
		description = ''
	
	if 'author' in X.keys():
		author = '<strong>Author: </strong>' + X['author']
	else:
		author = ''
	
	if 'title' in X.keys():
		title = '<strong>Title: </strong>' + X['title']
	else:
		title = ''
	
	if 'keywords' in X.keys():
		keywords = '<strong>Keywords: </strong>' + X['keywords']
	else:
		keywords = ''
	
	if 'signature' in X.keys():
		if X['signature'] != 'directory':
			signature = '<strong>Signature: </strong> This appears to be a ' + X['signature'] + ' file.'  
		elif 'DIR_signature' in X.keys():
			signature = '<strong>Signature: </strong> This is a directory consisting of ' + X['DIR_signature'] + ' files.'  
		else:
			signature = ''
	else:
		signature = ''
		X['signature'] = ''

	
	nr = [x for x in X.keys() if x.endswith('nrows')]
	nc = [x for x in X.keys() if x.endswith('ncols')]
	preamble = 'It has' if X['signature'] == 'tabular' else 'Its constituent datasets commonly have' if X['signature'] == 'directory' and 'DIR_signature' in X.keys() and X['DIR_signature'] == 'tabular' else 'This data has'
	if len(nr) > 0 and len(nc) > 0:
		ending = str(X[nr[0]]) + ' rows and ' + str(X[nc[0]]) + ' columns.'
	elif len(nr) > 0:
		ending = str(X[nr[0]]) + ' rows.'
	elif len(nc) > 0:
		ending = str(X[nc[0]]) + ' columns.'
	else:
		ending = ''
	if ending != '':
		tabulartext = preamble + ' ' + ending
	else:
		tabulartext = ''
		
	
	nn = [x for x in X.keys() if x.endswith('colnames')]
	if len(nn) > 0:
		names = X[nn[0]]
		nt = [x for x in X.keys() if x.endswith('coltypes') and len(X[x]) == len(X[nn[0]])]
		if len(nt):
			types = [' (' + t + ')' for t in X[nt[0]]]
		else:
			types = ['']*len(names)
			
		nd = [x for x in X.keys() if x.endswith('coldescrs') and isinstance(X[x],dict) and set(X[x].keys()).intersection(names)]

		if len(nd) > 0:			
			descrs = X[nd[0]]
			descrs = [': ' + descrs[n] if n in descrs.keys() else '' for n in names]
		else:
			descrs = ['']*len(names)
		
		coltext = 'The columns are:<br/>' + '<br/>'.join(['<strong>'+n+'</strong>' + t + d for (n,t,d) in zip(names,types,descrs)])
	else:
		coltext = ''
		nt = []
		nd = []

	#frequentwords
	if 'frequentwords' in X.keys():
		frequentwords = 'Frequent words in this data include: ' + repr(X['frequentwords']) + '.'
	else:
		frequentwords = ''
		
	text = '<br/><br/>'.join([x for x in [image,title,author,description,signature,tabulartext,coltext,frequentwords,keywords] if x != ''])
	
	OtherKeys = set(X.keys()).difference(['image','coloring','description','author','title','keywords','signature','frequentwords','colformats','nfiles','npaths'] + nr + nc + nn + nt + nd)
	if OtherKeys:
		text +=  '<br/><br/><strong>Other MetaData</strong>:' + '<br/><br/>'.join(['<strong>' + k + ': </strong>' + (X[k] if is_string_like(X[k]) else repr(X[k]))  for k in OtherKeys])
	
	return text


def inheritmetadata(From,To,creates='../System/MetaData/'):
	FromPath = opmetadatapath(From) if IsDotPath(From) else metadatapath(From)
	ToPath = opmetadatapath(To) if IsDotPath(To) else metadatapath(To)
	X = ConsolidateSources(FromPath,extensions=['Associated','Attached','Inherited'])
	F = open(ToPath + '/InheritedMetaData.pickle','wb')
	pickle.dump(X,F)
	F.close()
	ProcessMetaData(ToPath,objname=To)
	
