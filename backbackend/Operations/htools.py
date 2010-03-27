from System.Protocols import ApplyOperations2
from System.Utils import IsDir,IsFile,ListUnion,MakeDir,wget,activate,is_string_like
import tabular as tb
import numpy as np
	
@activate(lambda x : (x[0],x[1],x[2]),lambda x : (x[3],x[4]) if x[4] else x[3])
def applyparser(oldmanifestpath,oldtotallinkpath,datadir,newmanifestpath,newtotallinkpath,F,splitfunc,prefixlist,round):
	M = tb.tabarray(SVfile = oldmanifestpath)
	
	is_prefix = 'Prefix' in M.dtype.names
	if 'Extension' in M.dtype.names:
		Extensions = M['Extension']
	else:
		Extensions = ['html']*len(M)
	if 'Categories' in M.coloring.keys():		
		Results = [F(datadir + (M['Prefix'][i] + '/' if is_prefix else '')  + pathprocessor(M['Categories'][i]) + '.' +  Extensions[i],M[i]) for i in range(len(M))]
		RResults = [r for r in Results if r is not None]
		lens = [len(r) if r is not None else 0 for r in Results]
		NM = M['Categories'].repeat(lens).colstack(tb.tab_rowstack(RResults),mode='rename')
	else:
		Results = [F(datadir + (M['Prefix'][i] + '/' if is_prefix else '')  + pathprocessor(()) + '.' + Extensions[i],M[i]) for i in range(len(M))]
		NM = tb.tab_rowstack(Results)
		
	if not 'Download' in NM.coloring.keys() and 'URL' in NM.dtype.names:
		NM.coloring['Download'] = ['URL']
				
	if newtotallinkpath:
		assert 'Download' in NM.coloring.keys()
		T = tb.tabarray(SVfile = oldtotallinkpath)
		DD = np.array([str([(o,x[o]) for o in x.dtype.names]) for x in NM['Download']])
		NewLinks = np.invert(tb.fast.isin(DD,T['Download']))
		NM = NM[NewLinks]
		NT = T.rowstack(tb.tabarray(records = zip([round]*len(DD),DD),names = ['Round','Download']))
		NT.saveSV(newtotallinkpath,metadata=True)
	
	if (splitfunc != None) and (prefixlist != None):
		Prefixes = [splitfunc(x) for x in NM]
		badprefixes = [y for y in Prefixes if '/' in y or y not in prefixlist]
		assert len(badprefixes) == 0, 'Given the splitter prefix list ' + str(prefixlist) + ', the following bad prefixes occured:' + str(badprefixes)	
		NM = NM.addcols(Prefixes,names=['Prefix'])
		
	NM.saveSV(newmanifestpath,metadata=True)


@activate(lambda x : x[0],lambda x : x[1])
def applysplitter(manifest,splitdir):
	MakeDir(splitdir)
	M = tb.tabarray(SVfile = manifest)
	vals = tb.uniqify(M['Prefix'])
	for v in vals:
		Mv = M[M['Prefix'] == v]
		Mv.saveSV(splitdir + 'Manifest_' + pathprocessor([v]) + '.tsv', metadata=True)


def modwget(dd,path):
	if 'opstring' in dd.dtype.names:
		wget(dd['URL'],path,opstring=dd['opstring'])
	else:
		wget(dd['URL'],path)				

			
@activate(lambda x : x[0],lambda x : x[1])	
def applygetter(manifest,downloaddir,getfunc):
	MakeDir(downloaddir)
	X = tb.tabarray(SVfile = manifest)
	if 'Extension' in X.dtype.names:
		Extensions = X['Extension']
	else:
		Extensions = ['html']*len(X)
	if 'Categories' in X.coloring.keys():
		for i in range(len(X)):
			path = downloaddir + pathprocessor(X['Categories'][i]) + '.' + Extensions[i]
			dd = X['Download'][i]
			getfunc(dd,path)
	else:
		assert len(X) == 1
		path = downloaddir + pathprocessor(()) + '.'  + Extensions[0]
		dd = X['Download'][0]
		getfunc(dd,path)		
		

def pathprocessor(x):
	if len(x) > 0:
		cleaner = lambda x : x.replace(' ','').replace('\'','').replace('/','_').replace('&','_').replace('$','').replace('!','').replace('"','')
		return '!'.join([cleaner(y) for y in x])
	else:
		return 'root'
		

@activate(lambda x : dict(x[0])['URL'],lambda x : x[1])
def hstart(seed,datadir,getfunc):
	MakeDir(datadir)
	
	manifestpath = datadir + 'Manifest_0.tsv'
	totallinkpath = datadir + 'TotalLinks_0.tsv'
	downloaddir = datadir + 'Downloads_0/'
	
	Recs = [tuple(zip(*seed)[1])]
	names = list(zip(*seed)[0])
	X = tb.tabarray(records = Recs,names=names)
	X.coloring['Download'] = names
	X.saveSV(manifestpath,metadata=True)
	Recs = [(0,str(seed))]
	X = tb.tabarray(records = Recs,names=['Round','Download'])
	X.saveSV(totallinkpath,metadata = True)

	applygetter(manifestpath,downloaddir,getfunc)

	
	
def hsuck(seed,datadir,L,suffix='',write=True,ipath=None,getfunc0=None):
	
	if is_string_like(seed):
		seed = [('URL',seed)]
		
	if getfunc0 is None:
		getfunc0 = modwget
		
	if suffix and not suffix.endswith('_'):
		suffix = suffix + '_'
	
	if not datadir.endswith('/'):
		datadir += '/'
	
	D = [(suffix + 'initialize',hstart,(seed,datadir,getfunc0))]

	for (i,l) in enumerate(L[:-1]):
		round = i+1
		
		oldmanifestpath = datadir + 'Manifest_' + str(round-1) + '.tsv'
		newmanifestpath = datadir + 'Manifest_' + str(round) + '.tsv'
		oldtotallinkpath = datadir + 'TotalLinks_' + str(round-1) + '.tsv'
		newtotallinkpath = datadir + 'TotalLinks_' + str(round) + '.tsv'
		olddownloaddir = datadir + 'Downloads_' + str(round-1) + '/'
		newdownloaddir = datadir + 'Downloads_' + str(round) + '/'
		Suffix = suffix + 'Round' + str(round) + '_'
		
		if hasattr(l,'__call__'):
			Parser = l
			Getter = modwget
			splitfunc = None
			prefixlist = None
		else:
			assert isinstance(l,dict) and 'Parser' in l.keys()
			Parser = l['Parser']
			if 'Splitter' in l.keys():
				(splitfunc, prefixlist) = l['Splitter']		
			else:
				(splitfunc, prefixlist) = (None, None)
			if 'Getter' in l.keys():
				Getter = l['Getter']
			else:
				Getter = modwget
			
		D += [(Suffix + 'parse',applyparser,(oldmanifestpath,oldtotallinkpath,olddownloaddir,newmanifestpath,newtotallinkpath,Parser,splitfunc,prefixlist,round))]
			
		if (splitfunc != None) and (prefixlist != None):
			assert all(['/' not in p for p in prefixlist])		
			splitdir  = datadir + 'SplitManifest_' + str(round) + '/'
			D += [(Suffix + 'splitmanifest',applysplitter,(newmanifestpath,splitdir))]
			D += [(Suffix + 'initializedownloads',MakeDir,(newdownloaddir,))]
			D += [(Suffix + 'download_' + pathprocessor([p]).replace('!','_').replace('-','_'),applygetter,(splitdir + 'Manifest_' + pathprocessor([p]) + '.tsv',newdownloaddir + pathprocessor([p]) + '/',Getter)) for p in prefixlist]			
		else:
			D += [(Suffix + 'download',applygetter,(newmanifestpath,newdownloaddir,Getter))]

	if L[-1]:
		oldmanifestpath = datadir + 'Manifest_' + str(round) + '.tsv'
		newmanifestpath = datadir + 'Catalog.tsv'
		oldtotallinkpath = datadir + 'TotalLinks_' + str(round) + '.tsv'
		olddownloaddir = datadir + 'Downloads_' + str(round) + '/'
		Suffix = suffix + 'Final_'
		assert hasattr(L[-1],'__call__')
		Parser = L[-1]
		
		D += [(Suffix + 'parse',applyparser,(oldmanifestpath,oldtotallinkpath,olddownloaddir,newmanifestpath,None,Parser,None,None,'final'))]
		

	if write:
		assert ipath, 'ipath must be specified'
		ApplyOperations2(ipath,D)
	
	return D
	
	
def MakeTable(S,C,P,N):
	SL = [GetSubList(S,C[:i+1] + [C[-1]]) for i in range(len(C)-1)]
	Final = S.findAll(C[-1])
	Indices = [[(x[0],Find(Final,x[1][0]),Find(Final,x[1][-1])) if len(x[1]) > 0 else (x[0],0,-1) for x in sl] for sl in SL]
	Cols = []
	for (p,ind) in zip(P[:-1],Indices):
		labels = [p(i[0]) for i in ind]
		maxlen = max([len(l) for l in labels])
		Col = np.zeros((len(Final),),'|S' + str(maxlen))
		for (l,i) in zip(labels,ind):
			Col[i[1]:i[2]+1] = l
		Cols.append(Col)
	
	X = tb.tabarray(columns = Cols,names = N[:-1])

	FinalP =[P[-1](x) for x in Final]
	if len(N[-1]) == 1:
		return X.addcols(FinalP,names = N[-1])
	else:	
		return X.colstack(tb.tabarray(records = FinalP,names=N[-1]))
	
def Find(L,item):
	s = [i for i in range(len(L)) if L[i] is item]
	assert len(s) >0, 'item not found'
	return s[0]
	
def GetSubList(S,C):

	L1 = S.findAll(C[-2])
	L2 = S.findAll(C[-1])
	
	SubL = [l1.findNext(C[-1]) for l1 in L1]
	Sub = [[l1.findNext(c).findNext(C[-1]) if l1.findNext(c) else None for l1 in L1] for c in C[:-2]]
	Sub = [[None if el in M[i+1:] else el for (i,el) in enumerate(M)] for M in Sub]
	
	G = []
	
	#i = L2.index(SubL[0])
	for (j,l1) in enumerate(L1):
		try:
			i = Find(L2,SubL[j])
		except:
			pass
		else:
			G.append([])
			NL =  [M[j] for M in Sub] + SubL[j+1:]  
			for l2 in L2[i:]:
				if not any([l2 is nl for nl in NL]):
					G[-1].append(l2)
					i += 1
				else:
					break

	return zip(L1,G)	