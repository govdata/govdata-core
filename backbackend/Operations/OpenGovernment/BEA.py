import os
import numpy as np
import tabular as tb
from BeautifulSoup import BeautifulSoup, BeautifulStoneSoup, NavigableString
from mechanize import Browser
import urllib
import re

from System.Utils import MakeDir, Contents, listdir, IsDir, wget, uniqify, PathExists
import Operations.htools as htools

import Operations.OpenGovernment.OpenGovernment as OG

root = '../Data/OpenGovernment/BEA/'
protocolroot = '../Protocol_Instances/OpenGovernment/BEA/'



#############################################################################
# utilities
#############################################################################

def SafeContents(x):
	return ' '.join(Contents(x).strip().split())
					
def hr(x):
	return  len(x) - len(x.lstrip(' '))
	
def hr2(x):
	return  len(x.split('\xc2\xa0')) - 1

def initialize(creates = (root, protocolroot)):
	MakeDir(root)
	MakeDir(protocolroot)

def WgetMultiple(dd, fname, maxtries=5):
	link = dd['URL']
	if 'opstring' in dd.dtype.names:
		opstring = dd['opstring']
	else:
		opstring = ''
	for i in range(maxtries):
		wget(link, fname, opstring)
		F = open(fname,'r').read().strip()
		if not (F.startswith('<!DOCTYPE HTML') or F == '' or 'servlet error' in F.lower()):
			return
		else:
			print 'download of ' + link + ' failed'
	print 'download of ' + link + ' failed after ' + str(maxtries) + ' attempts'
	return

def GetFootnotes(line, FootnoteSplitter='/'):
	newline = ' '*(len(line)-len(line.lstrip())) + ' '.join([' '.join(x.split()[:-1]) for x in line.split(FootnoteSplitter)[:-1]])
	footnotes = ', '.join([x.split()[-1] for x in line.split(FootnoteSplitter)[:-1]])
	return (newline, footnotes)

def GetFootnotes2(line, FootnoteSplitter='\\'):
	newline = ' '.join(line.split(FootnoteSplitter)[:-1])
	footnotes = ', '.join([x.split()[0] for x in line.split(FootnoteSplitter)[1:]])
	return (newline, footnotes)

def GetFootnotesLazy(line, FootnoteSplitter='\\'):
	newline = line.split(FootnoteSplitter)[0]
	footnotes = line.split(FootnoteSplitter)[1]
	return (newline, footnotes)
	
def CleanLinesForMetadata(x):
	x = [line.strip('"').strip() for line in x]
	line = x[0]
	while line == '':
		x = x[1:]
		line = x[0]
	line = x[-1]
	while line == '':
		x = x[:-1]
		line = x[-1]
	return x
		
	
def ExpandString(S):
	ind2 = [i for i in range(1,len(S)-1) if S[i].lower() != S[i] and S[i+1].lower() == S[i+1]] + [len(S)]
	ind1 = [0] + ind2[:-1]
	return ' '.join([S[i:j] for (i,j) in zip(ind1,ind2)])



			
#############################################################################
# Main parsers
#############################################################################


def NEA_Parser(page, headerlines=None, FootnoteSplitter = '/', FootnotesFunction = GetFootnotes, CategoryColumn=None,FormulaColumn=None):
	
	[Y, header, footer, keywords]	 = BEA_Parser(page, headerlines=headerlines, FootnoteSplitter = FootnoteSplitter, FootnotesFunction = FootnotesFunction, CategoryColumn=CategoryColumn, NEA=True, FormulaColumn=FormulaColumn)
	levelnames = [n for n in Y.dtype.names if n.startswith('Level_')]
	displaylevels = np.array([np.where(Y[n] != '', i,0) for (i,n) in enumerate(levelnames)]).T.max(axis=1)
	Y = Y.addcols(displaylevels,names=['DisplayLevel'])		
	
	labels = [x.strip() for x in Y['Category']]
	for k in range(0,Y['DisplayLevel'].max()):
		badcounts = [i for i in range(len(labels)) if labels.count(labels[i]) > 1]
		for i in badcounts:
			if Y['DisplayLevel'][i] - k >= 1:
				labels[i] = (labels[i] + ' - ' + Y['Level_' + str(Y['DisplayLevel'][i] - k)][i]).strip()
	Y = Y.addcols(labels,names=['Label'])
	
	Y.metadata = {'labelcollist':['Label']}
					
	return [Y, header, footer, keywords]				
	
	
					
def BEA_Parser(page, headerlines=None, FootnoteSplitter = '/', FootnotesFunction = GetFootnotes, CategoryColumn=None, NEA=False, FormulaColumn=None):

	if NEA:
		G = [line.strip() for line in open(page, 'rU').read().strip('\n').split('\n')]	
		# get header
		keepon = 1
		i = 0
		header = []
		while keepon:
			line = G[i]
			if not line.startswith('Line'):
				header += [line]
				i = i + 1
			else:
				keepon = 0				
		[F, meta] = tb.io.loadSVrecs(page, headerlines=i+1)
	else:
		[F, meta] = tb.io.loadSVrecs(page, headerlines=headerlines)
		header = None
	
	names = [n.strip() for n in meta['names']]
	if names[1] == '':
		names[1] = 'Category'
	
	# get footer
	keepon = 1
	i = len(F)-1
	footer = []	
	while keepon:
		rec = F[i]
		if len(rec) != len(names):
			footer = [','.join(rec)] + footer
			i = i - 1
		else:
			keepon = 0

	F = F[:i+1]
	F = [line + ['']*(len(names)-len(line)) for line in F ]
	
	"""
	title = names[0]
	if len(title.split(FootnoteSplitter)) > 1:
		LineFootnote = title.split(FootnoteSplitter)[0].split()[-1]
		title = ' '.join(title.split(FootnoteSplitter)[0].split()[:-1]).strip()
	else:
		LineFootnote = None
	"""

	if CategoryColumn:
		i = [i for i in range(len(names)) if names[i].strip() == CategoryColumn][0]
		CatCol = np.array([row[i] for row in F])
		
	F = [tuple([col.strip() for col in row]) for row in F]

	ind = [i for i in range(len(names)) if names[i][:4].isdigit()][0]
	X = tb.tabarray(records=F, names=names, coloring={'Info': names[:ind], 'Data': names[ind:]})	
	
	FootnoteColumns = []
	FootnoteNames = []
	for cname in X.coloring['Info']:			
		L = [len(c.split(FootnoteSplitter)) > 1 for c in X[cname]]
		if any(L):
			#Column_Footnote = [FootnotesFunction(X[cname][i]) if L[i] else (col[i], '') for i in range(len(X))]
			Column_Footnote = [FootnotesFunction(c) if l else (c, '') for (c, l) in zip(X[cname], L)]
			X[cname] = [c for (c,n) in Column_Footnote]
			FootnoteColumns += [[n for (c,n) in Column_Footnote]]
			FootnoteNames += [cname + ' Footnotes']
			if cname == CategoryColumn:
				Column_Footnote = np.array([FootnotesFunction(c) if l else (c, '') for (c, l) in zip(CatCol, L)])
				CatCol = np.array([c for (c,n) in Column_Footnote])
				
	if FootnoteColumns:
		Footnotes = tb.tabarray(columns = FootnoteColumns, names = FootnoteNames, coloring = {'Footnotes': FootnoteNames})
	else:
		Footnotes = None
		
	if FormulaColumn:
		L = [len(c.split('(')) > 1 and any([x.isdigit() and x<1000 for x in c.split('(')[1]]) for c in X[FormulaColumn]]
		if any(L):
			Formula = [X[FormulaColumn][i].split('(')[1].split(')')[0] if L[i] else '' for i in range(len(X))]
			X[FormulaColumn] = [X[FormulaColumn][i].split('(')[0].rstrip() if L[i] else X[FormulaColumn][i] for i in range(len(X))]
			X = X['Info'].colstack(tb.tabarray(columns = [Formula], names = ['Formula'])).colstack(X['Data'])
			X.coloring['Info'] += ['Formula']

	if CategoryColumn:
		[cols, hl] = OG.gethierarchy(CatCol, hr, postprocessor = lambda x : x.strip())		
		columns = [c for c in cols if not (c == '').all()]
		if len(columns) > 1:
			categorynames = ['Level_' + str(i) for i in range(1, len(columns)+1)]		
			Categories = tb.tabarray(columns = columns, names = categorynames, coloring = {'Categories': categorynames})
		else:
			Categories = None
	else:
		Categories = None
	
	Y = X['Info']
	if Footnotes != None:
		Y = Y.colstack(Footnotes)
	if Categories != None:
		Y = Y.colstack(Categories)		
	if NEA:
		X.replace('---','')
		Y = Y.colstack(tb.tabarray(columns=[tb.utils.DEFAULT_TYPEINFERER(X[c]) for c in X.coloring['Data']], names=X.coloring['Data'], coloring={'Data': X.coloring['Data']}))
	else:
		Y = Y.colstack(X['Data'])
	
	if CategoryColumn:
		keywords = [y for y in tb.utils.uniqify([x.replace(',', '').replace('.', '').replace(',', '') for x in X[CategoryColumn]]) if y]
	else:
		keywords = []

	if footer:
		footer = CleanLinesForMetadata(footer)
	if header:
		header = CleanLinesForMetadata(header)
		
	return [Y, header, footer, keywords]	



	
#############################################################################
# Main NIPA Tables
#############################################################################
	

def NEA_NIPA_Parse1(page, x):
	S = [s.strip() for s in open(page, 'r').read().split('Section')[1:]]
	Section_SplitSoup = [(s.split('-')[1].split('<')[0].strip(), BeautifulSoup(s,convertEntities=BeautifulStoneSoup.HTML_ENTITIES)) for s in S]
	D = {'(A)': 'Year', '(Q)': 'Qtr', '(M)': 'Month'}
	Recs = []
	for (Section, Soup) in Section_SplitSoup:
		alist = [tr.findAll('a')[0] for tr in Soup.findAll('tr') if tr.findAll('a')]
		Table_URL = [(SafeContents(a), 'http://www.bea.gov/national/nipaweb/' + str(dict(a.attrs)['href'])) for a in alist if 'href' in dict(a.attrs).keys()]
		Number_Name_URL = [(t.split()[1], ' '.join(t.split()[2:]), url) for (t, url) in Table_URL]
		XYZ = [tuple(n[0].strip('.').split('.')) if len(n[0].strip('.').split('.'))==3 else tuple(n[0].strip('.').split('.')) + ('',) for n in Number_Name_URL]
		for i in range(len(XYZ)):
			(Section_Number, Subsection_Number, Table_Number) = XYZ[i]
			(Number, Name, u) = Number_Name_URL[i]
			Dlist = [d for d in D.keys() if d in Name]
			for d in Dlist:
				Freq = D[d]
				URL = u.split('Freq=')[0] + 'Freq=' + Freq + '&' + '&'.join(u.split('Freq=')[1].split('&')[1:])
				Recs += [(Section, Section_Number, Subsection_Number, Table_Number, Number, Freq, Name, URL)]
	return tb.tabarray(records = Recs, names = ['Section', 'Section_Number', 'Subsection_Number', 'Table_Number', 'Number', 'Freq', 'Name', 'URL'], coloring = {'Categories': ['Number', 'Freq', 'Name']})
	
def NEA_NIPA_Parse2(page, x):
	print page
	URL = open(page, 'r').read().split('">Download All Years(CSV)')[0].split('"')[-1]
	return tb.tabarray(records = [x.tolist()[:-1] + ('http://www.bea.gov/national/nipaweb/' + URL, 'csv')], names = list(x.dtype.names)[:-1] + ['URL', 'Extension'])

def NEA_NIPA_CatalogInstantiator(creates = protocolroot + 'NEA_NIPA.py'):
	L = [NEA_NIPA_Parse1, {'Parser': NEA_NIPA_Parse2, 'Getter': WgetMultiple}, None]
	htools.hsuck('http://www.bea.gov/national/nipaweb/SelectTable.asp', root + 'NEA_NIPA/', L, ipath=creates)
		
def NEA_NIPA_ParseDownloads(depends_on=(root+'NEA_NIPA/Downloads_2/',root+'NEA_NIPA/Manifest_1.tsv'), creates=root+'NEA_NIPA/ParsedFiles/'):
	MakeDir(creates)
	M = tb.tabarray(SVfile=depends_on[1])
	for x in M:
		f = depends_on[0] + htools.pathprocessor([x[n] for n in M.dtype.names if n in M.coloring['Categories']]) + '.csv'
		savepath = creates + f.split('/')[-1].replace('!','_').replace('.csv','.tsv')
		print f
		[X, header, footer, keywords] = NEA_Parser(f, FootnoteSplitter='\\', FootnotesFunction=GetFootnotesLazy, CategoryColumn='Category',FormulaColumn='Category')		

		metadata = {}
		metadata['Header'] = '\n'.join(header)
		[title, units] = header[:2]
		notes = '\n'.join(header[2:-2])
		[owner, info] = header[-2:]
		metadata['title'] = title
		metadata['description'] = 'National Income and Product Accounts (NIPA) "' + title + '" from the <a href="http://www.bea.gov/national/nipaweb/SelectTable.asp?Selected=N">All NIPA Tables</a> data set under the <a href="http://www.bea.gov/national/index.htm">National Economic Accounts</a> section of the <a href="http://www.bea.gov/">Bureau of Economic Accounts (BEA)</a> website.  For additional information on the NIPAs, see: <a href="http://www.bea.gov/scb/pdf/misc/nipaguid.pdf">A Guide to the National Income and Product Accounts of the United States (PDF)</a>, <a href="http://www.bea.gov/scb/pdf/2009/11%20November/1109_nipa_method.pdf">Updated Summary of NIPA Methodologies (PDF)</a>, and <a href="http://www.bea.gov/scb/pdf/2003/08August/0803NIPA-Preview.pdf#page=9">Guide to the Numbering of the NIPA Tables (PDF)</a>.'
		metadata['Agency'] = 'DOC'
		metadata['Subagency'] = 'BEA'
		metadata['Type'] = 'National'
		metadata['Category'] = 'NIPA Tables'
		metadata['Section'] = x['Section']
		metadata['Table'] = ' '.join(title.split()[1:])
		metadata['Categories'] = ', '.join(['Agency', 'Subagency', 'Type', 'Category', 'Section', 'Table'])
		metadata['Units'] = units.strip('[]')
		metadata['Notes'] = notes
		metadata['Owner'] = owner
		metadata['DownloadedOn'] = info.split('Last')[0]
		metadata['LastRevised'] = info.split('Revised')[1].strip()		

		if footer:
			metadata['Footer'] = '\n'.join(footer)
	
		table = title.split()[1].strip('.')

		metadata['keywords'] = 'National Economic Accounts,' + ','.join(keywords)
			
		Years = [int(y[:4]) for y in X.coloring['Data']]
		metadata['TimePeriod'] = str(min(Years)) + '-' + str(max(Years))
		N = X.coloring['Data'][0]
		if len(N) == 4:
			metadata['DateDivisions'] = 'Years'
		elif 'I' in N:
			metadata['DateDivisions'] = 'Quarters'
		else:
			assert '-' in N
			metadata['DateDivisions'] = 'Months'
	
		X.metadata.update(metadata)
		X.saveSV(savepath, metadata=True, comments='#', delimiter='\t')


#############################################################################
#Fixed Asset Tables
#############################################################################

def NEA_FixedAssetTablesInstantiator(creates = protocolroot + 'NEA_FixedAssetTables.py'):
	L = [{'Parser':NEA_FixedAssetTables_Parse1,'Getter':WgetMultiple},{'Parser': NEA_FixedAssetTables_Parse2, 'Getter': WgetMultiple},None]
	htools.hsuck('http://www.bea.gov/national/FA2004/SelectTable.asp', root + 'NEA_FixedAssetTables/', L, ipath=creates)

def NEA_FixedAssetTables_Parse1(path,x):
	Soup = BeautifulSoup(open(path),convertEntities=BeautifulStoneSoup.HTML_ENTITIES)
	c1 = lambda x : x.name == 'a' and 'name' in dict(x.attrs).keys() and dict(x.attrs)['name'].startswith('S')
	c2 = lambda x : x.name == 'tr' and 'class' in dict(x.attrs).keys() and dict(x.attrs)['class'] == 'TR' and x.findAll('a') and 'href' in dict(x.findAll('a')[0].attrs).keys() and  dict(x.findAll('a')[0].attrs)['href'].startswith('Table')
	
	p1 = lambda x : Contents(x).strip().strip('\xc2\xa0').strip()
	p2 = lambda x : (p1(x),'http://www.bea.gov/national/FA2004/' + str(dict(x.findAll('a')[0].attrs)['href']) + '&AllYearsChk=YES')
	
	X = htools.MakeTable(Soup,[c1,c2],[p1,p2],['Section',['Table','URL']])
	X.coloring['Categories'] = ['Section','Table']
	return X

def NEA_FixedAssetTables_Parse2(path,x):
	Soup = BeautifulSoup(open(path),convertEntities=BeautifulStoneSoup.HTML_ENTITIES)
	c = lambda x : x.name == 'a' and 'Download All Years' in Contents(x)
	url = 'http://www.bea.gov/national/FA2004/' + str(dict(Soup.findAll(c)[0].attrs)['href'])
	X = tb.tabarray(records = [(url,'csv')],names = ['URL','Extension'])
	print url
	return X

def NEA_FixedAssetTables_ParseDownloads(depends_on=(root+'NEA_FixedAssetTables/Downloads_2/',root+'NEA_FixedAssetTables/Manifest_2.tsv'), creates=root+'NEA_FixedAssetTables/ParsedFiles/'):
	MakeDir(creates)
	flist = [depends_on[0]+f for f in listdir(depends_on[0]) if f.endswith('.csv')]
	M = tb.tabarray(SVfile=depends_on[1])
	for x in M:
		f = depends_on[0] + htools.pathprocessor([x[n] for n in M.dtype.names if n in M.dtype.names if n in M.coloring['Categories']]) + '.csv'
		[X, header, footer, keywords] = NEA_Parser(f, FootnoteSplitter='\\', FootnotesFunction=GetFootnotesLazy, CategoryColumn='Category', FormulaColumn='Category')		
		savepath = creates + f.split('/')[-1].replace('!','_').replace('.csv','.tsv')
		
		metadata = {}
		metadata['Header'] = '\n'.join(header)
		(title, units, bea) = header
		metadata['title'] = title
		metadata['description'] = 'Fixed Asset "' + title + '" from the <a href="http://www.bea.gov/national/FA2004/SelectTable.asp">Standard Fixed Asset Tables</a> data set under the <a href="http://www.bea.gov/national/index.htm">National Economic Accounts</a> section of the <a href="http://www.bea.gov/">Bureau of Economic Accounts (BEA)</a> website.  For additional information on the Fixed Asset Tables, see: <a href="http://www.bea.gov/national/pdf/Fixed_Assets_1925_97.pdf"> Methodology, Fixed Assets and Consumer Durable Goods in the United States, 1925-97 | September 2003 (PDF)</a>, <a href="http://www.bea.gov/scb/pdf/national/niparel/1997/0797fr.pdf">The Measurement of Depreciation in the NIPA\'s | SCB, July 1997 (PDF) </a>, and <a href="http://www.bea.gov/national/FA2004/Tablecandtext.pdf">BEA Rates of Depreciation, Service Lives, Declining-Balance Rates, and Hulten-Wykoff categories | February 2008  (PDF)</a>.'
		metadata['Agency'] = 'DOC'
		metadata['Subagency'] = 'BEA'
		metadata['Type'] = 'National'
		metadata['Category'] = 'Fixed Asset Tables'
		metadata['Table'] = ' '.join(title.split()[1:])
		metadata['Section'] = x['Section']
		metadata['Categories'] = ', '.join(['Agency', 'Subagency', 'Type', 'Category', 'Section', 'Table'])
		metadata['Units'] = units.strip('[]')
		if footer:
			metadata['Footer'] = '\n'.join(footer)

		Years = [int(y[:4]) for y in X.coloring['Data']]
		metadata['TimePeriod'] = str(min(Years)) + '-' + str(max(Years))
		N = X.coloring['Data'][0]
		if len(N) == 4:
			metadata['DateDivisions'] = 'Years'
		elif 'I' in N:
			metadata['DateDivisions'] = 'Quarters'
		else:
			assert '-' in N
			metadata['DateDivisions'] = 'Months'
		metadata['keywords'] = 'National Economic Accounts,' + ','.join(keywords)		
			
		X.metadata.update(metadata)
		X.saveSV(savepath, metadata=True, comments='#', delimiter='\t')
		

#############################################################################
# Unpublished NIPA Tables
#############################################################################


def NEA_NIPA_Unpublished_Parse1(page, x):
	S = [s.strip() for s in open(page, 'r').read().split('Section')[1:]]
	Section_SplitSoup = [(s.split('-')[1].split('<')[0].strip(), BeautifulSoup(s,convertEntities=BeautifulStoneSoup.HTML_ENTITIES)) for s in S]
	D = {'(A)': 'Year', '(Q)': 'Qtr', '(M)': 'Month'}
	Recs = []
	for (Section, Soup) in Section_SplitSoup:
		alist = [tr.findAll('a')[0] for tr in Soup.findAll('tr') if tr.findAll('a')]
		Table_URL = [(SafeContents(a), 'http://www.bea.gov/national/nipaweb/' + str(dict(a.attrs)['href'])) for a in alist if 'href' in dict(a.attrs).keys()]
		Number_Name_URL = [(t.split()[1], ' '.join(t.split()[2:]), url) for (t, url) in Table_URL]
		XYZ = [tuple(n[0].strip('.').split('.')) if len(n[0].strip('.').split('.'))==3 else tuple(n[0].strip('.').split('.')) + ('',) for n in Number_Name_URL]
		for i in range(len(XYZ)):
			(Section_Number, Subsection_Number, Table_Number) = XYZ[i]
			(Number, Name, u) = Number_Name_URL[i]
			Dlist = [d for d in D.keys() if d in Name]
			for d in Dlist:
				Freq = D[d]
				URL = u.split('Freq=')[0] + 'Freq=' + Freq + '&' + '&'.join(u.split('Freq=')[1].split('&')[1:])
				Recs += [(Section, Section_Number, Subsection_Number, Table_Number, Number, Freq, Name, URL)]
	return tb.tabarray(records = Recs, names = ['Section', 'Section_Number', 'Subsection_Number', 'Table_Number', 'Number', 'Freq', 'Name', 'URL'], coloring = {'Categories': ['Number', 'Freq', 'Name']})
	
def	NEA_NIPA_Unpublished_Parse2(page, x):
	print page
	URL = open(page, 'r').read().split('">Download All Years(CSV)')[0].split('"')[-1]
	return tb.tabarray(records = [x.tolist()[:-1] + ('http://www.bea.gov/national/nipaweb/' + URL, 'csv')], names = list(x.dtype.names)[:-1] + ['URL', 'Extension'])

def NEA_NIPA_Unpublished_CatalogInstantiator(creates = protocolroot + 'NEA_NIPA_Unpublished.py'):
	L = [NEA_NIPA_Unpublished_Parse1, {'Parser': NEA_NIPA_Unpublished_Parse2, 'Getter': WgetMultiple}, None]
	htools.hsuck('http://www.bea.gov/national/nipaweb/SelectTable.asp?Selected=3&Unpub=Y', root + 'NEA_NIPA_Unpublished/', L, ipath=creates)

def NEA_MakeDir(creates=root+'NEA/'):
	MakeDir(creates)
		
def NEA_NIPA_Unpublished_ParseDownloads(depends_on=(root+'NEA_NIPA_Unpublished/Downloads_2/',root+'NEA_NIPA_Unpublished/Manifest_1.tsv'), creates=root+'NEA_NIPA_Unpublished/ParsedFiles/'):
	MakeDir(creates)
	M = tb.tabarray(SVfile=depends_on[1])
	for x in M:
		f = depends_on[0] + htools.pathprocessor([x[n] for n in M.dtype.names if n in M.coloring['Categories']]) + '.csv'
		savepath = creates + f.split('/')[-1].replace('!','_').replace('.csv','.tsv')
		print f
		[X, header, footer, keywords] = NEA_Parser(f, FootnoteSplitter='\\', FootnotesFunction=GetFootnotesLazy, CategoryColumn='Category',FormulaColumn='Category')		
		metadata = {}

		metadata['Header'] = '\n'.join(header)
		[title, units] = header[:2]
		notes = '\n'.join(header[2:-2])
		[owner, info] = header[-2:]
		metadata['title'] = 'Unpublished ' + title
		metadata['description'] = 'Unpublished National Income and Product Accounts (NIPA) "' + title + '" from the <a href="http://www.bea.gov/national/nipaweb/SelectTable.asp?Selected=3&Unpub=Y">unpublished</a> data set under the <a href="http://www.bea.gov/national/index.htm">National Economic Accounts</a> section of the <a href="http://www.bea.gov/">Bureau of Economic Accounts (BEA)</a> website.  For additional information on the NIPAs, see: <a href="http://www.bea.gov/scb/pdf/misc/nipaguid.pdf">A Guide to the National Income and Product Accounts of the United States (PDF)</a>, <a href="http://www.bea.gov/scb/pdf/2009/11%20November/1109_NIPA_Unpublished_method.pdf">Updated Summary of NIPA Methodologies (PDF)</a>, and <a href="http://www.bea.gov/scb/pdf/2003/08August/0803NIPA-Preview.pdf#page=9">Guide to the Numbering of the NIPA Tables (PDF)</a>.  All unpublished NIPA tables are in units of millions of dollars.  See the cautionary note below.'
		metadata['Agency'] = 'DOC'
		metadata['Subagency'] = 'BEA'
		metadata['Type'] = 'National'
		metadata['Category'] = 'NIPA Tables Unpublished'
		metadata['Section'] = x['Section']
		metadata['Table'] = ' '.join(title.split()[1:])
		metadata['Categories'] = ', '.join(['Agency', 'Subagency', 'Type', 'Category', 'Section', 'Table'])
		metadata['Units'] = units.strip('[]')
		metadata['Notes'] = notes
		metadata['Owner'] = owner
		metadata['DownloadedOn'] = info.split('Last')[0]
		metadata['LastRevised'] = info.split('Revised')[1].strip()		

		if footer:
			metadata['Footer'] = '\n'.join(footer)
	
		table = title.split()[1].strip('.')

		metadata['keywords'] = 'National Economic Accounts,' + ','.join(keywords)
			
		Years = [int(y[:4]) for y in X.coloring['Data']]
		metadata['TimePeriod'] = str(min(Years)) + '-' + str(max(Years))
		N = X.coloring['Data'][0]
		if len(N) == 4:
			metadata['DateDivisions'] = 'Years'
		elif 'I' in N:
			metadata['DateDivisions'] = 'Quarters'
		else:
			assert '-' in N
			metadata['DateDivisions'] = 'Months'
	
		X.metadata.update(metadata)
		X.saveSV(savepath, metadata=True, comments='#', delimiter='\t')

	
#############################################################################
# Underlying Detail - NIPA Tables
#############################################################################

def	NEA_NIPA_UnderlyingDetail_Parse1(page, x):
	S = [s.strip() for s in open(page, 'r').read().split('Section')[1:]]
	Section_SplitSoup = [(s.split('-')[1].split('<')[0].strip(), BeautifulSoup(s,convertEntities=BeautifulStoneSoup.HTML_ENTITIES)) for s in S]
	D = {'(A)': 'Year', '(Q)': 'Qtr', '(M)': 'Month'}
	Recs = []
	for (Section, Soup) in Section_SplitSoup:
		alist = [tr.findAll('a')[0] for tr in Soup.findAll('tr') if tr.findAll('a')]
		Table_URL = [(SafeContents(a), 'http://www.bea.gov/national/nipaweb/' + str(dict(a.attrs)['href'])) for a in alist if 'href' in dict(a.attrs).keys()]
		Number_Name_URL = [(t.split()[1], ' '.join(t.split()[2:]), url) for (t, url) in Table_URL]
		for (Number, Name, u) in Number_Name_URL:
			Dlist = [d for d in D.keys() if d in Name]
			for d in Dlist:
				Freq = D[d]
				URL = u.split('Freq=')[0] + 'Freq=' + Freq + '&' + '&'.join(u.split('Freq=')[1].split('&')[1:])
				Recs += [(Section, Number, Freq, Name, URL)]
	return tb.tabarray(records = Recs, names = ['Section', 'Number', 'Freq', 'Name', 'URL'], coloring = {'Categories': ['Number', 'Freq', 'Name']})
	
def	NEA_NIPA_UnderlyingDetail_Parse2(page, x):
	print page
	URL = open(page, 'r').read().split('">Download All Years(CSV)')[0].split('"')[-1]
	return tb.tabarray(records = [x.tolist()[:-1] + ('http://www.bea.gov/national/nipaweb/' + URL, 'csv')], names = list(x.dtype.names)[:-1] + ['URL', 'Extension'])

def NEA_NIPA_UnderlyingDetail_CatalogInstantiator(creates = protocolroot + 'NEA_NIPA_UnderlyingDetail.py'):
	L = [NEA_NIPA_UnderlyingDetail_Parse1, {'Parser': NEA_NIPA_UnderlyingDetail_Parse2, 'Getter': WgetMultiple}, None]
	htools.hsuck('http://www.bea.gov/national/nipaweb/nipa_underlying/SelectTable.asp', root + 'NEA_NIPA_UnderlyingDetail/', L, ipath=creates)
	
def NEA_NIPA_UnderlyingDetail_ParseDownloads(depends_on=(root+'NEA_NIPA_UnderlyingDetail/Downloads_2/',root+'NEA_NIPA_UnderlyingDetail/Manifest_1.tsv'), creates=root+'NEA_NIPA_UnderlyingDetail/ParsedFiles/'):
	MakeDir(creates)
	M = tb.tabarray(SVfile=depends_on[1])
	for x in M:
		f = depends_on[0] + htools.pathprocessor([x[n] for n in M.dtype.names if n in M.coloring['Categories']]) + '.csv'
		savepath = creates + f.split('/')[-1].replace('!','_').replace('.csv','.tsv')
		print f
		[X, header, footer, keywords] = NEA_Parser(f, FootnoteSplitter='\\', FootnotesFunction=GetFootnotesLazy, CategoryColumn='Category',FormulaColumn='Category')		
		metadata = {}

		metadata['Header'] = '\n'.join(header)
		[title, units] = header[:2]
		notes = '\n'.join(header[2:-2])
		[owner, info] = header[-2:]
		metadata['title'] = 'Underlying Detail ' + title
		metadata['description'] = 'National Income and Product Accounts (NIPA) Underlying Detail "' + title + '" from the <a href="http://www.bea.gov/national/nipaweb/nipa_underlying/SelectTable.asp">Underlying Detail</a> data set under the <a href="http://www.bea.gov/national/index.htm">National Economic Accounts</a> section of the <a href="http://www.bea.gov/">Bureau of Economic Accounts (BEA)</a> website.  For additional information on the NIPAs, see: <a href="http://www.bea.gov/scb/pdf/misc/nipaguid.pdf">A Guide to the National Income and Product Accounts of the United States (PDF)</a>, <a href="http://www.bea.gov/scb/pdf/2009/11%20November/1109_NIPA_UnderlyingDetail_method.pdf">Updated Summary of NIPA Methodologies (PDF)</a>, and <a href="http://www.bea.gov/scb/pdf/2003/08August/0803NIPA-Preview.pdf#page=9">Guide to the Numbering of the NIPA Tables (PDF)</a>.   "Cautionary note on the use of underlying detail -- The tables provided include detailed estimates of underlying NIPA series that appear regularly in the national income and product account (NIPA) tables published elsewhere on [the Web site <a href="www.bea.gov">www.bea.gov</a>] and in the Survey of Current Business. The Bureau of Economic Analysis (BEA) does not include these detailed estimates in the published tables because their quality is significantly less than that of the higher level aggregates in which they are included. Compared to these aggregates, the more detailed estimates are more likely to be either based on judgmental trends, on trends in the higher level aggregate, or on less reliable source data."'
		metadata['Agency'] = 'DOC'
		metadata['Subagency'] = 'BEA'
		metadata['Type'] = 'National'
		metadata['Category'] = 'NIPA Tables Underlying Detail'
		metadata['Section'] = x['Section']
		metadata['Table'] = ' '.join(title.split()[1:])
		metadata['Categories'] = ', '.join(['Agency', 'Subagency', 'Type', 'Category', 'Section', 'Table'])
		metadata['Units'] = units.strip('[]')
		metadata['Notes'] = notes
		metadata['Owner'] = owner
		metadata['DownloadedOn'] = info.split('Last')[0]
		metadata['LastRevised'] = info.split('Revised')[1].strip()		

		if footer:
			metadata['Footer'] = '\n'.join(footer)
	
		table = title.split()[1].strip('.')

		metadata['keywords'] = 'National Economic Accounts,' + ','.join(keywords)
			
		Years = [int(y[:4]) for y in X.coloring['Data']]
		metadata['TimePeriod'] = str(min(Years)) + '-' + str(max(Years))
		N = X.coloring['Data'][0]
		if len(N) == 4:
			metadata['DateDivisions'] = 'Years'
		elif 'I' in N:
			metadata['DateDivisions'] = 'Quarters'
		else:
			assert '-' in N
			metadata['DateDivisions'] = 'Months'
	
		X.metadata.update(metadata)
		X.saveSV(savepath, metadata=True, comments='#', delimiter='\t')


#############################################################################
#NEA pdf descriptions
#############################################################################

def NEA_AdditionalInformation_Parse1(page, x):
	Soup = BeautifulSoup(open(page,'r'),convertEntities=BeautifulStoneSoup.HTML_ENTITIES)
	PA = [(SafeContents(p), str(dict(p.findAll('a')[0].attrs)['href'])) for p in Soup.findAll('blockquote')[0].findAll('p')]
	Recs = [(p, 'http://www.bea.gov' + a, 'pdf') for (p,a) in PA]
	return tb.tabarray(records = Recs, names = ['Name', 'URL', 'Extension'], coloring = {'Categories': ['Name']})

def NEA_AdditionalInformation_CatalogInstantiator(creates = protocolroot + 'NEA_AdditionalInformation.py'):
	L = [NEA_AdditionalInformation_Parse1, None]
	htools.hsuck('http://www.bea.gov/national/nipaweb/Index.asp', root + 'NEA_AdditionalInformation/', L, ipath=creates)

	

#############################################################################
# REA MAIN
#############################################################################

def REA_Parse1(page,x):
	Soup = BeautifulSoup(open(page,'r'),convertEntities=BeautifulStoneSoup.HTML_ENTITIES)
	Level1_Name_Contents = tb.utils.listunion([[(SafeContents(L), str(dict(d.attrs)['class']).split('quick_')[-1], d) if SafeContents(d).strip('\xc2\xa0') != '' else (SafeContents(L), str(dict(d.attrs)['class']).split('quick_')[-1], '') for d in [div for div in q.findAll('div') if str(div).startswith('<div class="quick_')]] for (L, q) in zip(Soup.findAll('h2'), Soup.findAll('div', 'quick_wrap'))])
	Level1_Name_URL = [(L, n, 'http://www.bea.gov' + str(dict(c.findAll('a')[0].attrs)['href'])) if c != '' else (L, n, '') for (L,n,c) in Level1_Name_Contents]
	return tb.tabarray(records = Level1_Name_URL, names = ['Category', 'DataType', 'URL'], coloring = {'Categories': ['Category', 'Data_Type']})
	
def REACatalogInstantiator(creates = protocolroot + 'REA.py'):
	L = [REA_Parse1, None]
	htools.hsuck('http://www.bea.gov/regional/quick.cfm', root + 'REA/', L, ipath=creates)
	 	

#############################################################################
# REA GDP by state
#############################################################################

		
def REA_GDPByState_Parse1(page, x):
	Soup = BeautifulSoup(open(page,'r'),convertEntities=BeautifulStoneSoup.HTML_ENTITIES)
	Category = SafeContents(Soup.findAll('h1')[0])
	SeriesYearsDict = dict([(str(c).split()[0], [int(i) for i in str(c).split()[1].strip('()').split('-')]) for c in Soup.findAll('form')[0].findAll('table')[0].findAll('tr')[0].findAll('div')[0].contents if isinstance(c, NavigableString) and c.strip() != ''])
	for series in SeriesYearsDict.keys():
		(a,b) = SeriesYearsDict[series]
		A = range(a, b, 20)
		B = range(a+20, b, 20) + [b+1]
		SeriesYearsDict[series] = [(str(a)+'-'+str(b-1), '&'.join(['selYears=' + str(y) for y in range(a,b)])) for (a,b) in zip(A,B)]
	page = 'file:' + os.getcwd() + '/' + page
	br = Browser()
	br.open(page)
	br.select_form(nr=0)
	D = dict([(c.name, c) for c in br.form.controls])
	series_values = [i.attrs['value'] for i in D['series'].items]
	selFips_values = [(i.attrs['label'], i.attrs['value']) for i in D['selFips'].items]
	Recs = tb.utils.listunion(tb.utils.listunion([[[(Category, series, region, years, x['URL'] + "action.cfm?querybutton=Download%20CSV&selFips=" + selFips + "&selLineCode=ALL&selTable=ALL&" + yearlist + "&series=" + series, 'csv') for (years, yearlist) in SeriesYearsDict[series]] for (region, selFips) in selFips_values if selFips != 'ALL'] for series in series_values]))
	return tb.tabarray(records=Recs, names=['Category', 'Series', 'Region', 'Years', 'URL', 'Extension'], coloring={'Categories': ['Category', 'Series', 'Region', 'Years']})
			
def REA_GDPByState_CatalogInstantiator(depends_on = root + 'REA/Manifest_1.tsv', creates = protocolroot + 'REA_GDPByState.py'):
	M = tb.tabarray(SVfile=depends_on)
	URL = M[(M['Category'] == 'Gross Domestic Product (GDP) by State') & (M['DataType'] == 'estimates')]['URL'][0]
	L = [{'Parser': REA_GDPByState_Parse1, 'Getter': WgetMultiple}, None]
	htools.hsuck(URL, root + 'REA_GDPByState/', L, ipath=creates)


def REA_GDPByState_ParseDownloads(depends_on=(root+'REA_GDPByState/Downloads_1/', root+'REA_GDPByState/Manifest_1.tsv'), creates=root+'REA_GDPByState/ParsedFiles/'):
	MakeDir(creates)
	M = tb.tabarray(SVfile=depends_on[1])
	for x in M:
		(ct, IC, Region, dr, URL, ex) = x
		f = depends_on[0] + htools.pathprocessor([x[n] for n in M.dtype.names if n in M.coloring['Categories']]) + '.csv'	
		savepath = creates + f.split('/')[-1].replace('!','_').replace('.csv','.tsv')
		[X, header, footer, keywords] = BEA_Parser(f, headerlines=1, FootnoteSplitter='\\', FootnotesFunction=GetFootnotes2, CategoryColumn='Industry')
		
		p = re.compile('\(.*\)')
		ParsedComp = [p.sub('',x).strip() for x in X['Component']]
		Units = [x[p.search(x).start()+1:p.search(x).end()-1] for x in X['Component']]
		X = X.addcols([ParsedComp,Units],names=['ParsedComponent','Units'])
			
		metadata = {}

		Years = [int(y[:4]) for y in X.coloring['Data'] if y[:4].isdigit()]
		N = X.coloring['Data'][0]
		if '-' in N:
			assert N.replace('-','').isdigit()
			assert int(N.split('-')[1]) - int(N.split('-')[0]) == 1		
			metadata['TimePeriod'] = str(min(Years)) + '-' + str(max(Years)+1)
			metadata['DateDivisions'] = 'Yearly Differences'
		else:
			assert len(N) == 4
			metadata['TimePeriod'] = str(min(Years)) + '-' + str(max(Years))
			metadata['DateDivisions'] = 'Years'
		
		metadata['keywords'] = 'Regional Economic Accounts,' + ','.join(keywords)
		metadata['title'] = 'GDP by State: ' + Region + ' (' + IC + ', ' + metadata['TimePeriod'] + ')'
		metadata['description'] = 'Gross domestic product (GDP) for a single state or larger region, ' + Region + ', for the years ' + metadata['TimePeriod'] + ', and using the ' + IC + ' industry classification.  This data comes from the <a href="http://www.bea.gov/regional/gsp/">GDP by State</a> data set under the <a href="http://www.bea.gov/regional/index.htm">Regional Economic Accounts</a> section of the <a href="http://www.bea.gov/">Bureau of Economic Accounts (BEA)</a> website.  For more information on the industry classifications, see the BEA web pages on  <a href="http://www.bea.gov/regional/definitions/nextpage.cfm?key=NAICS">NAICS (1997-2008)</a> and <a href="http://www.bea.gov/regional/definitions/nextpage.cfm?key=SIC">SIC (1963-1997)</a>.  Component units are as follows:  Gross Domestic Product by State (millions of current dollars), Compensation of Employees (millions of current dollars), Taxes on Production and Imports less Subsidies (millions of current dollars), Gross Operating Surplus (millions of current dollars), Real GDP by state (millions of chained 2000 dollars), Quantity Indexes for Real GDP by State (2000=100.000), Subsidies (millions of current dollars), Taxes on Production and Imports (millions of current dollars), Per capita real GDP by state (chained 2000 dollars).'
		metadata['Agency'] = 'DOC'
		metadata['Subagency'] = 'BEA'
		metadata['Type'] = 'Regional'
		metadata['Category'] = 'GDP by State'
		metadata['IndustryClassification'] = IC
		metadata['Region'] = Region
		metadata['FIPS'] = X['FIPS'][0]
		metadata['Categories'] = ', '.join(['Agency', 'Subagency', 'Type', 'Category', 'IndustryClassification', 'Region', 'TimePeriod'])
		metadata['URL'] = URL
		metadata['LastRevised'] = 'June 2009'
		if footer:
			footer = CleanLinesForMetadata(footer)
			metadata['footer'] = '\n'.join(footer)
			[source] = footer
			metadata['Source'] = source.split('Source:')[1].strip()
		
		X.metadata = metadata
		X.metadata['unitcol'] = ['Units']
		X.metadata['labelcollist'] = ['Industry','ParsedComponent']
		X.saveSV(savepath, metadata=True, comments='#', delimiter='\t')


#############################################################################
# REA GDP by metropolitan area
#############################################################################

		
		
def REA_GDPByMetropolitanArea_Parse1(page,x):
	Soup = BeautifulSoup(open(page,'r'),convertEntities=BeautifulStoneSoup.HTML_ENTITIES)
	Category = SafeContents(Soup.findAll('h1')[0])
	URL = 'http://www.bea.gov' + str(dict(Soup.findAll('h2', id='download')[0].findNext().findAll('a')[0].attrs)['href'])
	return tb.tabarray(records = [(Category, URL, 'zip')], names = ['Category', 'URL', 'Extension'], coloring = {'Categories': ['Category']})

def REA_GDPByMetropolitanArea_CatalogInstantiator(depends_on = root + 'REA/Manifest_1.tsv', creates = protocolroot + 'REA_GDPByMetropolitanArea.py'):
	M = tb.tabarray(SVfile=depends_on)
	URL = M[(M['Category'] == 'Gross Domestic Product (GDP) by Metropolitan Area') & (M['DataType'] == 'estimates')]['URL'][0]
	L = [{'Parser': REA_GDPByMetropolitanArea_Parse1, 'Getter': WgetMultiple}, None]
	htools.hsuck(URL, root + 'REA_GDPByMetropolitanArea/', L, ipath=creates)

def REA_GDPByMetropolitanArea_Unzip(depends_on = root + 'REA_GDPByMetropolitanArea/Downloads_1/GrossDomesticProductbyMetropolitanArea.zip', creates = root + 'REA_GDPByMetropolitanArea/GrossDomesticProductbyMetropolitanArea/'):
	os.system('unzip -d ' + creates + ' ' + depends_on)
	
def REA_GDPByMetropolitanArea_ParseDownloads(depends_on=root+'REA_GDPByMetropolitanArea/GrossDomesticProductbyMetropolitanArea/', creates=root+'REA_GDPByMetropolitanArea/ParsedFiles/'):
	MakeDir(creates)
	flist = [depends_on+f for f in listdir(depends_on) if f.endswith('.csv') and f.startswith('gmp')]
	for f in flist:
		print f
		savepath = creates + f.split('/')[-1].replace('!','_').replace('.csv','.tsv')
		[X, header, footer, keywords] = BEA_Parser(f, headerlines=1, CategoryColumn='industry_name')
	
		p = re.compile('\(.*\)')
		ParsedComp = [p.sub('',x).strip() for x in X['component_name']]
		Units = [x[p.search(x).start()+1:p.search(x).end()-1] for x in X['component_name']]
		X = X.addcols([ParsedComp,Units],names=['ParsedComponent','Units'])
		
		
		metadata = {}
		metadata['keywords'] = 'Regional Economic Accounts,' + ','.join(keywords)
		MSA = X['area_name'][0].split('(MSA)')[0]
		metadata['title'] = 'GDP by Metropolitan Area: ' + MSA + ' (FIPS=' + X['FIPS'][0] + ')'
		metadata['description'] = 'Gross domestic product (GDP) for a single Metropolitan Statistical Area (MSA) -- ' + MSA + ' (FIPS=' + X['FIPS'][0] + ') -- from the <a href="http://www.bea.gov/regional/gdpmetro/">GDP by Metropolitan Areas</a> data set under the <a href="http://www.bea.gov/regional/index.htm">Regional Economic Accounts</a> section of the <a href="http://www.bea.gov/">Bureau of Economic Accounts (BEA)</a> website.  Note that NAICS industry detail is based on the 1997 NAICS.  For more information on Metropolitan Statistical Areas, see the BEA website on <a href="http://www.bea.gov/regional/docs/msalist.cfm?mlist=45">Statistical Areas</a>.  Component units are as follows:  GDP by Metropolitan Area (millions of current dollars), Quantity Indexes for Real GDP by Metropolitan Area (2001=100.000), Real GDP by Metropolitan Area (millions of chained 2001 dollars), Per capita real GDP by Metropolitan Area (chained 2001 dollars).'
		metadata['Agency'] = 'DOC'
		metadata['Subagency'] = 'BEA'
		metadata['Type'] = 'Regional'
		metadata['Category'] = 'GDP by Metropolitan Area'
		metadata['Region'] = MSA
		metadata['Categories'] = ', '.join(['Agency', 'Subagency', 'Type', 'Category', 'Region'])
		metadata['FIPS'] = X['FIPS'][0]		
		if footer:
			metadata['footer'] = '\n'.join(footer)
			metadata['Notes'] = '\n'.join([x.split('Note:')[1].strip() for x in footer[:-1]])
			metadata['Source'] = footer[-1].split('Source:')[1].strip()
			metadata['LastRevised'] = metadata['Source'].split('--')[-1].strip()
	
		Years = [int(y[:4]) for y in X.coloring['Data'] if y[:4].isdigit()]
		N = X.coloring['Data'][0]
		if '-' in N:
			assert N.replace('-','').isdigit()
			assert int(N.split('-')[1]) - int(N.split('-')[0]) == 1		
			metadata['TimePeriod'] = str(min(Years)) + '-' + str(max(Years)+1)
			metadata['DateDivisions'] = 'Yearly Differences'
		else:
			assert len(N) == 4
			metadata['TimePeriod'] = str(min(Years)) + '-' + str(max(Years))
			metadata['DateDivisions'] = 'Years'
		
		X.metadata = metadata
		X.metadata['unitcol'] = ['Units']
		X.metadata['labelcollist'] = ['industry_name','ParsedComponent']
		X.saveSV(savepath, metadata=True, comments='#', delimiter='\t')
		
		
#############################################################################
# REA State Quarterly Personal income
#############################################################################
		

def REA_StateQuarterlyPersonalIncome_Parse1(page,x):
	print page
	Soup = BeautifulSoup(open(page,'r'),convertEntities=BeautifulStoneSoup.HTML_ENTITIES)
	Category = SafeContents(Soup.findAll('h1')[0])
	R = [(Category, x['URL'] + str(dict(a.attrs)['href'])) for a in Soup.findAll('h2', id='step1')[0].findNext().findNext().findAll('a')]
	Recs = []
	for rec in R:
		D = dict([tuple(x.split('=')) for x in rec[1].split('?')[-1].split('&')])
		if not 'selSeries' in D:
			Recs += [(rec[0], D['selTable'], '', rec[1])]
		else:
			Recs += [(rec[0], D['selTable'], D['selSeries'], rec[1])]
			D['selTable'] = D['selTable'].strip('N')
			D['selSeries'] = 'SIC'
			Recs += [(rec[0], D['selTable'], D['selSeries'], rec[1])]		
	return tb.tabarray(records = Recs, names = ['Category', 'Subcategory', 'Series', 'URL'], coloring = {'Categories': ['Category', 'Subcategory', 'Series']})

def REA_StateQuarterlyPersonalIncome_Parse2(page,x):	
	print page
	Soup = BeautifulSoup(open(page,'r'),convertEntities=BeautifulStoneSoup.HTML_ENTITIES)
	S = [h for h in Soup.findAll('h2') if str(h).startswith('<h2 id="step')][-1]
	try:
		action = str(dict(S.findParent().attrs)['action'])
	except:
		action = str(dict(S.findParent().findAll('form')[0].attrs)['action'])
	P = tb.utils.listunion([[(str(dict(s.attrs)['name']), str(dict(o.attrs)['value']), SafeContents(o)) for o in s.findAll('option')] for s in S.findNext().findNext().findAll('select')])
	URL = x['URL'].replace('default.cfm', action) + '&rformat=Download&selLineCode=10&' + '&'.join([key + '=' + val for (key, val, name) in P])
	return tb.tabarray(records = [(x['Category'], x['Subcategory'], x['Series'], URL, 'csv')], names = ['Category', 'Subcategory', 'Series', 'URL', 'Extension'], coloring = {'Categories': ['Category', 'Subcategory', 'Series']})
	
def REA_StateQuarterlyPersonalIncome_CatalogInstantiator(depends_on = root + 'REA/Manifest_1.tsv', creates = protocolroot + 'REA_StateQuarterlyPersonalIncome.py'):
	M = tb.tabarray(SVfile=depends_on)
	URL = M[(M['Category'] == 'Quarterly State Personal Income') & (M['DataType'] == 'estimates')]['URL'][0]
	L = [{'Parser': REA_StateQuarterlyPersonalIncome_Parse1, 'Getter': WgetMultiple}, {'Parser': REA_StateQuarterlyPersonalIncome_Parse2, 'Getter': WgetMultiple}, None]
	htools.hsuck(URL, root + 'REA_StateQuarterlyPersonalIncome/', L, ipath=creates)
	
def REA_StateQuarterlyPersonalIncome_DownloadZip(depends_on = '../Data/OpenGovernment/BEA/REA_StateQuarterlyPersonalIncome/Downloads_0/root.html', creates = root + 'REA_StateQuarterlyPersonalIncome/sqpi.zip'):
	Soup = BeautifulSoup(open(depends_on,'r'),convertEntities=BeautifulStoneSoup.HTML_ENTITIES)
	wget('http://www.bea.gov/regional/sqpi/' + str(dict(Soup.findAll('h2', id='download')[0].findNext().findAll('a')[0].attrs)['href']), creates)

def REA_StateQuarterlyPersonalIncome_Unzip(depends_on = root + 'REA_StateQuarterlyPersonalIncome/sqpi.zip', creates = root + 'REA_StateQuarterlyPersonalIncome/sqpi/'):
	os.system('unzip -d ' + creates + ' ' + depends_on)
	flist = [creates + f for f in listdir(creates) if f.endswith('.csv')]
	for f in flist:
		F = open(f, 'rU').read().strip('\n').split('\n')
		Source = [line for line in F if line.strip('"').startswith('Source')]
		F = [line for line in F if not line.strip('"').startswith('Source')]
		F += Source
		G = open(f, 'w')
		G.write('\n'.join(F))

def REA_StateQuarterlyPersonalIncome_ParseDownloads(depends_on = root + 'REA_StateQuarterlyPersonalIncome/sqpi/', creates = root + 'REA_StateQuarterlyPersonalIncome/ParsedFiles/'):
	MakeDir(creates)
	flist = [depends_on + f for f in listdir(depends_on) if f.endswith('.csv')]
	for f in flist:
		print f
		savepath = creates + f.split('/')[-1].replace('!','_').replace('.csv','.tsv')
		[X, header, footer, keywords] = BEA_Parser(f, headerlines=1, FootnoteSplitter='/', FootnotesFunction=GetFootnotes, CategoryColumn='Line Title')
		metadata = {}
		metadata['keywords'] = 'Regional Economic Accounts,' + ','.join(keywords)
		metadata['Agency'] = 'DOC'
		metadata['Subagency'] = 'BEA'
		metadata['Type'] = 'Regional'
		metadata['Category'] = 'State Quarterly Personal Income'
		tn = f.split('/')[-1].split('_')[0] + 'fn.txt'
		tnfiles = [fn for fn in listdir(depends_on) if fn.lower().endswith(tn.lower())]
		if tnfiles:
			tnfile = depends_on + tnfiles[0]
			print 'Getting table footnote data from', tnfile
			F = open(tnfile, 'rU').read().rstrip().split('\n')
			line = F[0]
			while line.strip() == '':
				F = F[1:]
				line = F[0]
			metadata['TableFootnotes'] = '\n'.join(F)
			metadata['Table'] = tn + ' ' + F[1].strip()
			metadata['Units'] = F[2].strip().strip('()')

		else:
			print 'No tablefootnote file found'
			for k in ['TableFootnotes','Table','Units']:
				metadata[k] = ''
			 
		metadata['Region'] = X['State Name'][0]
		metadata['FIPS'] = X['State FIPS'][0]
		metadata['Categories'] = ', '.join(['Agency', 'Subagency', 'Type', 'Category', 'Table', 'Region'])
		metadata['title'] = 'Table ' + metadata['Table'] + ', ' + metadata['Region']
		metadata['LastRevised'] = metadata['TableFootnotes'].split('\n')[-1]
		metadata['description'] = 'Table "' + metadata['Table'] + '" for the state or larger region, ' + metadata['Region'] + ', from the <a href="http://www.bea.gov/regional/sqpi/">State Quarterly Personal Income</a> data set under the <a href="http://www.bea.gov/regional/index.htm">Regional Economic Accounts</a> section of the <a href="http://www.bea.gov/">Bureau of Economic Accounts (BEA)</a> website.'
		if footer:
			metadata['footer'] = '\n'.join(footer)
			[source] = footer
			metadata['Source'] = source.split('Source:')[1].strip()
	
		Years = [int(y[:4]) for y in X.coloring['Data'] if y[:4].isdigit()]
		N = X.coloring['Data'][0]
		if '-' in N:
			assert N.replace('-','').isdigit()
			assert int(N.split('-')[1]) - int(N.split('-')[0]) == 1		
			metadata['TimePeriod'] = str(min(Years)) + '-' + str(max(Years)+1)
			metadata['DateDivisions'] = 'Yearly Differences'
		elif '.' in N:
			assert N.replace('.','').isdigit()
			assert len(N) == 6 and N[-1] in ['1','2','3','4']
			metadata['TimePeriod'] = str(min(Years)) + '-' + str(max(Years))
			metadata['DateDivisions'] = 'Quarters'
		else:
			assert len(N) == 4
			metadata['TimePeriod'] = str(min(Years)) + '-' + str(max(Years))
			metadata['DateDivisions'] = 'Years'
		
		X.metadata = metadata			
		X.metadata['labelcollist'] = ['Line Title']
		X.saveSV(savepath, metadata=True, comments='#', delimiter='\t')		
		
def REA_StateQuarterlyPersonalIncome_GetReadmeFiles(depends_on = root + 'REA_StateQuarterlyPersonalIncome/sqpi/', creates = root + 'REA_StateQuarterlyPersonalIncome/ReadmeFiles/'):
	MakeDir(creates)
	flist = [depends_on + f for f in listdir(depends_on) if f.endswith('.txt')]
	for f in flist:
		flist = [depends_on + f for f in listdir(depends_on) if f.endswith('.txt')]
		for f in flist:
			os.system('cp ' + f + ' ' + creates + f.split('/')[-1])
			
			
#############################################################################
# REA State Annual Personal income
#############################################################################
					

def REA_StateAnnualPersonalIncome_Parse1(page,x):
	Soup = BeautifulSoup(open(page,'r'),convertEntities=BeautifulStoneSoup.HTML_ENTITIES)
	Category = SafeContents(Soup.findAll('h1')[0])
	F = Soup.findAll('form')[0]
	action = str(dict(F.attrs)['action'])
	P = [[(str(dict(s.attrs)['name']), str(dict(o.attrs)['value']), str(o.contents[0]).strip()) for o in s.findAll('option')] for s in F.findAll('select')][0]
	Recs = [(Category, name.replace('(','--').replace(')','--'), x['URL'] + str(dict(F.attrs)['action']) + '?DownloadZIP=Download%20ZIP&' + key + '=' + val, 'zip') for (key, val, name) in P]
	return tb.tabarray(records = Recs, names = ['Category', 'Subcategory', 'URL', 'Extension'], coloring = {'Categories': ['Category', 'Subcategory']})
	
def REA_StateAnnualPersonalIncome_CatalogInstantiator(depends_on = root + 'REA/Manifest_1.tsv', creates = protocolroot + 'REA_StateAnnualPersonalIncome.py'):
	M = tb.tabarray(SVfile=depends_on)
	URL = M[(M['Category'] == 'Annual State Personal Income and Employment') & (M['DataType'] == 'estimates')]['URL'][0]
	L = [{'Parser': REA_StateAnnualPersonalIncome_Parse1, 'Getter': WgetMultiple}, None]
	htools.hsuck(URL, root + 'REA_StateAnnualPersonalIncome/', L, ipath=creates)
	
def REA_StateAnnualPersonalIncome_Unzip(depends_on = root + 'REA_StateAnnualPersonalIncome/Downloads_1/', creates = root + 'REA_StateAnnualPersonalIncome/Downloads_1_Unzipped/'):
	MakeDir(creates)
	flist = [f for f in listdir(depends_on) if f.endswith('.zip')]
	for f in flist:
		os.system('unzip -d ' + creates + f.replace('.zip', '/') + ' ' + depends_on + f)
	fixlist = [creates + 'StateAnnualPersonalIncome!AllNAICStables1990-2008/SA07N_2001_2008_GTLK.csv'] + [creates + 'StateAnnualPersonalIncome!Allothertables--04,30,35,45,50--1969-2008/' + f for f in listdir(creates + 'StateAnnualPersonalIncome!Allothertables--04,30,35,45,50--1969-2008/')]
	for f in fixlist:
		F = open(f, 'rU').read().strip('\n').split('\n')
		Source = [line for line in F if line.strip('"').startswith('Source')]
		F = [line for line in F if not line.strip('"').startswith('Source')]
		F += Source
		G = open(f, 'w')
		G.write('\n'.join(F))
		

def REA_StateAnnualPersonalIncome_ParseDownloads(depends_on = (root + 'REA_StateAnnualPersonalIncome/Downloads_1_Unzipped/', root + 'REA_StateAnnualPersonalIncome/Manifest_1.tsv'), creates = root + 'REA_StateAnnualPersonalIncome/ParsedFiles/'):
	MakeDir(creates)
	M = tb.tabarray(SVfile=depends_on[1])
	for x in M:
		d = htools.pathprocessor([x[n] for n in M.dtype.names if n in M.coloring['Categories']])
		MakeDir(creates + d)
		flist = [depends_on[0] + d + '/' + f for f in listdir(depends_on[0] + d) if f.endswith('.csv')]
		if 'Readme.txt' in listdir(depends_on[0] + d):
			Summary = False
			GeneralDescription = open(depends_on[0] + d + '/Readme.txt', 'rU').read().strip()
		else:
			Summary = True
			R = open(depends_on[0] + d + '/readme_sum.txt', 'rU').read().strip().split('\n')			
			ind = [i for i in range(len(R)) if R[i].strip().startswith('FOOTNOTES')][0]
			GeneralDescription = '\n'.join(R[:ind-1])
			Footnotes = '\n'.join(R[ind+1:])
		for f in flist:
			print 'Processing', f
			savepath = creates + d + '/' + f.split('/')[-1].replace('!','_').replace('.csv','.tsv')
			[X, header, footer, keywords] = BEA_Parser(f, headerlines=1, FootnoteSplitter='/', FootnotesFunction=GetFootnotes, CategoryColumn='Line Title')
			metadata = {}
			metadata['keywords'] = 'Regional Economic Accounts,' + ','.join(keywords)
			metadata['Category Description'] = GeneralDescription
			metadata['Agency'] = 'DOC'
			metadata['Subagency'] = 'BEA'
			metadata['Type'] = 'Regional'
			metadata['Category'] = x['Category']
			metadata['Subcategory'] = x['Subcategory']
			metadata['URL'] = x['URL']
			if Summary:				
				metadata['Footnotes'] = Footnotes
				if f.split('/')[-1].startswith('SA1-3'):
					metadata['Table'] = 'SA1-3 Summary personal income'
				else:
					assert f.split('/')[-1].startswith('SA51-52')
					metadata['Table'] = 'SA51-52 Summary disposable personal income'
				metadata['Region'] = 'Aggregate'
				metadata['Categories'] = ', '.join(['Agency', 'Subagency', 'Type', 'Category', 'Subcategory', 'Table', 'Region'])
				metadata['title'] = metadata['Table']
				metadata['description'] = 'Summary Table "' + metadata['Table'] + '" from the <a href="http://www.bea.gov/regional/spi/">State Annual Personal Income</a> data set under the <a href="http://www.bea.gov/regional/index.htm">Regional Economic Accounts</a> section of the <a href="http://www.bea.gov/">Bureau of Economic Accounts (BEA)</a> website.  For more information on the industry classifications, see the BEA web pages on  <a href="http://www.bea.gov/regional/definitions/nextpage.cfm?key=NAICS">NAICS (1997-2008)</a> and <a href="http://www.bea.gov/regional/definitions/nextpage.cfm?key=SIC">SIC (1963-1997)</a>.  With the exception of per capita estimates, all dollar estimates are in thousands of dollars. All employment estimates are number of jobs. All dollar estimates are in current dollars.'				
				metadata['LastRevised'] = 'September 2009'
				unitsdict = {'personal income': 'Thousands of dollars', 'population': 'Number of persons', 'per capita personal income': 'Dollars'}
				units = [unitsdict[i.lower().replace('disposable ', '').strip()] for i in X['Line Title']]
				X = X['Info'].colstack(tb.tabarray(columns=[units], names=['Units'])).colstack(X['Footnotes']).colstack(X['Data'])
			else:
				tfnfile = depends_on[0] + d + '/' + f.split('/')[-1].split('_')[0] + 'foot.txt'
				if not PathExists(tfnfile):
					tfnfile = depends_on[0] + d + '/' + f.split('/')[-1].split('_')[0].lower() + 'foot.txt'
				if not PathExists(tfnfile):
					print 'No table footnot metadata file found.'
					metadata['TableFootnotes'] = ''
				else:
					print 'Getting table footnote metadata from', tfnfile
					metadata['TableFootnotes'] = open(tfnfile,'rU').read().strip()
				metadata['Table'] = metadata['TableFootnotes'].split('\n')[0].strip()
				metadata['Region'] = X['State Name'][0]
				metadata['FIPS'] = X['State FIPS'][0]
				metadata['Categories'] = ', '.join(['Agency', 'Subagency', 'Type', 'Category', 'Subcategory', 'Table', 'Region'])
				metadata['title'] = metadata['Table'] + ', ' + metadata['Region']
				metadata['description'] = 'Table "' + metadata['Table'] + '" for the state or larger region, ' + metadata['Region'] + ', from the <a href="http://www.bea.gov/regional/spi/">State Annual Personal Income</a> data set under the <a href="http://www.bea.gov/regional/index.htm">Regional Economic Accounts</a> section of the <a href="http://www.bea.gov/">Bureau of Economic Accounts (BEA)</a> website.  For more information on the industry classifications, see the BEA web pages on  <a href="http://www.bea.gov/regional/definitions/nextpage.cfm?key=NAICS">NAICS (1997-2008)</a> and <a href="http://www.bea.gov/regional/definitions/nextpage.cfm?key=SIC">SIC (1963-1997)</a>.'
				metadata['LastRevised'] = metadata['TableFootnotes'].split('\n')[-1]
			
			if footer:
				metadata['footer'] = '\n'.join(footer)
				[source] = footer
				metadata['Source'] = source.split('Source:')[1].strip()
		
			Years = [int(y[:4]) for y in X.coloring['Data'] if y[:4].isdigit()]
			N = X.coloring['Data'][0]
			if '-' in N:
				assert N.replace('-','').isdigit()
				assert int(N.split('-')[1]) - int(N.split('-')[0]) == 1		
				metadata['TimePeriod'] = str(min(Years)) + '-' + str(max(Years)+1)
				metadata['DateDivisions'] = 'Yearly Differences'
			else:
				assert len(N) == 4
				metadata['TimePeriod'] = str(min(Years)) + '-' + str(max(Years))
				metadata['DateDivisions'] = 'Years'
			
			X.metadata = metadata	
			if len(uniqify(X['Line Title'])) == len(X):
				X.metadata['labelcollist'] = ['Line Title']
			else:
				X.metadata['labelcollist'] = ['Line Title','State Name']
			X.saveSV(savepath, metadata=True, comments='#', delimiter='\t')			


def REA_StateAnnualPersonalIncome_GetReadmeFiles(depends_on = root + 'REA_StateAnnualPersonalIncome/Downloads_1_Unzipped/', creates = root + 'REA_StateAnnualPersonalIncome/ReadmeFiles/'):
	MakeDir(creates)
	dlist = [d for d in listdir(depends_on) if IsDir(depends_on + d)]
	for d in dlist:
		MakeDir(creates + d)
		flist = [depends_on + d + '/' + f for f in listdir(depends_on + d) if f.endswith('.txt')]
		for f in flist:
			os.system('cp ' + f + ' ' + creates + d + '/' + f.split('/')[-1])
			
			
#############################################################################
# REA Local Area Personal income
#############################################################################
					

def REA_LocalAreaPersonalIncome_Parse1(page,x):
	Soup = BeautifulSoup(open(page,'r'),convertEntities=BeautifulStoneSoup.HTML_ENTITIES)
	Category = SafeContents(Soup.findAll('h1')[0])
	F = Soup.findAll('h2', id='step2')[0].findParent()
	URL = '/'.join(x['URL'].split('/')[:-1]) +'/' + str(dict(F.attrs)['action'])	
	postdata = '&'.join([str(dict(i.attrs)['name']) + '=' + urllib.quote(str(BeautifulSoup(dict(i.attrs)['value'], convertEntities=BeautifulStoneSoup.HTML_ENTITIES))) for i in F.findAll('input')]) + '&'
	S = F.findAll('select')[0]
	Recs = [(Category, SafeContents(o).split(' - ')[1].strip(), SafeContents(o).split(' - ')[0].strip(), URL, '--referer="' + x['URL']+ '" --post-data="' + postdata + str(dict(S.attrs)['name']) + '=' + urllib.quote(str(dict(o.attrs)['value'])) + '"') for o in S.findAll('option')]	
	return tb.tabarray(records = Recs, names = ['Category', 'Table', 'Table_Number', 'URL', 'opstring'], coloring = {'Categories': ['Category', 'Table'], 'Download': ['URL', 'opstring']})

def REA_LocalAreaPersonalIncome_Parse2(page,x):
	print page
	Soup = BeautifulSoup(open(page,'r'),convertEntities=BeautifulStoneSoup.HTML_ENTITIES)
	F = Soup.findAll('h2', id='step3')[0].findNext().findNext().findAll('form')[0]
	URL = '/'.join(x['URL'].split('/')[:-1]) +'/' + str(dict(F.attrs)['action'])	

	inputs = '&'.join([str(dict(i.attrs)['name']) + '=' + urllib.quote(str(BeautifulSoup(dict(i.attrs)['value'], convertEntities=BeautifulStoneSoup.HTML_ENTITIES))) for i in Soup.findAll('h2', id='step4')[0].findNext().findAll('input') if dict(i.attrs)['value'] != 'Display'])
	AllYears = '&'.join(['selYears=' + str(dict(o.attrs)['value']) for o in F.findAll('select', id='selYears')[0].findAll('option')])
	postdata = inputs + '&' + AllYears + '&'

	S = F.findAll('select', id='estimate')[0]
	Recs = [(SafeContents(o).split(' - ')[1].strip(), x['Table_Number'], SafeContents(o).split(' - ')[0].strip(), URL, '--referer="' + x['URL'].split('#')[0] + '" --post-data="' + postdata + str(dict(S.attrs)['name']) + '=' + urllib.quote(str(dict(o.attrs)['value'])) + '"', 'csv') for o in S.findAll('option')]
	X = tb.tabarray(records = Recs, names = ['Line', 'Table_Number', 'Line_Number', 'URL', 'opstring', 'Extension'], coloring = {'Categories': ['Line'], 'Download': ['URL', 'opstring']})
	
	[cols, hl] = OG.gethierarchy(X['Line'], hr2, postprocessor = lambda y : y.strip('\xc2\xa0').strip())		
	cols = [c for c in cols if not (c == '').all()]
	names = ['Level_' + str(i) for i in range(1, len(cols)+1)]
	return tb.tabarray(columns=[[y.strip('\xc2\xa0').strip() for y in X['Line']]], names=['Line'], coloring={'Categories': ['Line']}).colstack(X[['Table_Number', 'Line_Number']]).colstack(X[['URL', 'opstring', 'Extension']]).colstack(tb.tabarray(columns = cols, names = names, coloring={'Levels': names})) 

def splitfunc(x):
	return x['Table']
		
def REA_LocalAreaPersonalIncome(depends_on = root + 'REA/Manifest_1.tsv', creates = protocolroot + 'REA_LocalAreaPersonalIncome.py'):
	M = tb.tabarray(SVfile=depends_on)
	URL = M[(M['Category'] == 'Local Area Personal Income and Employment (including metropolitan areas)') & (M['DataType'] == 'estimates')]['URL'][0]
	L = [REA_LocalAreaPersonalIncome_Parse1, {'Parser': REA_LocalAreaPersonalIncome_Parse2, 'Splitter': (splitfunc, ['Personal income summary', 'Personal income and employment summary', 'Personal income by major source and earnings by NAICS industry', 'Total full-time and part-time employment by NAICS industry', 'Personal income by major source and earnings by SIC industry', 'Compensation of employees by SIC industry', 'Compensation of employees by NAICS industry', 'Total full-time and part-time employment by SIC industry', 'Regional economic profiles', 'Wage and salary summary', 'Personal current transfer receipts', 'Farm income and expenses']), 'Getter': WgetMultiple}, None]
	htools.hsuck(URL + 'default.cfm?selTable=Single%20Line', root + 'REA_LocalAreaPersonalIncome/', L, ipath=creates)
	
def REA_LocalAreaPersonalIncome_ParseDownloads(depends_on=(root+'REA_LocalAreaPersonalIncome/Downloads_2/', root+'REA_LocalAreaPersonalIncome/Manifest_2.tsv'), creates=root+'REA_LocalAreaPersonalIncome/ParsedFiles/'):
	MakeDir(creates)
	M = tb.tabarray(SVfile=depends_on[1])
	for x in M:
		(c, EstimateSubject, DataLine, tn, ln, URL, opstring, ex, prefix) = [x[n] for n in M.dtype.names if n not in M.coloring['Levels']]
		f = depends_on[0] + htools.pathprocessor([prefix]) + '/' + htools.pathprocessor([x[n] for n in M.dtype.names if n in M.coloring['Categories']]) + '.csv'
		savepath = creates + f.split('/')[-1].replace('!','_').replace('.csv','.tsv')
		print f
		[X, header, footer, keywords] = BEA_Parser(f, headerlines=1, FootnoteSplitter='/', FootnotesFunction=GetFootnotes, CategoryColumn=None)
		title = X.dtype.names[0]
		if len(title.split('/')) > 1:
			LineFootnote = title.split('/')[0].split()[-1]
			title = ' '.join(title.split('/')[0].split()[:-1]).strip()
		else:
			LineFootnote = None
		
		X.renamecol(X.dtype.names[0], 'Line')
	
		metadata = {}
		metadata['keywords'] = 'Regional Economic Accounts'
		metadata['title'] = EstimateSubject + ' (' + tn + '); ' + DataLine + ' (' + ln + ')'
		metadata['description'] = 'Local Area Personal Income data for all counties, for the single data line "' + DataLine + ' (' + ln + ')" under the estimate subject "' + EstimateSubject + ' (' + tn + ')", from the <a href="http://www.bea.gov/regional/reis/default.cfm?selTable=Single%20Line">Local Area Personal Income "Single Line of data for all counties"</a> data set under the <a href="http://www.bea.gov/regional/index.htm">Regional Economic Accounts</a> section of the <a href="http://www.bea.gov/">Bureau of Economic Accounts (BEA)</a> website.  For more information on the industry classifications, see the BEA web pages on  <a href="http://www.bea.gov/regional/definitions/nextpage.cfm?key=NAICS">NAICS (1997-2008)</a> and <a href="http://www.bea.gov/regional/definitions/nextpage.cfm?key=SIC">SIC (1963-1997)</a>.  With the exception of per capita estimates, all dollar estimates are in thousands of dollars. All employment estimates are number of jobs. All dollar estimates are in current dollars.'
		metadata['Agency'] = 'DOC'
		metadata['Subagency'] = 'BEA'
		metadata['Type'] = 'Regional'
		metadata['Category'] = 'Local Area Personal Income'
		metadata['Table'] = EstimateSubject + ' (' + tn + ')'
		metadata['Subject'] = DataLine + ' (' + ln + ')'
		levels = [int(n.split('_')[-1]) for n in M.coloring['Levels']]
		levels.sort()
		levels = ['Level_' + str(n) for n in levels]
		metadata['SubjectHierarchy'] = [x[n] for n in levels]
		metadata['Categories'] = ', '.join(['Agency', 'Subagency', 'Type', 'Category', 'Table', 'Subject'])
		metadata['URL'] = 'http://www.bea.gov/regional/reis/default.cfm?selTable=Single%20Line'
		if footer:
			metadata['footer'] = '\n'.join(footer)
			[source, info, category, owner, date] = footer
			(link, table) = info.replace('"','').split('HYPERLINK(')[1].split(')')[0].split(',')
			metadata['TableFootnotes'] = '<a href="' + link.strip() + '">' + metadata['Table'] + ' Footnotes</a>'
			if LineFootnote:
				metadata['SubjectFootnote'] = 'See footnote' + LineFootnote + ' under ' + metadata['TableFootnotes']
			metadata['Source'] = source.split('Source:')[1].strip()
			metadata['LastRevised'] = date.strip("'")
			
		Years = [int(y[:4]) for y in X.coloring['Data'] if y[:4].isdigit()]
		N = X.coloring['Data'][0]
		if '-' in N:
			assert N.replace('-','').isdigit()
			assert int(N.split('-')[1]) - int(N.split('-')[0]) == 1		
			metadata['TimePeriod'] = str(min(Years)) + '-' + str(max(Years)+1)
			metadata['DateDivisions'] = 'Yearly Differences'
		else:
			assert len(N) == 4
			metadata['TimePeriod'] = str(min(Years)) + '-' + str(max(Years))
			metadata['DateDivisions'] = 'Years'
		
		coldescrs = {}
		for n in X.dtype.names:
			if '/' in n:
				(newname, footnote) = GetFootnotes(n)
				X.renamecol(n, newname)
				coldescrs[newname] = 'See footnote ' + footnote + ' under ' + metadata['TableFootnotes']
		if coldescrs:
			metadata['coldescrs'] = coldescrs
	
		X.metadata = metadata
		X.metadata['labelcollist'] = ['AreaName']
		X.saveSV(savepath, metadata=True, comments='#', delimiter='\t')



			
	
	
