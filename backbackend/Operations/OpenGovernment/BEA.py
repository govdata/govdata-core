import os
import numpy as np
import tabular as tb
from BeautifulSoup import BeautifulSoup, BeautifulStoneSoup, NavigableString
from mechanize import Browser
import urllib
import re
import pymongo as pm
from System.Utils import MakeDir, Contents, listdir, IsDir, wget, uniqify, PathExists,RecursiveFileList, ListUnion, MakeDirs,delete,strongcopy
from System.Protocols import activate,ApplyOperations2
import Operations.htools as htools
import Operations.OpenGovernment.OpenGovernment as OG
import cPickle as pickle

NIPA_NAME = 'BEA_NIPA'

#=-=-=-=-=-=-=-=-=-Utilities
def SafeContents(x):
    return ' '.join(Contents(x).strip().split())
    
def WgetMultiple(link,fname,opstring='',  maxtries=5):
    for i in range(maxtries):
        wget(link, fname, opstring)
        F = open(fname,'r').read().strip()
        if not (F.lower().startswith('<!doctype') or F == '' or 'servlet error' in F.lower()):
            return
        else:
            print 'download of ' + link + ' failed'
    print 'download of ' + link + ' failed after ' + str(maxtries) + ' attempts'
    return
    
def hr(x):
    return  len(x) - len(x.lstrip(' '))
    
def hr2(x):
    return  len(x.split('\xc2\xa0')) - 1

    
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
        
        
def NEA_Parser(page, headerlines=None, FootnoteSplitter = '/', FootnotesFunction = GetFootnotes, CategoryColumn=None,FormulaColumn=None):
    
    [Y, header, footer, keywords]  = BEA_Parser(page, headerlines=headerlines, FootnoteSplitter = FootnoteSplitter, FootnotesFunction = FootnotesFunction, CategoryColumn=CategoryColumn, NEA=True, FormulaColumn=FormulaColumn)
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
        [F, meta] = tb.io.loadSVrecs(page, headerlines=i+1,delimiter = ',')

    else:
        [F, meta] = tb.io.loadSVrecs(page, headerlines=headerlines)
        header = None

    names = [n.strip() for n in meta['names']]

    if not NEA:
        names = names[:len(F[0])]
        F = [f[:len(names)] for f in F]

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

        
def NEA_preparser2(inpath,filepath,metadatapath,L = None):

    MakeDir(filepath)

    if L == None:
        L = [inpath + x for x in listdir(inpath) if x.endswith('.tsv')]
    T = [x.split('/')[-1].split('_')[0].strip('.') for x in L]
    R = tb.tabarray(columns=[L,T],names = ['Path','Table']).aggregate(On=['Table'],AggFunc=lambda x : '|'.join(x))
    
    ColGroups = {}
    Metadict = {}
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
        Z.saveSV(filepath + str(j) + '.tsv',metadata=['dialect','formats','names'])
    
    AllKeys = uniqify(ListUnion([k.keys() for k in Metadict.values()]))
    AllMeta = {}
    for k in AllKeys:
        if all([k in Metadict[l].keys() for l in Metadict.keys()]) and len(uniqify([Metadict[l][k] for l in Metadict.keys()])) == 1:
            AllMeta[k] = Metadict[Metadict.keys()[0]][k]
            for l in Metadict.keys():
                Metadict[l].pop(k)
    
    Category = AllMeta.pop('Category')

    AllMeta['Source'] = [('Agency',{'Name':'Department of Commerce','ShortName':'DOC'}),('Subagency',{'Name':'Bureau of Economic Analysis','ShortName':'BEA'}),('Program','National Economic Accounts'), ('Dataset',Category)]
    AllMeta['TopicHierarchy'] = ('Agency','Subagency','Program','Dataset','Section','Table')
    AllMeta['UniqueIndexes'] = ['TableNo','Line']
    AllMeta['ColumnGroups'] = ColGroups
    AllMeta['DateFormat'] = 'YYYYqmm'
    AllMeta['sliceCols'] = ['Section','Table','Topics']
    AllMeta['phraseCols'] = ['Section','Table','Topics','Line','TableNo']

    
    Subcollections = Metadict
    Subcollections[''] = AllMeta
        
    F = open(metadatapath,'w')
    pickle.dump(Subcollections,F)
    F.close()


#=-=-=-=-=-=-=-=-=-=-=-=-=-=FAT
FAT_NAME = 'BEA_FAT'
@activate(lambda x : 'http://www.bea.gov/national/FA2004/SelectTable.asp',lambda x : x[0])
def FAT_downloader(maindir):

    MakeDirs(maindir)
   
    get_FAT_manifest(maindir)
    
    connection = pm.Connection()
    
    incremental = FAT_NAME in connection['govdata'].collection_names()
    
    MakeDir(maindir + 'raw/')
    
    URLBase = 'http://www.bea.gov/national/FA2004/csv/NIPATable.csv?'
    
   
    X = tb.tabarray(SVfile = maindir + 'manifest.tsv')
    for x in X:
        
        NC = x['NumCode']
        Freq = x['Freq']
        if incremental:
            Vars = ['TableName','FirstYear','LastYear','Freq']
            FY = x['FirstYear'] 
            LY = x['LastYear']
            url = URLBase + '&'.join([v + '=' + str(val) for (v,val) in zip(Vars,[NC,FY,LY,Freq])])
        else:
            Vars = ['TableName','AllYearChk','FirstYear','LastYear','Freq']
            FY = 1800
            LY = 2200
            url = URLBase + '&'.join([v + '=' + str(val) for (v,val) in zip(Vars,[NC,'YES',FY,LY,Freq])])
     
        
        topath = maindir + 'raw/' + x['Section'] + '_' + x['Table']  + '.csv'
        
        WgetMultiple(url,topath)
        
                
        
def get_FAT_manifest(download_dir,depends_on = 'http://www.bea.gov/national/FA2004/SelectTable.asp'):
    
    wget(depends_on,download_dir + 'manifest.html')
    nc = re.compile('SelectedTable=[\d]+')
    fy = re.compile('FirstYear=[\d]+')
    ly = re.compile('LastYear=[\d]+')
    fr = re.compile('Freq=[a-zA-Z]+')
    
    L = lambda reg , x : int(reg.search(str(dict(x.findAll('a')[0].attrs)['href'])).group().split('=')[-1])
    L2 = lambda reg , x : reg.search(str(dict(x.findAll('a')[0].attrs)['href'])).group().split('=')[-1]
    
    path = download_dir + 'manifest.html'
    Soup = BeautifulSoup(open(path),convertEntities=BeautifulStoneSoup.HTML_ENTITIES)
    c1 = lambda x : x.name == 'a' and 'name' in dict(x.attrs).keys() and dict(x.attrs)['name'].startswith('S')
    c2 = lambda x : x.name == 'tr' and 'class' in dict(x.attrs).keys() and dict(x.attrs)['class'] == 'TR' and x.findAll('a') and 'href' in dict(x.findAll('a')[0].attrs).keys() and  dict(x.findAll('a')[0].attrs)['href'].startswith('Table')
    
    p1 = lambda x : Contents(x).strip().strip('\xc2\xa0').strip()
    p2 = lambda x : (p1(x),'http://www.bea.gov/national/FA2004/' + str(dict(x.findAll('a')[0].attrs)['href']),L(nc,x),L(fy,x),L(ly,x),L2(fr,x))
    
    X = htools.MakeTable(Soup,[c1,c2],[p1,p2],['Section',['Table','URL','NumCode','FirstYear','LastYear','Freq']])
    secnums = [x['Section'].split(' ')[1].strip() for x in X]
    secnames = [x['Section'].split('-')[1].strip() for x in X]
    tablenums = [x['Table'].split(' ')[1].split('.')[-2].strip() for x in X]
    tablenames = [' '.join(x['Table'].split(' ')[2:]).strip() for x in X]
    X = X.addcols([secnums,secnames,tablenums,tablenames],names=['Section','SectionName','Table','TableName'])
    X.saveSV(download_dir + 'manifest.tsv',metadata=True)

@activate(lambda x : (x[0] + 'raw/',x[0] + 'manifest.tsv'), lambda x : x[0] + 'preparsed/')
def FAT_preparser1(maindir):    

    targetdir = maindir + 'preparsed/'
    sourcedir = maindir + 'raw/'
    
    MakeDir(targetdir) 
    M = tb.tabarray(SVfile = maindir + 'manifest.tsv')
    for x in M:
        f = sourcedir + x['Section'].strip('.') + '_' + x['Table'] + '.csv'
        print f 
        savepath = targetdir + x['Section'].strip('.') + '_' + x['Table'] + '.tsv'
    
        [X, header, footer, keywords] = NEA_Parser(f, FootnoteSplitter='\\', FootnotesFunction=GetFootnotesLazy, CategoryColumn='Category',FormulaColumn='Category')        
        
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

        metadata['keywords'] = 'National Economic Accounts,' + ','.join(keywords)       
            
        X.metadata.update(metadata)
        X.saveSV(savepath, metadata=True, comments='#', delimiter='\t')
        
        
@activate(lambda x: (x[0] + 'preparsed/',x[0]+'manifest.tsv'),lambda x : (x[0] + '__PARSE__/',x[0] + '__metadata.pickle'))
def FAT_preparser2(maindir): 
    sourcedir = maindir + 'preparsed/'
    filedir = maindir + '__PARSE__/'
    metadatapath = maindir + '__metadata.pickle'

    MakeDir(filedir)
   
    GoodKeys = ['Category', 'Section', 'Units', 'Table', 'Footer']
    
    Metadict = {}
    ColGroups = {}
    
    M = tb.tabarray(SVfile = maindir + 'manifest.tsv')
    for (i,x) in enumerate(M):
        l = sourcedir + x['Section'].strip('.') + '_' + x['Table'] + '.tsv'
        print l
        t = x['Section'] + '.' + x['Table']
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
        X.saveSV(filedir + str(i) + '.tsv',metadata=['dialect','names','formats'])
    
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
    AllMeta['UniqueIndexes'] = ['TableNo','Line']
    AllMeta['ColumnGroups'] = ColGroups
    AllMeta['DateFormat'] = 'YYYYqmm'
    AllMeta['sliceCols'] = ['Section','Table','Topics']
    AllMeta['phraseCols'] = ['Section','Table','Topics','Line','TableNo']
    
    Subcollections = Metadict
    Subcollections[''] = AllMeta

    F = open(metadatapath ,'w')
    pickle.dump(Subcollections,F)
    F.close()
        

def fat_trigger():
    connection = pm.Connection()
    
    incremental = FAT_NAME in connection['govdata'].collection_names()
    if incremental:
        return 'increment'
    else:
        return 'overall'
        
#=-=-=-=-=-=-=-=-=-=-=-=-=-=NIPA
@activate(lambda x : 'http://www.bea.gov/national/nipaweb/csv/NIPATable.csv',lambda x : x[0])
def NIPA_downloader(maindir):

    MakeDirs(maindir)
   
    get_manifest(maindir)
    
    connection = pm.Connection()
    
    incremental = NIPA_NAME in connection['govdata'].collection_names()
    
    MakeDir(maindir + 'raw/')
    
    URLBase = 'http://www.bea.gov/national/nipaweb/csv/NIPATable.csv?'
    
    Vars = ['TableName','FirstYear','LastYear','Freq']
    X = tb.tabarray(SVfile = maindir + 'manifest.tsv')
    for x in X:
        
        NC = x['NumCode']
        Freq = x['Freq']
        if incremental:
            FY = x['FirstYear'] 
            LY = x['LastYear']
        else:
            FY = 1800
            LY = 2200
            ystr = ''
        
        url = URLBase + '&'.join([v + '=' + str(val) for (v,val) in zip(Vars,[NC,FY,LY,Freq])])
        
        topath = maindir + 'raw/' + x['Number'].strip('.') + '_' + Freq + '.csv'
        
        WgetMultiple(url,topath)

    

@activate(lambda x : (x[0] + 'raw/',x[0] + 'manifest.tsv'), lambda x : x[0] + 'preparsed/')
def NIPA_preparser1(maindir):
    targetdir = maindir + 'preparsed/'
    sourcedir = maindir + 'raw/'
    
    MakeDir(targetdir) 
    M = tb.tabarray(SVfile = maindir + 'manifest.tsv')
    for x in M:
        f = sourcedir + x['Number'].strip('.') + '_' + x['Freq'] + '.csv'
        print f 
        savepath = targetdir + x['Number'].strip('.') + '_' + x['Freq'] + '.tsv'
    
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

        metadata['keywords'] = ['National Economic Accounts'] + keywords
            
        X.metadata.update(metadata)
        X.saveSV(savepath, metadata=True, comments='#', delimiter='\t')
    
    
@activate(lambda x: x[0] + 'preparsed/',lambda x : (x[0] + '__PARSE__/',x[0] + '__metadata.pickle'))
def NIPA_preparser2(maindir): 
    inpath = maindir + 'preparsed/'
    filedir = maindir + '__PARSE__/'
    metapath = maindir + '__metadata.pickle'
    NEA_preparser2(inpath,filedir,metapath)
    
@activate(lambda x : 'http://www.bea.gov/national/nipaweb/Index.asp',lambda x : (x[0] + 'additional_info.html',x[0] + 'additional_info.csv',x[0] + '__FILES__/'))   
def get_additional_info(download_dir):
    wget('http://www.bea.gov/national/nipaweb/Index.asp',download_dir + 'additional_info.html')
    page = download_dir + 'additional_info.html'
    Soup = BeautifulSoup(open(page,'r'),convertEntities=BeautifulStoneSoup.HTML_ENTITIES)
    PA = [(SafeContents(p), str(dict(p.findAll('a')[0].attrs)['href'])) for p in Soup.findAll('blockquote')[0].findAll('p')]
    Recs = [(p, 'http://www.bea.gov' + a) for (p,a) in PA]
    X = tb.tabarray(records = Recs, names = ['Name', 'URL'])
    X.saveSV(download_dir + 'additional_info.csv',metadata=True)
    MakeDir(download_dir + '__FILES__')
    for x in X:
        name = download_dir + '__FILES__/' + x['Name'].replace(' ','_')
        wget(x['URL'],name)
    

def get_manifest(download_dir,depends_on = 'http://www.bea.gov/national/nipaweb/SelectTable.asp?'):
    
    wget(depends_on,download_dir + 'manifest.html')
    nc = re.compile('SelectedTable=[\d]+')
    fy = re.compile('FirstYear=[\d]+')
    ly = re.compile('LastYear=[\d]+')
    page = download_dir + 'manifest.html'
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
                NumCode = int(nc.search(URL).group().split('=')[-1])
                FirstYear = int(fy.search(URL).group().split('=')[-1])
                LastYear = int(ly.search(URL).group().split('=')[-1])
                Recs += [(Section, Section_Number, Subsection_Number, Table_Number, Number, Freq, Name, URL,NumCode,FirstYear,LastYear)]
        M =  tb.tabarray(records = Recs, names = ['Section', 'Section_Number', 'Subsection_Number', 'Table_Number', 'Number', 'Freq', 'Name', 'URL','NumCode','FirstYear','LastYear'])
        
        M.saveSV(download_dir + 'manifest.tsv',metadata=True)       
        
def trigger():
    connection = pm.Connection()
    
    incremental = NIPA_NAME in connection['govdata'].collection_names()
    if incremental:
        return 'increment'
    else:
        return 'overall'
        
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

@activate(lambda x : (x[0] + 'State_Preparsed/',x[0] + 'State_Manifest_1.tsv',x[0] + 'Metro_Preparsed.tsv'),lambda x : (x[0] + '__PARSE__/',x[0] + '__metadata.pickle'))
def RegionalGDP_Preparse2(maindir):

    inpath = maindir + 'State_Preparsed/'
    outpath = maindir + '__PARSE__/'
    MakeDir(outpath)
    
    
    R = tb.tabarray(SVfile = maindir + 'State_Manifest_1.tsv')[['Region','IC','File']].aggregate(On=['Region','IC'],AggFunc = lambda x : '|'.join(x))
    
    GoodKeys = ['Category', 'description','footer', 'LastRevised']  

    Metadict = {}
    LenR = len(R)
    ColGroups = {}
    for (i,r) in enumerate(R):
        state = r['Region']
        indclass = r['IC']
        ps = r['File'].split('|')
        print state,indclass
        
        X = [tb.tabarray(SVfile = inpath + p[:-4] + '.tsv') for p in ps]
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
        Z.renamecol('State','Location')
        Z = Z.addcols(['{"s":' + repr(z['Location']) + ',"f":{"s":' + repr(z['FIPS']) + '}}' for z in Z],names = ['Location'])
        Z = Z.deletecols(['FIPS'])
        
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

        Z.coloring['LabelColumns'] =  ['Location','Industry','Component']       
        for k in Z.coloring.keys():
            if k in ColGroups.keys():
                ColGroups[k] = uniqify(ColGroups[k] + Z.coloring[k])
            else:
                ColGroups[k] = Z.coloring[k]        
        
        Metadict[state] = Z.metadata
        
        Z = Z.addcols([len(Z)*[indclass], len(Z)*['S']],names=['IndClass','Subcollections'])
        Z.saveSV(outpath + str(i) + '.tsv',metadata=['dialect','names','formats'])
    
    AllKeys = uniqify(ListUnion([k.keys() for k in Metadict.values()]))
    AllMeta = {}
    for k in AllKeys:
        if all([k in Metadict[l].keys() for l in Metadict.keys()]) and len(uniqify([Metadict[l][k] for l in Metadict.keys()])) == 1:
            AllMeta[k] = Metadict[Metadict.keys()[0]][k]
    
    Subcollections = {'S':AllMeta}
    Subcollections['S']['Title'] = 'GDP by State'
    
    del(Z)
        
    L = ['Metro_Preparsed.tsv']

    Metadict = {}
    for (i,l) in enumerate(L):
        print l
        X = tb.tabarray(SVfile = maindir + l)
        X.renamecol('industry_id','IndustryCode')
        X.renamecol('component_id','ComponentCode')
        X.renamecol('area_name','Metropolitan Area')
        X.renamecol('ParsedComponent','Component')
        X.renamecol('industry_name','Industry')
        
        X1 = X.deletecols('component_name')
        X1 = X1.addcols(['{"m":' + repr(x['Metropolitan Area']) + ',"f":{"m":' + repr(x['FIPS']) + '}}' for x in X],names=['Location'])
        X1 = X1.deletecols(['FIPS','Metropolitan Area'])
        X1.metadata = X.metadata
        X = X1  
        
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

        X.coloring['LabelColumns'] = ['Location','Industry','Component']    
        for k in X.coloring.keys():
            if k in ColGroups.keys():
                ColGroups[k] = uniqify(ColGroups[k] + X.coloring[k])
            else:
                ColGroups[k] = X.coloring[k]                
        
        Metadict[l] = X.metadata
        X = X.addcols([['NAICS']*len(X),['M']*len(X)],names=['IndClass','Subcollections'])
        X.saveSV(outpath + str(i+LenR) + '.tsv',metadata=['dialect','names','formats'])

    
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
    AllMeta['UniqueIndexes'] = ['Location','IndustryCode','ComponentCode','IndClass']
    ColGroups['SpaceColumns'] = ['Location']
    AllMeta['ColumnGroups'] = ColGroups
    AllMeta['DateFormat'] = 'YYYYqmm'
    
    AllMeta['sliceCols'] = [['Location.s', 'Location.m', 'IndustryHierarchy'] ,['Location.s', 'Location.m','Component'],['IndustryHierarchy','Component']]
    AllMeta['phraseCols'] = ['Component', 'IndClass', 'IndustryHierarchy','Industry','Units','Units']    

    Subcollections[''] = AllMeta
        
    F = open(maindir+'__metadata.pickle','w')
    pickle.dump(Subcollections,F)
    F.close()   


@activate(lambda x : x[0] + 'Metro_Raw/allgmp.csv',lambda x : x[0] + 'Metro_Preparsed.tsv')
def Metro_PreParse1(maindir):
    f = maindir + 'Metro_Raw/allgmp.csv'
    savepath = maindir + 'Metro_Preparsed.tsv'
    [X, header, footer, keywords] = BEA_Parser(f, headerlines=1, CategoryColumn='industry_name')

    X = X[(X['component_name'] != '') & (X['component_name'] != 'component_name')]
    p = re.compile('\(.*\)')
    ParsedComp = [p.sub('',x).strip() for x in X['component_name']]
    Units = [x[p.search(x).start()+1:p.search(x).end()-1] for x in X['component_name']]
    X = X.addcols([ParsedComp,Units],names=['ParsedComponent','Units'])


    metadata = {}
    metadata['keywords'] = ['Regional Economic Accounts']
    metadata['title'] = 'GDP by Metropolitan Area'
    metadata['description'] = 'Gross domestic product (GDP) for individual metropolitan statistical areas -- from the <a href="http://www.bea.gov/regional/gdpmetro/">GDP by Metropolitan Areas</a> data set under the <a href="http://www.bea.gov/regional/index.htm">Regional Economic Accounts</a> section of the <a href="http://www.bea.gov/">Bureau of Economic Accounts (BEA)</a> website.  Note that NAICS industry detail is based on the 1997 NAICS.  For more information on Metropolitan Statistical Areas, see the BEA website on <a href="http://www.bea.gov/regional/docs/msalist.cfm?mlist=45">Statistical Areas</a>.  Component units are as follows:  GDP by Metropolitan Area (millions of current dollars), Quantity Indexes for Real GDP by Metropolitan Area (2001=100.000), Real GDP by Metropolitan Area (millions of chained 2001 dollars), Per capita real GDP by Metropolitan Area (chained 2001 dollars).'
    metadata['Agency'] = 'DOC'
    metadata['Subagency'] = 'BEA'
    metadata['Type'] = 'Regional'
    metadata['Category'] = 'GDP by Metropolitan Area'
    metadata['Categories'] = ', '.join(['Agency', 'Subagency', 'Type', 'Category', 'Region'])
    if footer:
        footer = [ff for ff in footer if ff.strip('\x1a')]
        metadata['footer'] = '\n'.join(footer)
        metadata['Notes'] = '\n'.join([x.split('Note:')[1].strip() for x in footer[:-1]])
        metadata['Source'] = footer[-1].split('Source:')[1].strip()
        metadata['LastRevised'] = metadata['Source'].split('--')[-1].strip()
    
    X.metadata = metadata
    X.metadata['unitcol'] = ['Units']
    X.metadata['labelcollist'] = ['industry_name','ParsedComponent']
    X.saveSV(savepath, metadata=True, comments='#', delimiter='\t')
        

@activate(lambda x : ( x[0] + 'State_Manifest_1.tsv', x[0] + 'State_Raw/'),lambda x : x[0] + 'State_Preparsed/')
def State_PreParse1(maindir):
    target = maindir + 'State_Preparsed/'
    sourcedir = maindir + 'State_Raw/'
    manifest = maindir + 'State_Manifest_1.tsv'
    
    MakeDir(target)
    M = tb.tabarray(SVfile=manifest)
    for mm in M:
        FIPS,Region,IC,file = mm['FIPS'],mm['Region'],mm['IC'],mm['File']

  
        f = sourcedir + file
        savepath = target + file[:-4] + '.tsv'
        [X, header, footer, keywords] = BEA_Parser(f, headerlines=1, FootnoteSplitter='\\', FootnotesFunction=GetFootnotes2, CategoryColumn='Industry')
    
        p = re.compile('\(.*\)')
        ParsedComp = [p.sub('',x).strip() for x in X['Component']]
        Units = [x[p.search(x).start()+1:p.search(x).end()-1] for x in X['Component']]
        X = X.addcols([ParsedComp,Units],names=['ParsedComponent','Units'])
            
        metadata = {}

        Years = [int(y[:4]) for y in X.coloring['Data'] if y[:4].isdigit()]
        
        metadata['keywords'] = ['Regional Economic Accounts']
        metadata['description'] = 'Gross domestic product (GDP) for a single state or larger region, ' + Region + ' using the ' + IC + ' industry classification.  This data comes from the <a href="http://www.bea.gov/regional/gsp/">GDP by State</a> data set under the <a href="http://www.bea.gov/regional/index.htm">Regional Economic Accounts</a> section of the <a href="http://www.bea.gov/">Bureau of Economic Accounts (BEA)</a> website.  For more information on the industry classifications, see the BEA web pages on  <a href="http://www.bea.gov/regional/definitions/nextpage.cfm?key=NAICS">NAICS (1997-2008)</a> and <a href="http://www.bea.gov/regional/definitions/nextpage.cfm?key=SIC">SIC (1963-1997)</a>.  Component units are as follows:  Gross Domestic Product by State (millions of current dollars), Compensation of Employees (millions of current dollars), Taxes on Production and Imports less Subsidies (millions of current dollars), Gross Operating Surplus (millions of current dollars), Real GDP by state (millions of chained 2000 dollars), Quantity Indexes for Real GDP by State (2000=100.000), Subsidies (millions of current dollars), Taxes on Production and Imports (millions of current dollars), Per capita real GDP by state (chained 2000 dollars).'
        metadata['Agency'] = 'DOC'
        metadata['Subagency'] = 'BEA'
        metadata['Type'] = 'Regional'
        metadata['Category'] = 'GDP by State'
        metadata['IndustryClassification'] = IC
        metadata['Region'] = Region
        metadata['FIPS'] = X['FIPS'][0]
        metadata['Categories'] = ', '.join(['Agency', 'Subagency', 'Type', 'Category', 'IndustryClassification', 'Region', 'TimePeriod'])
        if footer:
            footer = CleanLinesForMetadata(footer)
            metadata['footer'] = '\n'.join(footer)
            [source] = footer
            metadata['Source'] = source.split('Source:')[1].strip()
        
        X.metadata = metadata
        X.metadata['unitcol'] = ['Units']
        X.metadata['labelcollist'] = ['Industry','ParsedComponent']
        X.saveSV(savepath, metadata=True, comments='#', delimiter='\t')


def GetStateManifest(maindir):
    wget('http://www.bea.gov/regional/gsp/',maindir + 'State_Index.html')
    page = maindir + 'State_Index.html'
    Soup = BeautifulSoup(open(page),convertEntities=BeautifulStoneSoup.HTML_ENTITIES)
    O = Soup.findAll('select',{'name':'selFips'})[0].findAll('option')
    L = [(str(dict(o.attrs)['value']),Contents(o).strip()) for o in O]
    tb.tabarray(records = L,names = ['FIPS','Region']).saveSV(maindir + 'State_Manifest.tsv',metadata=True)
    
    
@activate(lambda x : 'http://www.bea.gov/national/nipaweb/csv/NIPATable.csv',lambda x : x[0])
def RegionalGDP_initialize(maindir):
    MakeDirs(maindir)
    GetStateManifest(maindir)
    
@activate(lambda x : x[0] + 'State_Manifest.tsv',lambda x : (x[0] + 'State_Raw/',x[0] + 'State_Manifest_1.tsv'))
def DownloadStateFiles(maindir):
    connection = pm.Connection()
    incremental = REG_NAME in connection['govdata'].collection_names()
    
    X = tb.tabarray(SVfile = maindir + 'State_Manifest.tsv')
    rawdir = maindir + 'State_Raw/'
    MakeDir(rawdir)
    Recs = []
    for x in X:
        fips = x['FIPS']
        if fips != 'ALL':
            Region = x['Region']
            Recs.append((fips,Region,'NAICS','NAICS_' + fips+ '.csv'))

            if incremental:
                selYears = 'selYears=2008'   #this could be improved
            else:
                selYears = 'selYears=ALL'
            Postdata= 'series=NAICS&selTable=ALL&selFips=' + str(fips) + '&selLineCode=ALL&' + selYears + '&querybutton=Download+CSV'
            WgetMultiple('http://www.bea.gov/regional/gsp/action.cfm', rawdir + 'NAICS_' + fips + '.csv', opstring = '--post-data="' + Postdata + '"')
            
            if not incremental:
                Recs.append((fips,Region,'SIC','SIC_1_' + fips+ '.csv'))
                Recs.append((fips,Region,'SIC','SIC_2_' + fips+ '.csv'))            
                selYears = '&'.join(['selYears=' + str(y) for y in range(1963,1983)])
                Postdata= 'series=SIC&selTable=ALL&selFips=' + str(fips) + '&selLineCode=ALL&' + selYears + '&querybutton=Download+CSV'
                WgetMultiple('http://www.bea.gov/regional/gsp/action.cfm', rawdir + 'SIC_1_' + fips + '.csv', opstring = '--post-data="' + Postdata + '"')
                selYears = '&'.join(['selYears=' + str(y) for y in range(1983,1998)])
                Postdata= 'series=SIC&selTable=ALL&selFips=' + str(fips) + '&selLineCode=ALL&' + selYears + '&querybutton=Download+CSV'
                WgetMultiple('http://www.bea.gov/regional/gsp/action.cfm', rawdir + 'SIC_2_' + fips + '.csv', opstring = '--post-data="' + Postdata + '"')
        
    tb.tabarray(records = Recs,names = ['FIPS','Region','IC','File']).saveSV(maindir + 'State_Manifest_1.tsv',metadata=True)    

@activate(lambda x : x[0] + 'GDPMetro.zip',lambda x : x[0] + 'Metro_Raw/')
def DownloadMetroFiles(maindir):
    wget('http://www.bea.gov/regional/zip/GDPMetro.zip',maindir + 'GDPMetro.zip')
    os.system('unzip -d ' + maindir + 'Metro_Raw ' + maindir + 'GDPMetro.zip')

REG_NAME = 'BEA_RegionalGDP'

     
def reg_trigger():
    connection = pm.Connection()
    
    incremental = REG_NAME in connection['govdata'].collection_names()
    if incremental:
        return 'increment'
    else:
        return 'overall'
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=


PI_NAME = 'BEA_PersonalIncome'

def PI_dateparse(x):
	d = x.split('.')
	if len(d) == 1:
		return d[0] + 'X' + 'XX' 
	else:
		return d[0] + str(int(d[1]))  + 'XX'

@activate(lambda x : 'http://www.bea.gov/regional/sqpi/action.cfm?zipfile=/regional/zip/sqpi.zip',lambda x : (x[0] + 'sqpi.zip', x[0] + 'sqpi_raw/'))
def SQPI_downloader(maindir):

    wget('http://www.bea.gov/regional/sqpi/action.cfm?zipfile=/regional/zip/sqpi.zip',maindir + 'sqpi.zip')
    os.system('unzip -d ' + maindir + 'sqpi_raw/ ' + maindir + 'sqpi.zip')
            
    t = 'SQ1'
    selYears = range(1969,2020)
        
    #postdata = '--post-data="selLineCode=10&rformat=Download&selTable=' + t + '&selYears=' + ','.join(map(str,selYears)) + '"'
    #wget('http://www.bea.gov/regional/sqpi/drill.cfm',maindir + 'sqpi_raw/' + t + '.csv',opstring = postdata)
        
            
@activate(lambda x : 'http://www.bea.gov/regional/spi/action.cfm',lambda x : x[0] + 'sapi_raw/')
def SAPI_downloader(maindir):
    target = maindir + 'sapi_raw/'
    MakeDirs(target)
    src = 'http://www.bea.gov/regional/spi/action.cfm'
    for x in ['sa','sa_sum','sa_naics','sa_sic']:
        postdata = '--post-data="archive=' + x + '&DownloadZIP=Download+ZIP"'           
        wget(src,target + x + '.zip', opstring=postdata)
        os.system('unzip -d ' + target + x + ' ' + target + x + '.zip')

@activate(lambda x : 'http://www.bea.gov/regional/reis/default.cfm?selTable=Single%20Line', lambda x : (x[0] + 'lapi_codes/',x[0] + 'lapi_codes.tsv'))
def get_line_codes(maindir):
    target = maindir + 'lapi_codes/'
    MakeDirs(target)
    wget('http://www.bea.gov/regional/reis/default.cfm?selTable=Single%20Line',target + '/index.html')
    Soup = BeautifulSoup(open(target + 'index.html'))
    O = Soup.findAll('select',id='selTable')[0].findAll('option')
    O1 = [(str(dict(o.attrs)['value']),Contents(o).split(' - ')[0].strip(),Contents(o).split(' - ')[1].strip()) for o in O]
    g = 'http://www.bea.gov/regional/reis/default.cfm#step2'
    Recs = []
    for (op,n,m) in O1:
        s = '--post-data="singletable=' + op + '&nextarea=Next+%E2%86%92&section=next&selTable=Single+Line&areatype=ALLCOUNTY&catable_name=' + op + '"'
        wget(g,target + op + '.html',opstring=s)
        Soup = BeautifulSoup(open(target + op + '.html'))
        O = Soup.findAll('select',{'name':'selLineCode'})[0].findAll('option')
        Recs += [(op,m,str(dict(o.attrs)['value']),Contents(o).split(' - ')[1].strip()) for o in O]
    tb.tabarray(records = Recs,names = ['Table','TableDescr','Code','CodeDescr']).saveSV(maindir + 'lapi_codes.tsv',metadata=True)
    
@activate(lambda x : x[0] + 'lapi_codes.tsv',lambda x : x[0] + 'lapi_codes_processed.tsv')
def process_line_codes(maindir):
    inpath = maindir + 'lapi_codes.tsv'
    outpath = maindir + 'lapi_codes_processed.tsv'
    
    X = tb.tabarray(SVfile = inpath)
    Vals = []
    v = ()
    pi = 0
    for x in X:
        ci = x['CodeDescr'].count('&nbsp;') / 2
        t = x['CodeDescr'].split('&nbsp;')[-1]
        if ci <= pi:
            v = v[:ci] + (t,)
        elif ci > pi:
            v = v + (t,)
        Vals.append(v)
        pi = ci
   
    m = max(map(len,Vals))
    
    Vals = [v + (m - len(v))*('',) for v in Vals]
    NewX = tb.tabarray(records = Vals,names = ['Level_' + str(i) for i in range(m)])
    X = X.colstack(NewX)
    X.coloring['Hierarchy'] = list(NewX.dtype.names)
    X.saveSV(outpath,metadata = True)
    
@activate(lambda x : (x[0] + 'lapi_codes.tsv','http://www.bea.gov/regional/reis/drill.cfm'),lambda x : x[0] + 'lapi_raw/' + x[1] + '_' + x[2] + '/')
def LAPI_downloader(maindir,table,level):
    target = maindir + 'lapi_raw/' + table + '_' + level + '/'
    MakeDirs(target)
    X = tb.tabarray(SVfile = maindir + 'lapi_codes.tsv')
    X = X[X['Table'] == table]
    connection = pm.Connection()
    incremental = PI_NAME in connection['govdata'].collection_names()
    if incremental:
        selYears = range(2008,2012)
    else:
        selYears = range(1969,2012)
    for x in X['Code']:
        s = '--post-data="areatype=' + level + '&SelLineCode=' + x + '&rformat=Download&selTable=Single+Line&catable_name=' + table + '&' + '&'.join(map(lambda y : 'selYears='+str(y),selYears)) + '"'
        print 'Getting:', s
        wget('http://www.bea.gov/regional/reis/drill.cfm',target + x + '.csv',opstring = s)

@activate(lambda x : 'http://www.bea.gov/regional/docs/footnotes.cfm', lambda x : (x[0] + 'footnotes/',x[0] + 'footnotes.tsv'))
def get_footnotes(maindir):
    target = maindir + 'footnotes/'
    MakeDirs(target)
    index = target + 'index.html'
    wget('http://www.bea.gov/regional/docs/footnotes.cfm',index)
    Soup = BeautifulSoup(open(index))
    A = Soup.findAll(lambda x : x.name == 'a' and  'footnotes.cfm' in str(x))
    Tables = [Contents(a).strip() for a in A]
    Recs = []
    for t in Tables:
        wget('http://www.bea.gov/regional/docs/footnotes.cfm?tablename=' + t, target + t + '.html')
        Soup = BeautifulSoup(open(target + t + '.html'))
        caption = Contents(Soup.findAll('caption')[0]).split(' - ')[-1].strip()
        TR = Soup.findAll('caption')[0].findParent().findAll('tr')
        Recs += [(t,caption,Contents(tr.findAll('strong')[0]).replace('\t',' '), Contents(tr.findAll('td')[-1]).replace('\t',' ')) for tr in TR]
    tb.tabarray(records = Recs, names = ['Table','TableDescr','Number','Text']).saveSV(maindir + 'footnotes.tsv',metadata=True)

   
def pi_trigger():
    connection = pm.Connection()
    
    incremental = PI_NAME in connection['govdata'].collection_names()
    if incremental:
        return 'increment'
    else:
        return 'overall'
        
        
@activate(lambda x : (x[0] + 'sqpi_raw/',x[0] + 'footnotes.tsv'),lambda x : x[0] + '__PARSE__/sqpi/')
def SQPI_preparse(maindir):
    sourcedir = maindir + 'sqpi_raw/'
    target =  maindir + '__PARSE__/sqpi/'
    MakeDirs(target)
    
    Y = tb.tabarray(SVfile = maindir + 'footnotes.tsv')
    filelist = [sourcedir + x for x in  listdir(sourcedir) if x.endswith('.csv')]
    
    for (i,f) in enumerate(filelist):
        print f

        [X, header, footer, keywords] = BEA_Parser(f, headerlines=1, FootnoteSplitter='/', FootnotesFunction=GetFootnotes, CategoryColumn='Line Title')
        table = X['Table'][0]
        X = X.deletecols(['First Year']).addcols(len(X)*[table + ',SQ'],names=['Subcollections'])
        X = X.addcols(['{"s":' + repr(x) + ',"f":{"s":' + repr(f) + '}}' for (f,x) in X[['State FIPS','State Name']]],names = ['Location'])
        X = X.deletecols(['State FIPS','State Name'])               
        X.renamecol('Line Code','LineCode')
        X.renamecol('Line Title','Line')
        X.renamecol('Line Title Footnotes', 'Line Footnotes')


    
        TimeColNames = X.coloring.get('Data')       
        for n in TimeColNames:
            X.renamecol(n,PI_dateparse(n))    
        X.coloring['TimeColNames'] = X.coloring.pop('Data')

        X.saveSV(target + str(i) + '.tsv',metadata=['dialect','names','formats'])
        

                
@activate(lambda x : x[0] + 'sapi_raw/',lambda x : x[0] + '__PARSE__/sapi/')
def SAPI_preparse(maindir):
    sourcedir = maindir + 'sapi_raw/'
    target =  maindir + '__PARSE__/sapi/'
    MakeDirs(target)
 
   
    M = [x for x in RecursiveFileList(sourcedir) if x.endswith('.csv')]

    for (i,f) in enumerate(M):
        print 'Processing', f
        
        temppath = target + f.split('/')[-1]
        strongcopy(f,temppath)
        F = open(temppath,'rU').read().strip().split('\n')
        sline = [j for j in range(len(F)) if F[j].startswith('"Source:')][0]
        nline = [j for j in range(len(F)) if F[j].startswith('"State FIPS')][0]
        s = F[nline] + '\n' + '\n'.join([F[j] for j in range(len(F)) if j not in [sline, nline]]) + '\n' + F[sline]
        F = open(temppath,'w')
        F.write(s)
        F.close()

        [X, header, footer, keywords] = BEA_Parser(temppath, headerlines=1, FootnoteSplitter='/', FootnotesFunction=GetFootnotes, CategoryColumn='Line Title')
        delete(temppath)
        
        table = X['Table'][0] 
        
        Summary = f.split('/')[-2] == 'sa_sum'
        if Summary:             
            unitsdict = {'personal income': 'Thousands of dollars', 'population': 'Number of persons', 'per capita personal income': 'Dollars'}
            units = [unitsdict[i.lower().replace('disposable ', '').strip()] for i in X['Line Title']]          
            X = X[['Info','Data','Footnotes']].addcols([units],names=['Units'])
     
        X = X.deletecols(['First Year']).addcols(len(X)*[table + ',SA' + (',SA_S' if Summary else '')],names=['Subcollections'])
        X = X.addcols(['{"s":' + repr(sname) + ',"f":{"s":' + repr(fips) + '}}' for (fips,sname) in X[['State FIPS','State Name']]],names = ['Location'])
        X = X.deletecols(['State FIPS','State Name'])   
        X.renamecol('Line Code','LineCode')
        X.renamecol('Line Title','Line')
        X.renamecol('Line Title Footnotes', 'Line Footnotes')
        
        TimeColNames = X.coloring.get('Data')
        for n in TimeColNames:
            X.renamecol(n,PI_dateparse(n))
        X.coloring['TimeColNames'] = X.coloring.pop('Data')
            
        X.saveSV(target + str(i) + '.tsv',metadata=['dialect','names','formats'])
            
    
def loc_processor(f,x,level):   
    if level == 'ALLCOUNTY':
        return '{"c":' + repr(','.join(x.split(',')[:-1]).strip()) + ',"S":' + repr( x.split(',')[-1].strip()) +',"f":{"c":' + repr(f[2:]) + ',"s":' + repr(f[:2]) + '}}' 
    elif level ==   'STATE':
        return '{"f":{"s":' + repr(f[:2]) + '}}' 
    elif level == 'METRO':
        return '{"m":' + repr(x) + ',"f":{"m":' + repr(f) + '}}' 
    elif level == 'CSA':
        return '{"b":' + repr(x) + ',"f":{"b":' + repr(f[2:]) + '}}' 
    elif level == 'MDIV':
        return '{"B":' + repr(x) + ',"f":{"B":' + repr(f) + '}}' 
    elif level == 'ECON':
        return '{"X":' + repr(x) + ',"f":{"X":' + repr(f) + '}}' 
        
        
@activate(lambda x : (x[0] + 'lapi_raw/' + x[1] + '_' + x[2] + '/',x[0] + 'lapi_codes_processed.tsv'),lambda x : x[0] + '__PARSE__/lapi/'+x[1] + '_' + x[2] + '/')
def LAPI_preparse(maindir,table,level):
    sourcedir = maindir + 'lapi_raw/' + table + '_' + level + '/'
    target =  maindir + '__PARSE__/lapi/' + table + '_' + level + '/'
    MakeDirs(target)
    
    Codes = tb.tabarray(SVfile = maindir + 'lapi_codes_processed.tsv')
    Codes = Codes[Codes['Table'] == table]
    
    for (i,c) in enumerate(Codes):
        linecode,line = c['Code'],c['CodeDescr']
        
        f = sourcedir + linecode + '.csv'
        [X, header, footer, keywords] = BEA_Parser(f, headerlines=1, FootnoteSplitter='/', FootnotesFunction=GetFootnotes, CategoryColumn=None)
        title = X.dtype.names[0]
        if len(title.split('/')) > 1:
            LineFootnote = title.split('/')[0].split()[-1]
            title = ' '.join(title.split('/')[0].split()[:-1]).strip()
        else:
            LineFootnote = None
        X.renamecol(X.dtype.names[0], 'LineCode')
        
        id = table + '_' + linecode

        h = Codes['Hierarchy'][i]
        subjcols = [list(x) for x in zip(*[tuple(h)]*len(X))]

        X = X.addcols([[table]*len(X),[line]*len(X),[table + ',LA,' + level + ',' + id]*len(X)] + subjcols,names=['Table','Line','Subcollections'] + list(h.dtype.names))
        X = X.addcols([loc_processor(fips,aname,level) for (fips,aname) in X[['FIPS','AreaName']]],names = ['Location'])
        X = X.deletecols(['FIPS','AreaName'])

        TimeColNames = X.coloring['Data']
        for n in TimeColNames:
            X.renamecol(n,PI_dateparse(n))    
        X.coloring['TimeColNames'] = X.coloring.pop('Data')
        
        X.metadata = {'LineFootnote':LineFootnote,'Table':table,'Line':line,'LineCode':linecode}

        X.saveSV(target + str(i) + '.tsv',metadata=True)
        

@activate(lambda x : x[0] + 'footnotes.tsv',lambda x : x[0] + '__metadata.pickle')
def PI_metadata(maindir):
    Y = tb.tabarray(SVfile = maindir + 'footnotes.tsv')
   
    Metadata = {}
    
    TFN = Y.aggregate(On=['Table'],AggList=[('TableDescr',lambda x : x[0]),('Footnote',lambda x : '\n'.join([w +': ' + z for (w,z) in zip(x['Number'],x['Text'])]),['Number','Text'])],KeepOthers=False)
    for x in TFN:
        Metadata[x['Table']] = {'Title':x['TableDescr'], 'Footnotes': x['Footnote']}
    
    Metadata['SQ'] = {'Title':'State Quarertly Persona Income','Description':'<a href="http://www.bea.gov/regional/sqpi/">State Quarterly Personal Income</a> data set under the <a href="http://www.bea.gov/regional/index.htm">Regional Economic Accounts</a> section of the <a href="http://www.bea.gov/">Bureau of Economic Accounts (BEA)</a> website.', 'Units':'Millions of dollars, seasonally adjusted at annual rates'}
    

    Metadata['SA_S'] = {'Title': 'State Annual Summary'}
    Metadata['SA'] = {'Title': 'State Annual Personal Income', 'Description' :  '<a href="http://www.bea.gov/regional/spi/">State Annual Personal Income</a> data set under the <a href="http://www.bea.gov/regional/index.htm">Regional Economic Accounts</a> section of the <a href="http://www.bea.gov/">Bureau of Economic Accounts (BEA)</a> website.  U.S. DEPARTMENT OF COMMERCE--ECONOMICS AND STATISTICS ADMINISTRATION BUREAU OF ECONOMIC ANALYSIS--REGIONAL ECONOMIC INFORMATION SYSTEM STATE ANNUAL TABLES 1969 - 2008 for the states and regions of the nation September 2009 These files are provided by the Regional Economic Measurement Division of the Bureau of Economic Analysis. They contain tables of annual estimates (see below) for 1969-2008 for all States, regions, and the nation. State personal income estimates, released September 18, 2009, have been revised for 1969-2008 to reflect the results of the comprehensive revision to the national income and product accounts released in July 2009 and to incorporate newly available state-level source data. For the year 2001 in the tables SA05, SA06, SA07, SA25, and SA27, the industry detail is available by division-level SIC only. Tables based upon the North American Industry Classification System (NAICS) are available for 2001-07. Newly available earnings by NAICS industry back to 1990 were released on September 26, 2006.   For more information on the industry classifications, see the BEA web pages on  <a href="http://www.bea.gov/regional/definitions/nextpage.cfm?key=NAICS">NAICS (1997-2008)</a> and <a href="http://www.bea.gov/regional/definitions/nextpage.cfm?key=SIC">SIC (1963-1997)</a>.   Historical estimates 1929-68 will be updated in the next several months. TABLES The estimates are organized by table. The name of the downloaded file indicates the table. For example, any filename beginning with "SA05" contains information from the SA05 table. With the exception of per capita estimates, all dollar estimates are in thousands of dollars. All employment estimates are number of jobs. All dollar estimates are in current dollars. SA04 - State income and employment summary (1969-2008) SA05 - Personal income by major source and earnings by industry (1969-2001, 1990-2008) SA06 - Compensation of employees by industry (1998-2001, 2001-08) SA07 - Wage and salary disbursements by industry (1969-01, 2001-08) SA25 - Total full-time and part-time employment by industry (1969-2001, 2001-08) SA27 - Full-time and part-time wage and salary employment by industry (1969-2001, 2001-08) SA30 - State economic profile (1969-08) SA35 - Personal current transfer receipts (1969-08) SA40 - State property income (1969-2008) SA45 - Farm income and expenses (1969-2008) SA50 - Personal current taxes (this table includes the disposable personal income estimate) (1969-08) DATA (*.CSV) FILES The files containing the estimates (data files) are in comma-separated-value text format with textual information enclosed in quotes.  (L) Less than $50,000 or less than 10 jobs, as appropriate, but the estimates for this item are included in the total. (T) SA05N=Less than 10 million dollars, but the estimates for this item are included in the total. SA25N=Estimate for employment suppressed to cover corresponding estimate for earnings. Estimates for this item are included in the total. (N) Data not available for this year. If you have any problems or comments on the use of these data files call or write: Regional Economic Information System Bureau of Economic Analysis (BE-55) U.S. Department of Commerce Washington, D.C. 20230 Phone (202) 606-5360 FAX (202) 606-5322 E-Mail: reis@bea.gov'}
    
    Metadata['LA'] = {'Title':'Local Area Personal Income', 'Description' : 'Local Area Personal Income data for all US counties, from the <a href="http://www.bea.gov/regional/reis/default.cfm?selTable=Single%20Line">Local Area Personal Income "Single Line of data for all counties"</a> data set under the <a href="http://www.bea.gov/regional/index.htm">Regional Economic Accounts</a> section of the <a href="http://www.bea.gov/">Bureau of Economic Accounts (BEA)</a> website.  For more information on the industry classifications, see the BEA web pages on  <a href="http://www.bea.gov/regional/definitions/nextpage.cfm?key=NAICS">NAICS (1997-2008)</a> and <a href="http://www.bea.gov/regional/definitions/nextpage.cfm?key=SIC">SIC (1963-1997)</a>.  With the exception of per capita estimates, all dollar estimates are in thousands of dollars. All employment estimates are number of jobs. All dollar estimates are in current dollars.'}

    AllMeta = {}
    AllMeta['Source'] = [('Agency',{'Name':'Department of Commerce','ShortName':'DOC'}),('Subagency',{'Name':'Bureau of Economic Analysis','ShortName':'BEA'}),('Program','Regional Economic Accounts'), ('Dataset','Personal Income')]
    AllMeta['TopicHierarchy']  = ('Agency','Subagency','Dataset','Category','Subcategory','SubjectHierarchy')
    AllMeta['UniqueIndexes'] = ['Location','Table','LineCode']
    AllMeta['ColumnGroups'] = {'SpaceColumns' : ['Location']}
    AllMeta['DateFormat'] = 'YYYYqmm'
    AllMeta['sliceCols'] = ['Location.c','Location.m','Location.s','Table','SubjectHierarchy']  
    AllMeta['phraseCols'] = ['Table','SubjectHierarchy','Line','LineCode']  
    Metadata[''] = AllMeta
    
    F = open(maindir + '__metadata.pickle','w')
    pickle.dump(Metadata,F)
    F.close()
    

class pi_parser(OG.csv_parser):

    def refresh(self,file):
        
        OG.csv_parser.refresh(self,file)
       
        self.metadata['']['ColumnGroups']['TimeColNames'] = uniqify(self.metadata['']['ColumnGroups']['TimeColNames'] + self.Data.coloring['TimeColNames'])
        
        if 'LineFootnote' in self.Data.metadata.keys():
            id = self.Data.metadata['Table'] + '_' + self.Data.metadata['LineCode']
            self.metadata[id] = {'Title':self.Data.metadata['Line'], 'Footnote': self.Data.metadata['LineFootnote']}
         
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def backend_BEA_PersonalIncome(creates = OG.CERT_PROTOCOL_ROOT + PI_NAME + '/',Fast = True):
  
    D = [((get_footnotes,'footnotes'),()),((get_line_codes,'lapi_line_codes'),()),((process_line_codes,'process_line_codes'),()),((SAPI_downloader,'sapi_raw'),()),((SQPI_downloader,'sqpi_raw'),())] 
    D += [((SAPI_preparse,'sapi_preparse'),()),((SQPI_preparse,'sqpi_preparse'),())] 
    
    tables = ['CA1-3', 'CA04', 'CA05N', 'CA25N', 'CA05', 'CA06', 'CA06N', 'CA25', 'CA30', 'CA34', 'CA35','CA45']
    areatypes = ['ALLCOUNTY', 'STATE','METRO','MDIV','CSA']
    D += [((LAPI_downloader,'lapi_' + t.replace('-','_') + '_' + a + '_raw'),(t,a)) for t in tables for a in areatypes]
    D += [((LAPI_preparse,'lapi_' + t.replace('-','_') + '_' + a + '_preparse'),(t,a)) for t in tables for a in areatypes]
    
    D += [((PI_metadata,'make_metadata'),())]
    
    (downloader, downloadArgs) = zip(*D)
    downloader = list(downloader)
    downloadArgs = list(downloadArgs)
    
    OG.backendProtocol(PI_NAME,pi_parser,downloader = downloader, downloadArgs = downloadArgs,trigger = pi_trigger,incremental=True)


def backend_BEA_RegionalGDP(creates = OG.CERT_PROTOCOL_ROOT + REG_NAME + '/',Fast = True):
    OG.backendProtocol(REG_NAME,OG.csv_parser,downloader = [(RegionalGDP_initialize,'initialize'),(GetStateManifest,'state_manifest'),(DownloadStateFiles,'get_state_files'),(DownloadMetroFiles,'get_metro_files'),(State_PreParse1,'state_preparse1'),(Metro_PreParse1,'metro_preparse1'),(RegionalGDP_Preparse2,'preparse2')],trigger = reg_trigger,incremental=True)


def backend_BEA_NIPA(creates = OG.CERT_PROTOCOL_ROOT + NIPA_NAME + '/',Fast = True):
    OG.backendProtocol(NIPA_NAME,OG.csv_parser,downloader = [(NIPA_downloader,'raw'),(NIPA_preparser1,'preparse1'),(NIPA_preparser2,'preparse2'),(get_additional_info,'additional_info')],trigger = trigger,incremental=True)
    
  
def backend_BEA_FAT(creates = OG.CERT_PROTOCOL_ROOT + FAT_NAME + '/',Fast = True):
    OG.backendProtocol(FAT_NAME,OG.csv_parser,downloader = [(FAT_downloader,'raw'),(FAT_preparser1,'preparse1'),(FAT_preparser2,'preparse2')],trigger = fat_trigger,incremental=True)
    
    
    
    
