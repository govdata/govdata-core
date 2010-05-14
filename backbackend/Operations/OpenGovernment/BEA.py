import os
import numpy as np
import tabular as tb
from BeautifulSoup import BeautifulSoup, BeautifulStoneSoup, NavigableString
from mechanize import Browser
import urllib
import re
import pymongo as pm
from System.Utils import MakeDir, Contents, listdir, IsDir, wget, uniqify, PathExists,RecursiveFileList, ListUnion, MakeDirs
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
    
    [Y, header, footer, keywords]    = BEA_Parser(page, headerlines=headerlines, FootnoteSplitter = FootnoteSplitter, FootnotesFunction = FootnotesFunction, CategoryColumn=CategoryColumn, NEA=True, FormulaColumn=FormulaColumn)
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
    
    
def backend_BEA_NIPA(creates = OG.CERT_PROTOCOL_ROOT + NIPA_NAME + '/',Fast = True):
    OG.backendProtocol(NIPA_NAME,trigger,OG.csv_parser,downloader = [(NIPA_downloader,'raw'),(NIPA_preparser1,'preparse1'),(NIPA_preparser2,'preparse2'),(get_additional_info,'additional_info')],uptostep='updateCollection',incremental=True)
    
    
    
    
    
