import os
from System.Utils import MakeDir,Contents,listdir,wget,PathExists, strongcopy,uniqify,ListUnion,Rename, delete, MakeDirs
import Operations.htools as htools
from BeautifulSoup import BeautifulSoup,BeautifulStoneSoup
import tabular as tb
import numpy as np
from System.MetaData import AttachMetaData,loadmetadata
from System.Protocols import activate,ApplyOperations2
import time,re
import cPickle as pickle
import hashlib
import Operations.OpenGovernment.OpenGovernment as OG       

root = '../Data/OpenGovernment/BLS/'
protocol_root = '../Protocol_Instances/OpenGovernment/BLS/'

MAIN_SPLITS = ['cu', 'cw', 'su', 'ap', 'li', 'pc', 'wp', 'ei', 'ce', 'sm', 'jt', 'bd', 'oe', 'lu', 'la', 'ml', 'nw', 'ci', 'cm', 'eb', 'ws', 'le', 'cx', 'pr', 'mp', 'ip', 'in', 'fi', 'ch', 'ii']

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
    
    
def WgetMultiple(link, fname, maxtries=10):
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


@activate(lambda x : "ftp://ftp.bls.gov/pub/time.series/" + x[1], lambda x : x[0])
def bls_downloader(download_dir,code):

    MakeDirs(download_dir)
    download_dir += ('/' if download_dir[-1] != '/' else '')

    MakeDir(download_dir + 'RawDownloads/')
    
    get = "ftp://ftp.bls.gov/pub/time.series/" + code + '/'
    
    WgetMultiple(get,download_dir + 'RawDownloads/index.html')
    Soup = BeautifulSoup(open(download_dir + 'RawDownloads/index.html'))
    A = Soup.findAll('a')
    Records = [(Contents(a),str(dict(a.attrs)['href'])) for a in A]
    Records = [r for r in Records if 'Current' not in r[0].split('.')]
    RecordsR = [r for r in Records if 'AllData' in r[0]]
    if RecordsR:
        Records = RecordsR + [r for r in Records if not '.data.' in r[0]]
    T = tb.tabarray(records = Records,names = ['File','URL'])
    for (f,u) in T:
        wget(u,download_dir + 'RawDownloads/' + f + '.txt')

    makemetadata(code,download_dir + 'RawDownloads/',download_dir + 'metadata.pickle',download_dir + 'filenames.tsv')
    
    MakeDir(download_dir + '__FILES__')
    
    processtextfile(download_dir + 'RawDownloads/',download_dir + '/__FILES__/documentation.txt')
    
    MakeDir(download_dir + '__PARSE__')
    for l in listdir(download_dir  + 'RawDownloads/'):
        if '.data.' in l:
            Rename(download_dir + 'RawDownloads/' + l, download_dir + '__PARSE__/' + l)

    SPs = [download_dir + 'RawDownloads/' + l for l in listdir(download_dir + 'RawDownloads/') if l.endswith('.series.txt')]
    assert len(SPs) == 1, 'Wrong number of series paths.'
    serpath = SPs[0]    
    parse_series(download_dir + 'RawDownloads/',download_dir + 'series.txt')
    delete(serpath)


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
            X = X.addcols(NewCols,  names = NewNames)
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
def makemetadata(code,datadir,outfile1,outfile2,depends_on = (root + 'ProcessedManifest_2_HandAdditions.tsv',)):
    Z = getcategorydata(code)

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
    


def getcategorydata(code,depends_on = (root + 'BLS_Hierarchy/Manifest_1.tsv',root + 'Keywords.txt')):
    
    manifest,keywords = depends_on
    
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
    elif 'msa' in parts and ('code' in parts or 'fips' in parts):
        return 'f.m'
    



class bls_parser(OG.dataIterator):

    def __init__(self,source):
    
        self.metafile = metafile = source + 'metadata.pickle'
        self.docfile = docfile = source + 'documentation.txt'
        self.seriesfile = seriesfile = source + 'series.txt'
        self.filelistfile = filelistfile = source + 'filenames.tsv'
  
        
        self.metadata = {}
        self.metadata[''] = D = {}
        
        M = pickle.load(open(metafile))
        for x in ['ContactInfo','description','keywords']:
            D[x] = M[x]
        D['Source'] = [('Agency',{'Name':'Department of Labor','ShortName':'DOL'}),('Subagency',{'Name':'Bureau of Labor Statistics','ShortName':'BLS'}),('Topic',M['Topic']),('Subtopic',M['Subtopic']),('Program',{'Name':M['ProgramName'],'ShortName':M['ProgramAbbr']}),('Dataset',{'Name':M['Dataset'],'ShortName':M['DatasetCode']})]
        D['DateFormat'] = 'YYYYhqmm'

        M = tb.io.getmetadata(seriesfile)[0]
        self.headerlines = M['headerlines']
        getnames = M['coloring']['NewNames']
        names = M['names']
        spaceCodes = [inferSpaceCode(n) for n in names]
        self.getcols = [names.index(x) for (x,y) in zip(names,spaceCodes) if y == None and x in getnames]
        self.spacecols = [(names.index(x),y) for (x,y) in zip(names,spaceCodes) if y != None]
        self.fipscols = [(j,y) for (j,y) in self.spacecols  if y.startswith('f.')]
        self.nonfipscols = [(j,y) for (j,y) in self.spacecols  if not y.startswith('f.')]
        goodNames = [nameProcessor(x) for x in names]
        self.NAMES = ['Subcollections', 'Series'] + [goodNames[i] for i in self.getcols] + (['Location'] if self.spacecols else [])
        
        labelcols = [goodNames[i] for i in self.getcols] + (['Location'] if self.spacecols else [])

        self.TIMECOLS = []
        D['ColumnGroups'] = {'TimeColNames': self.TIMECOLS, 'LabelColumns': labelcols }
        if self.spacecols:
            D['ColumnGroups']['SpaceColumns'] = ['Location']
        D['UniqueIndexes'] = ['Series']
        D['sliceCols'] = [g for g in labelcols if g.lower().split('.')[0] not in ['footnote','seasonal','periodicity','location']] + (['Location.' + x for x in dict(self.spacecols).values() if not x.startswith('f.')] if self.spacecols else [])

        print 'Added general metadata.'
        
        
    def refresh(self,file):
  
        x = file
        ColNo = x.split('!')[-1].split('.')[x.split('!')[-1].split('.').index('data') + 1]
  
        self.ColNo = ColNo
        FLF = tb.tabarray(SVfile = self.filelistfile)
        Paths = FLF['Path'].tolist()
        self.metadata[ColNo] = {'Title':FLF['FileName'][Paths.index(file.split('/')[-1])]}

        print 'Initializing for ', self.metadata[ColNo]['Title']                
    
        self.G = open(self.seriesfile,'rU')
        for i in range(self.headerlines):
            self.G.readline()
        self.sline = self.G.readline().strip('\n')
            
        self.F = open(file,'rU')
        self.dnames = self.F.readline().strip().split('\t')
        self.dline = self.F.readline().strip('\n') 
    

    def next(self):

        if self.dline:
            dlinesplit = [x.strip() for x in self.dline.split('\t')]
            ser = dlinesplit[0]
    
            found = False
            while not found:
                if self.sline.split('\t')[0].strip() == ser:
                    found = True
                else:
                    self.sline = self.G.readline().strip('\n')
            slinesplit = self.sline.split('\t')
                        
            Vals = [[self.ColNo],ser] + [slinesplit[j].strip() for j in self.getcols] + ([dict(([(y,slinesplit[j])  for (j,y) in self.nonfipscols] if self.nonfipscols else []) + ([('f',dict([(y.split('.')[1],slinesplit[j])  for (j,y) in self.fipscols]))]  if self.fipscols else []))] if self.spacecols else [])
            
            servals = dict(zip(self.NAMES,Vals))
            
            while dlinesplit[0] == ser and self.dline:

                if dlinesplit[3]:
                    t = tval(dlinesplit[1],dlinesplit[2])
                    if not t in self.TIMECOLS:
                        self.TIMECOLS.append(t)
                
                    servals[t] = float(dlinesplit[3] )
                    
                self.dline = self.F.readline().strip('\n')
                dlinesplit = [x.strip() for x in self.dline.split('\t')]

            return servals
        else:
            raise StopIteration
                    
                        

#actual creators =-=-=-=-=-=-=-=-=

def BLS_Initialize2(creates = protocol_root):
    MakeDir(creates)
    
def MakeBLS_Resource(creates = protocol_root + 'make_resources.py'):

    L = [{'Parser':BLS_mainparse1,'Getter':WgetMultiple},None]
    D = htools.hsuck('http://www.bls.gov/data/', root + 'BLS_Hierarchy/', L, ipath=creates,write=False)
    ApplyOperations2(creates,D)

def backendBLS_ap(creates = OG.CERT_PROTOCOL_ROOT + 'BLS_ap/'):
    OG.backendProtocol('BLS_ap',bls_parser,downloader = bls_downloader,downloadArgs = ('ap',))
   
def backendBLS_bd(creates = OG.CERT_PROTOCOL_ROOT + 'BLS_bd/'):
    OG.backendProtocol('BLS_bd',bls_parser,downloader = bls_downloader,downloadArgs = ('bd',))
    
    
def backendBLS_cw(creates = OG.CERT_PROTOCOL_ROOT + 'BLS_cw/'):
    OG.backendProtocol('BLS_cw',bls_parser,downloader = bls_downloader,downloadArgs = ('cw',))    
    
def backendBLS_li(creates = OG.CERT_PROTOCOL_ROOT + 'BLS_li/'):
    OG.backendProtocol('BLS_li',bls_parser,downloader = bls_downloader,downloadArgs = ('li',))  
    
def backendBLS_pc(creates = OG.CERT_PROTOCOL_ROOT + 'BLS_pc/'):
    OG.backendProtocol('BLS_pc',bls_parser,downloader = bls_downloader,downloadArgs = ('pc',))  
    
def backendBLS_wp(creates = OG.CERT_PROTOCOL_ROOT + 'BLS_wp/'):
    OG.backendProtocol('BLS_wp',bls_parser,downloader = bls_downloader,downloadArgs = ('wp',))  
    
def backendBLS_ce(creates = OG.CERT_PROTOCOL_ROOT + 'BLS_ce/'):
    OG.backendProtocol('BLS_ce',bls_parser,downloader = bls_downloader,downloadArgs = ('ce',))   
    
def backendBLS_sm(creates = OG.CERT_PROTOCOL_ROOT + 'BLS_sm/'):
    OG.backendProtocol('BLS_sm',bls_parser,downloader = bls_downloader,downloadArgs = ('sm',))
    
def backendBLS_jt(creates = OG.CERT_PROTOCOL_ROOT + 'BLS_jt/'):
    OG.backendProtocol('BLS_jt',bls_parser,downloader = bls_downloader,downloadArgs = ('jt',))  
    

def backendBLS_lu(creates = OG.CERT_PROTOCOL_ROOT + 'BLS_lu/'):
    OG.backendProtocol('BLS_lu',bls_parser,downloader = bls_downloader,downloadArgs = ('lu',)) 
    
def backendBLS_la(creates = OG.CERT_PROTOCOL_ROOT + 'BLS_la/'):
    OG.backendProtocol('BLS_la',bls_parser,downloader = bls_downloader,downloadArgs = ('la',)) 