import os
import time
import re
import cPickle as pickle

import tabular as tb
import numpy as np

from BeautifulSoup import BeautifulSoup,BeautifulStoneSoup
from starflow.protocols import protocolize, actualize
from starflow.utils import MakeDir,Contents,listdir,PathExists, strongcopy,uniqify,ListUnion,Rename, delete, MakeDirs, is_string_like

import utils.htools as htools

from utils.basic import wget
from govdata.sources.core import SOURCE_COMPONENTS_DIR


DATA_ROOT = os.path.join('..','sources','BLS_data')


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
    link = link if is_string_like(link) else link['URL']
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
    
def initialize(creates = DATA_ROOT):
    MakeDir(creates)

   
@protocolize()  
def MakeBLS_Resource():
    L = [{'Parser':BLS_mainparse1,'Getter':WgetMultiple},None]
    D = htools.hsuck('http://www.bls.gov/data/', os.path.join(DATA_ROOT,'BLS_Hierarchy') + '/', L, write=False)
    actualize(D)
    
    
MAIN_SPLITS = ['cu', 'cw', 'su', 'ap', 'li', 'pc', 'wp', 'ei', 'ce', 'sm', 'jt', 'bd', 'oe', 'lu', 'la', 'ml', 'nw', 'ci', 'cm', 'eb', 'ws', 'le', 'cx', 'pr', 'mp', 'ip', 'in', 'fi', 'ch', 'ii']


def make_bls_resources(creates = os.path.join(SOURCE_COMPONENTS_DIR,'bls.pickle')):

    S = {}
    for code in MAIN_SPLITS:
        M = getcategorydata(code)
        S['BLS_' + code] = [('agency',{'name':'Department of Labor','shortName':'DOL'}),
                            ('subagency',{'name':'Bureau of Labor Statistics','shortName':'BLS'}),
                            ('topic',{'name':M['Topic']}),('subtopic',{'name':M['Subtopic']}),
                            ('program',{'name':M['ProgramName'],'shortName':M['ProgramAbbr']}),
                            ('dataset',{'name':M['Dataset'],'shortName':M['DatasetCode']})]

    F = open(creates,'w')
    pickle.dump(S,F)
    F.close()
    


def getcategorydata(code,depends_on = os.path.join(DATA_ROOT ,'BLS_Hierarchy','Manifest_1.tsv')):
    
    manifest = depends_on
    
    X = tb.tabarray(SVfile = manifest)
   
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
    
        
    return {'Topic':topic,'Subtopic':subtopic,'Dataset':Dataset,'ProgramName':ProgramName,'ProgramAbbr':ProgramAbbr,'DatasetCode':code}
