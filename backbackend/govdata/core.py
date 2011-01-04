import tabular as tb

try:
    from collections import OrderedDict
except ImportError:
    from common.OrderedDict import OrderedDict


class DataIterator(object):

    def __iter__(self):
        return self
        
    def __getattr__(self,attr):
    
        try:
            V = self.metadata[''][attr]
        except KeyError:
            raise AttributeError, "Can't find attribute " + attr
        else:
            return V        
            
class CsvParser(DataIterator):

    def __init__(self,source):
        self.metadata = pickle.load(open(source + '__metadata.pickle'))
        
    def refresh(self,file):
        print 'refreshing', file
        self.Data = tb.tabarray(SVfile = file,verbosity = 0)
        self.IND = 0
    
    def next(self):
        if self.IND < len(self.Data):
            r = self.Data[self.IND]
            r = OrderedDict([(self.Data.dtype.names[i],float(xx) if isinstance(xx,float) else int(xx) if isinstance(xx,int) else xx) for (i,xx) in enumerate(r) if xx != ''])
            
            if 'subcollections' in r.keys():
                r['subcollections'] = r['subcollections'].split(',')
                
            for k in self.columnGroups.get('timeColumns',[]) + self.columnGroups.get('spaceColumns',[]):
                if k in r.keys():
                    r[k] = eval(r[k])               
            
            self.IND += 1
                
            return r
            
        else:
            raise StopIteration
            
class GovParser(object):

    def __init__(self,
                 collectionName,
                 parser,
                 downloader = None, 
                 downloadProtocol= None,
                 downloadArgs = None, 
                 downloadKwargs = None, 
                 parserArgs = None, 
                 parserKwargs = None, 
                 trigger = None, 
                 slicesCorrespondToIndexes=True, 
                 ID = None,
                 incremental = False,
                ):

        self.collectionName = collectionName;
        self.parser = parser;
        self.downloader = downloader
        self.downloadProtocol = downloadProtocol
        self.downloadArgs = downloadArgs
        self.parserArgs = parserArgs
        self.parserKwargs = parserKwargs
        self.trigger = trigger
        self.slicesCorrespondToIndexes = slicesCorrespondToIndexes
        self.ID = ID
        self.incremental = incremental
        
    
        
    def verify(self):
        self.checkMetadata()
        
    def checkMetadata(self):
        checkMetadata(self.parser)
        
   
    
def checkMetadata(iterator):
    assert hasattr(iterator,'metadata'), 'Has no metadata attribute.'
    
    metadata = iterator.metadata
    
    assert isinstance(metadata,dict), 'Metadata isnt a dictionary.'
    assert all(map(is_string_like,metadata.keys())), 'Metadata keys must be strings'
    assert '' in metadata.keys(), 'Metadata must contain "" key.'
    assert all(map(lambda x : isinstance(x,dict),metadata.values())), 'Metadata values must be dictionaries.'
    assert all(map(lambda x : all(map(is_string_like,x.keys())),metadata.values())), 'metadata values\' keys must be strings.'
   
    assert all(['title' in metadata[k].keys() for k in metadata.keys() if k]), 'The subcollections following have no "title" metadata:' + str([k for k in metadata.keys() if 'title' not in metadata[k].keys() and k]) 
   
    M = metadata['']

    assert isinstance(M.get('keywords'),list) and all(map(is_string_like,M['keywords'])), 'Metadata must contain "keywords" entry, which must be a python list.'
    assert is_string_like(M.get('description')), 'Metadata must contain description.'
    
    S = M.get('source') 
    assert isinstance(S,list), 'Metadata must contain source list.'
    try:
        S = OrderedDict(S)
    except:
        print 'Metadata source list in wrong format for making SON object.'
    else:
        pass
    required = ['agency','subagency','dataset']
    assert all([r in S.keys() for r in required]), 'Source dictionary must contain agency, subagency, and dataset keys.'
    assert all(map(lambda x : isinstance(x,dict),S.values())), 'All source dictionary keys must be dictionaries containing "name" and possibly "shortName" keys.'

    assert all(map(lambda x : 'name' in x.keys(),S.values()))
    assert all(['shortName' in S[r].keys() for r in required]), 'shortName entry required for agency, subagency, and dataset source.'
    p = re.compile('[\w]*')
    assert all([p.match(y.get('shortName','')) for y in S.values()]), 'shortName entry can only contain alphanumeric and "_" characters.'
    
    SC = M.get('sliceCols')
    assert isinstance(SC,list) and all([isinstance(x,list) and all([is_string_like(y) or isinstance(y,tuple) for y in x]) for x in SC]), 'sliceCol metadata not present or improperly formed.'
       
    CG = M.get('columnGroups')
    assert isinstance(CG,dict) and 'labelColumns' in CG.keys(), 'columnGroup metadata not present or improperly formed.'

    
    