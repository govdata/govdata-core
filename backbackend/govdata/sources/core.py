import cPickle as pickle
import os
import pymongo as pm

from starflow.protocols import actualize, protocolize
from starflow.utils import RecursiveFileList, MakeDir, activate
from utils.basic import createCertificateDict


#put overalls into DB instead

SOURCE_COMPONENTS_DIR = '../sources/SOURCE_COMPONENTS/'

SOURCE_DB_NAME = '__COMPONENTS__'

CERTIFICATE_DIR = '../source/certificates'

def initialize(creates = SOURCE_COMPONENTS_DIR):
    MakeDir(creates)
    
def initialize_certificates(creates = CERTIFICATE_DIR):
    MakeDir(creates)


@protocolize()
def combine_components(depends_on = SOURCE_COMPONENTS_DIR):

    L = [(path,os.path.relpath(x,depends_on).replace(os.sep,'__')) for x in RecursiveFileList(depends_on) if x.endswith('.pickle')]
    D = [('component_' + name, add_component,(file,os.path.join(CERTIFICATE_DIR,name))) for (name,file) in L]
    
    actualize(D)
    
    
    
@activate(lambda x : x[0], lambda x : x[1])
def add_component(infile,certpath):

    All = pickle.load(open(infile))
       
    conn = pm.Connection()
    
    db = conn['govdata']
    
    coll = db[SOURCE_DB_NAME]
    coll.ensureIndex('name',unique=True)
    coll.ensureIndex('source',unique=True)
    
    for k in All:
       rec = {'name': k, 'source' : All[k]}
       coll.save(rec,safe=True)
       
    createCertificateDict(certpath,rec)
       
    
       
       
       
       
    
    
    
    
