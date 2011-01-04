import random
import cPickle

from starflow.utils import activate


@activate(lambda x : x[0],lambda x : x[1])
def wget(getpath,savepath,opstring=''):
    os.system('wget ' + opstring + ' "' + getpath + '" -O "' + savepath + '"')



def createCertificate(path,msg,tol=10000000000):
    F = open(path,'w')
    F.write(msg + ' Random certificate: ' + str(random.randint(0,tol)))
    F.close()
    
def createCertificateDict(path,d,tol=10000000000):
    x = d.get('__certificate__')
    if x: 
        raise ValueError, '__certificate__ key must not already be in dictionary'
        
    d['__certificate__'] = random.randint(0,tol)
    F = open(path,'w')
    cPickle.dump(d,F)
    F.close()
    
