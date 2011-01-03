from starflow.utils import activate

@activate(lambda x : x[0],lambda x : x[1])
def wget(getpath,savepath,opstring=''):
    os.system('wget ' + opstring + ' "' + getpath + '" -O "' + savepath + '"')
