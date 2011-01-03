
from starflow.utils import RecursiveFileList,CheckInOutFormulae

def get_live_modules(LiveModuleFilters):
    '''
    Function for filtering live modules that is fast by avoiding looking 
    through directories that will be irrelevant.
    '''
    FilteredModuleFiles = []
    Avoid = ['^RawData$','^Data$','^.svn$','^ZipCodeMaps$','.data$','^scrap$']
    FilterFn = lambda z,y : y.split('.')[-1] == 'py' and CheckInOutFormulae(z,y)
    for x in LiveModuleFilters.keys():
        Filter = lambda y : FilterFn(LiveModuleFilters[x],y) 
        FilteredModuleFiles += filter(Filter,RecursiveFileList(x,Avoid=Avoid))
    return FilteredModuleFiles
