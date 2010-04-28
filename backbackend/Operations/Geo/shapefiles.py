from System.Utils import MakeDir, wget,Contents, listdir, IsDir, RecursiveFileList
import os
import subprocess
import BeautifulSoup as BS
from System.Protocols import activate, ApplyOperations2

root = '../Data/ShapeFiles/'
protocol_root = '../Protocol_Instances/ShapeFiles/'

def initialize(creates = root):
	MakeDir(creates)
	
	
def protocol_initialize(creates = protocol_root):
	MakeDir(creates)	
	
def getCensusDivisions(depends_on = 'http://www.census.gov/geo/cob/bdy/dv/dv99_d00_shp.zip', creates = root + 'CensusDivisions/'):
	MakeDir(creates)
	wget(depends_on,creates + 'dv99_d00_shp.zip')
	os.system('cd ' + creates + ' ; unzip dv99_d00_shp.zip')
	
def getCensusRegions(depends_on = 'http://www.census.gov/geo/cob/bdy/rg/rg99_d00_shp.zip', creates = root + 'CensusRegions/'):
	MakeDir(creates)
	wget(depends_on,creates + 'rg99_d00_shp.zip')
	os.system('cd ' + creates + ' ; unzip rg99_d00_shp.zip')
	
def getCensusTracts(depends_on = 'http://www.census.gov/geo/www/cob/tr2000.html',creates = root + 'CensusTracts/'):
	MakeDir(creates)
	wget(depends_on,creates + '__Index.html')
	Soup = BS.BeautifulSoup(open(creates + '__Index.html'))
	A = [(Contents(x.findParent()).split(' - ')[0].strip(),str(dict(x.attrs)['href'])) for x in Soup.findAll('a') if '_shp' in str(x)]
	for (name,url) in A:
		print 'Downloading', name
		wget('http://www.census.gov' + url, creates + url.split('/')[-1])
		os.system('cd ' + creates + ' ; unzip ' + url.split('/')[-1])

def getCounties(depends_on = 'http://www.census.gov/geo/cob/bdy/co/co00shp/co99_d00_shp.zip', creates = root + 'Counties/'):
	MakeDir(creates)
	wget(depends_on,creates + 'co99_d00_shp.zip')
	os.system('cd ' + creates + ' ; unzip co99_d00_shp.zip')
	
	
def getCountySubdivisions(depends_on = 'http://www.census.gov/geo/www/cob/cs2000.html',creates = root + 'CountySubdivisions/'):
	MakeDir(creates)
	wget(depends_on,creates + '__Index.html')
	Soup = BS.BeautifulSoup(open(creates + '__Index.html'))
	A = [(Contents(x.findParent()).split(' - ')[0].strip(),str(dict(x.attrs)['href'])) for x in Soup.findAll('a') if '_shp' in str(x)]
	for (name,url) in A:
		print 'Downloading', name
		wget('http://www.census.gov' + url, creates + url.split('/')[-1])
		os.system('cd ' + creates + ' ; unzip ' + url.split('/')[-1])
		
		
def getMSA(depends_on = 'http://www.census.gov/geo/www/cob/mmsa2003.html', creates = root + 'MSA/'):
	MakeDir(creates)
	for x in ['cs99_03c_shp.zip','cb99_03c_shp.zip','md99_03c_shp.zip']:
		wget('http://www.census.gov/geo/cob/bdy/metroarea/2003/shp/' + x, creates + x)
		os.system('cd ' + creates + ' ; unzip ' + x)
	
	
def getStates(depends_on = 'http://www.census.gov/geo/cob/bdy/st/st00shp/st99_d00_shp.zip', creates = root + 'States/'):
	MakeDir(creates)
	wget(depends_on,creates + 'st99_d00_shp.zip')
	os.system('cd ' + creates + ' ; unzip st99_d00_shp.zip')
	
		
def getFiveDigitZCTAs(depends_on = 'http://www.census.gov/geo/www/cob/z52000.html',creates = root + 'FiveDigitZCTAs/'):
	MakeDir(creates)
	wget(depends_on,creates + '__Index.html')
	Soup = BS.BeautifulSoup(open(creates + '__Index.html'))
	A = [(Contents(x.findParent()).split(' - ')[0].strip(),str(dict(x.attrs)['href'])) for x in Soup.findAll('a') if '_shp' in str(x)]
	for (name,url) in A:
		print 'Downloading', name
		wget('http://www.census.gov' + url, creates + url.split('/')[-1])
		os.system('cd ' + creates + ' ; unzip ' + url.split('/')[-1])	
	
def OGRInspectorInstantiator(depends_on = root, creates = protocol_root + 'OGRInspectors.py'):
	L = [l for l in listdir(depends_on) if IsDir(depends_on + l)]
	
	outdir = '../Data/ShapeFileOGRInspections/'
	
	D = [('initialize',MakeDir,(outdir,))]
	D += [('Inspect_' + l,ogrinspectdir,(depends_on + l,l,outdir + l + '.py')) for l in L]
	
	ApplyOperations2(creates,D)
	
	
@activate(lambda x : x[0], lambda x : x[2])	
def ogrinspectdir(dirname,name,outfile):
	Files = [x for x in RecursiveFileList(dirname) if x.endswith('.shp')]
	Texts = [ogrinspect(x,name) for x in Files]
	Text = '\n\n'.join(['###########' + x + ':\n\n' + t for (x,t) in zip(Files,Texts)])
	F = open(outfile,'w')
	F.write(Text)
	F.close()

def ogrinspect(file,name):
	P = subprocess.Popen(['python ../../common/geo/geodjango/manage.py ogrinspect ' + file + ' ' + name + ' --srid=4326 --mapping --multi'],stdout=subprocess.PIPE,shell=True)
	return P.communicate()[0]

	
