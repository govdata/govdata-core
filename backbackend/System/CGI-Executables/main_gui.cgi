#!/usr/bin/python

'''
cgi module implementing main Data Environment Graphical interactive browser

'''

import cgi,os,cgitb
cgitb.enable()
# Required header that tells the browser how to render the HTML.
print 'Content-Type: text/html\n\n'

def AllGui(Path='../',Summary='../System/MetaData/',Fragment='../',Representation='../',Mode='ColDir',ShowUses='No',ShowImplied='No',View=None,Tab=None):
	'''
		Make 'combined view' gui
	'''

	print '<HTML><HEAD><TITLE>' + DE_NAME + ' -- Info For: ' + Path + ' </TITLE>\n'
	print '</HEAD>\n'
	print '<FRAMESET cols= "20%, 80%" BORDER=.2>'
	print '<FRAMESET rows= "60%, 40%">'
	print '<FRAME src = "/System/CGI-Executables/IntegratedFileBrowser.cgi?Path=' + Path + '&Mode=' + Mode + '&ShowUses=' + ShowUses + '&ShowImplied=' + ShowImplied + '" />'
	print '<FRAME src = "/System/CGI-Executables/SummaryMetaData.cgi?Summary=' + Summary + '&Fragment=' + Fragment +  '" />'
	print '</FRAMESET>'
	print '<FRAMESET rows= "6%,60%,34%">'
	print  '<FRAME src = "/System/CGI-Executables/ModeChooserBar.cgi?Path=' + Path + '&Summary=' + Summary + '&Fragment=' + Fragment + '&Representation=' + Representation + '&Mode=' + Mode +  '&ShowUses=' + ShowUses + '&ShowImplied=' + ShowImplied +  '&View=' + View + '&Tab=' + Tab + '"/>'
	print '<FRAME src = "/System/CGI-Executables/RepresentationMaker.cgi?Representation=' + Representation +'" />'
	print '<FRAME src = "/System/CGI-Executables/FragmentMaker.cgi?Fragment=' + Fragment + '&Mode=' + Mode +  '&ShowUses=' + ShowUses + '&ShowImplied=' + ShowImplied + '" />'
	print '</FRAMESET>'
	print '</FRAMESET>'
	print '</HTML>'
	

def TabGui(Path='../',Summary='../System/MetaData/',Fragment='../',Representation='../',Mode='ColDir',ShowUses='No',ShowImplied='No',View=None,Tab=None):
	'''
		Make 'tab view' gui
	'''
	
	print '<HTML><HEAD><TITLE>' + DE_NAME + '  -- Info For: ' + Path + ' </TITLE>\n'
	print '</HEAD>\n'
	print '<FRAMESET rows = "5%,95%" BORDER=.2>'
	print '<FRAME src = "/System/CGI-Executables/ModeChooserBar.cgi?Path=' + Path + '&Summary=' + Summary + '&Fragment=' + Fragment + '&Representation=' + Representation + '&Mode=' + Mode +  '&ShowUses=' + ShowUses + '&ShowImplied=' + ShowImplied +  '&View=' + View + '&Tab=' + Tab + '" title = "Chooser" />'	
	print '<FRAMESET cols= "15%, 85%">'
	print '<FRAME src = "/System/CGI-Executables/IntegratedFileBrowser.cgi?Path=' + Path + '&Mode=' + Mode + '&ShowUses=' + ShowUses + '&ShowImplied=' + ShowImplied + '" />'
	print '<FRAMESET rows= "85%,15%">'
	if Tab == 'Links':
		print '<FRAME src = "/System/CGI-Executables/FragmentMaker.cgi?Fragment=' + Fragment + '&Mode=' + Mode +  '&ShowUses=' + ShowUses + '&ShowImplied=' + ShowImplied + '" />'	
	else:
		print '<FRAME src = "/System/CGI-Executables/RepresentationMaker.cgi?Representation=' + Representation +'" />'
	print '<FRAME src = "/System/CGI-Executables/SummaryMetaData.cgi?Summary=' + Summary + '&Fragment=' + Fragment +  '" />'	
	print '</FRAMESET>'
	print '</FRAMESET>'
	print '</FRAMESET>'
	print '</HTML>'


def generate_gui(Path='../',Summary='../System/MetaData/',Fragment='../',Representation='../',Mode='ColDir',ShowUses='No',ShowImplied='No',View=None,Tab=None):
	'''
		subsidiary of "main" function 
	'''

	if os.path.exists('../System/initialize_GUI'):			
		if View == 'Tab':
			TabGui(Path=Path,Summary=Summary,Fragment=Fragment,Representation=Representation,Mode=Mode,ShowUses=ShowUses,ShowImplied=ShowImplied,View=View,Tab=Tab)
		elif View == 'All':
			AllGui(Path=Path,Summary=Summary,Fragment=Fragment,Representation=Representation,Mode=Mode,ShowUses=ShowUses,ShowImplied=ShowImplied,View=View,Tab=Tab)
		else:
			print 'View choice not recognized, defaulting to all'
			AllGui(Path=Path,Summary=Summary,Fragment=Fragment,Representation=Representation,Mode=Mode,ShowUses=ShowUses,ShowImplied=ShowImplied,View=View,Tab=Tab)
	else:
		print 'No GUI File configured (it should be at ../System/initialize_GUI)'

		
def main():
	Variables = cgi.FieldStorage()
	if Variables.has_key('Path'):
		Path = Variables['Path'].value
	else:
		Path = '../'
	if Variables.has_key('Summary'):
		Summary = Variables['Summary'].value
	else:
		Summary = '../System/MetaData/'
	if Variables.has_key('Fragment'):
		Fragment = Variables['Fragment'].value
	else:
		Fragment = '../'
	if Variables.has_key('Representation'):
		Representation = Variables['Representation'].value
	else:
		Representation = '../'		
	if Variables.has_key('Mode'):
		Mode = Variables['Mode'].value
	else:
		Mode = 'ColDir'			
	if Variables.has_key('ShowUses'):
		ShowUses = Variables['ShowUses'].value
	else:
		ShowUses = 'No'		
	if Variables.has_key('ShowImplied'):
		ShowImplied = Variables['ShowImplied'].value
	else:
		ShowImplied = 'No'	

	if os.path.exists('../System/GUICurrentState'):
		StateDict = eval(open('../System/GUICurrentState','r').read())
	else:
		StateDict = {}

	if Variables.has_key('View'):
		View = Variables['View'].value
	else:
		if 'ViewState' in StateDict.keys():
			View = StateDict['ViewState']
		else:
			View = 'All'
	
	if Variables.has_key('Tab'):
		Tab = Variables['Tab'].value
	else:
		if 'TabState' in StateDict.keys():
			Tab = StateDict['TabState']
		else:
			Tab = 'Links'

	generate_gui(Path,Summary,Fragment,Representation,Mode,ShowUses,ShowImplied,View,Tab)


os.chdir('../../Temp')

if os.path.exists('../System/initialize_GUI'):
	execfile('../System/initialize_GUI')
	try:
		from System.config.SetupFunctions import DE_NAME
	except:
		DE_NAME = 'Data Environment'
		
	main()
else:
	'No GUI setup file.'
