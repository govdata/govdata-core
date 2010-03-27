#!/usr/bin/python

'''
produces mode chooser bar at top of GUI
'''

import cgi,os, platform
# Required header that tells the browser how to render the HTML.
print 'Content-Type: text/html\n\n'

Modes = dict([('All','All'),('ColDir','Directories'),('ColPro','Protocols')])

Variables = cgi.FieldStorage()
if Variables.has_key('Path'):
	Path = Variables['Path'].value
else:
	Path = '../'
if Variables.has_key('Summary'):
	Summary = Variables['Summary'].value
else:
	Summary = '../System/MetData/'
if Variables.has_key('Fragment'):
	Fragment = Variables['Fragment'].value
else:
	Fragment = '../'
if Variables.has_key('Representation'):
	Representation = Variables['Representation'].value
else:
	Representation = '../'		
if Variables.has_key('Mode'):
	CurrentMode = Variables['Mode'].value
else:
	CurrentMode = 'ColDir'			
if Variables.has_key('ShowUses'):
	ShowUses = Variables['ShowUses'].value
else:
	ShowUses = 'No'	
if Variables.has_key('ShowImplied'):
	ShowImplied = Variables['ShowImplied'].value
else:
	ShowImplied = 'No'	

os.chdir('../../Temp')

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

StateDict['ViewState'] = View
StateDict['TabState'] = Tab

F = open('../System/GUICurrentState','w')
F.write(str(StateDict))
F.close()

if os.path.exists('../System/initialize_GUI'):
	execfile('../System/initialize_GUI')
	import System.Web.text2html
	from System.Utils import IsDir, IsFile
	
	X = os.environ
	AU = X['AutomaticUpdatesPath']
	LM = X['LiveModuleFilterPath']
	
	FilePath = Representation.split('@')[0]
	for (varname,to_open) in zip(('OpenAUs','OpenLMs','OpenFile'),(AU,LM,FilePath)):
		if varname in Variables.keys():
			openit = Variables[varname].value
		else:
			openit = 'No'
		
		if openit == 'Yes':
		
			if os.path.isfile(to_open):
				if 'Editor' in X.keys():
					os.system(X['Editor'] + ' ' + to_open)
				else:
					print 'No text editor command specified.'
			elif os.path.isdir(to_open):			
				if (platform.platform().startswith('Darwin') or platform.mac_ver()[0] != ''):
					#print 'open /Users/dyamins/NewDataEnvironment/' + to_open[3:]
					#os.system('open /Users/dyamins/NewDataEnvironment/' + to_open[3:])

					os.system('open ' + to_open)
				elif platform.platform().startswith('Windows') or platform.platform().startswith('CYGWIN'):
					os.system('explorer ' + to_open.replace('/',r'\\'))
				else:
					print "Your platform doesnt support gui file browsing."
					
print '<HTML><HEAD><TITLE>CurrentModeChooserBar</TITLE>'
print '<style type="text/css">'
print '.bleft {text-align: left}'
print '.bright {text-align: right}'
print '</style>'

print '</HEAD><BODY>'
print '<table><tr>'

print '<td align="left"><a target=_top href="/System/CGI-Executables/qstat.cgi"/>Qstat</a>'

print '&nbsp&nbsp'

if IsDir(FilePath) or IsFile(FilePath):
	message = 'Open Directory' if IsDir(FilePath) else 'Open File'
	print '<td align="left"><a href="/System/CGI-Executables/ModeChooserBar.cgi?Path=' + Path + '&Summary=' + Summary + '&Fragment=' + Fragment + '&Representation=' + Representation + '&Mode=' + CurrentMode +  '&ShowUses=' + ShowUses + '&ShowImplied=' + ShowImplied + '&View=' + View + '&Tab=' + Tab + '&OpenFile=Yes"/>' + message + '</a>'
else:
	print  '<span style="color: #696969;">Can\'t Open</span>'

print '&nbsp&nbsp'

print '<td align="left"><a href="/System/CGI-Executables/ModeChooserBar.cgi?Path=' + Path + '&Summary=' + Summary + '&Fragment=' + Fragment + '&Representation=' + Representation + '&Mode=' + CurrentMode +  '&ShowUses=' + ShowUses + '&ShowImplied=' + ShowImplied + '&View=' + View + '&Tab=' + Tab + '&OpenLMs=Yes"/>LMs</a>'

print '&nbsp&nbsp'	

print '<a href="/System/CGI-Executables/ModeChooserBar.cgi?Path=' + Path + '&Summary=' + Summary + '&Fragment=' + Fragment + '&Representation=' + Representation + '&Mode=' + CurrentMode +  '&ShowUses=' + ShowUses + '&ShowImplied=' + ShowImplied + '&View=' + View + '&Tab=' + Tab + '&OpenAUs=Yes"/>AUs</a>'

print '&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp</td>'

print '<td style="text-align:right">'	
if View == 'Tab':
	print '<a target=_top href="/System/CGI-Executables/main_gui.cgi?Path=' + Path + '&Summary=' + Summary + '&Fragment=' + Fragment + '&Representation=' + Representation + '&Mode=' + CurrentMode +  '&ShowUses=' + ShowUses + '&ShowImplied=' + ShowImplied + '&View=All&Tab=' + Tab + '">Combined View</a>	'	
else:
	print '<td style="text-align:right"><a target=_top href="/System/CGI-Executables/main_gui.cgi?Path=' + Path + '&Summary=' + Summary + '&Fragment=' + Fragment + '&Representation=' + Representation + '&Mode=' + CurrentMode +  '&ShowUses=' + ShowUses + '&ShowImplied=' + ShowImplied + '&View=Tab&Tab=' + Tab + '">Split View</a></span>'
print '&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp'
print '</td>'

print '<td align="right">'

if View == 'Tab':
	if Tab == 'Links':
		print '<a target=_top href="/System/CGI-Executables/main_gui.cgi?Path=' + Path + '&Summary=' + Summary + '&Fragment=' + Fragment + '&Representation=' + Representation + '&Mode=' + CurrentMode +  '&ShowUses=' + ShowUses + '&ShowImplied=' + ShowImplied + '&View=Tab&Tab=Representation">The Thing Itself</a>'
	else:
		print '<span style="color: #696969;">The Thing Itself</span>'
	print '&nbsp&nbsp or &nbsp&nbsp'

print 'Dependencies:&nbsp'
for mode in Modes.keys():
	if mode != CurrentMode or (View == 'Tab' and Tab == 'Representation'):
		print '<a target=_top href="/System/CGI-Executables/main_gui.cgi?Path=' + Path + '&Summary=' + Summary + '&Fragment=' + Fragment + '&Representation=' + Representation + '&Mode=' + mode + '&ShowUses=' + ShowUses + '&ShowImplied=' + ShowImplied + '&View=' + View + '&Tab=Links"/>' +  Modes[mode] + '</a>'
	else:
		print '<span style="color: #696969;">' + Modes[CurrentMode] + '</span>'
	print '&nbsp'

print '&nbsp'

if ShowUses != 'Yes':
	print '<a target=_top href="/System/CGI-Executables/main_gui.cgi?Path=' + Path + '&Summary=' + Summary + '&Fragment=' + Fragment + '&Representation=' + Representation + '&Mode=' + CurrentMode + '&ShowUses=Yes&ShowImplied=' + ShowImplied + '&View=' + View + '&Tab=Links"/>Show Uses</a>'
else:
	print '<a target=_top href="/System/CGI-Executables/main_gui.cgi?Path=' + Path + '&Summary=' + Summary + '&Fragment=' + Fragment + '&Representation=' + Representation + '&Mode=' + CurrentMode + '&ShowUses=No&ShowImplied=' + ShowImplied + '&View=' + View + '&Tab=Links"/>Hide Uses</a>'

print '&nbsp'	

if ShowImplied != 'Yes':
	print '<a target=_top href="/System/CGI-Executables/main_gui.cgi?Path=' + Path + '&Summary=' + Summary + '&Fragment=' + Fragment + '&Representation=' + Representation + '&Mode=' + CurrentMode + '&ShowUses=' + ShowUses + '&View=' + View + '&Tab=Links&ShowImplied=Yes"/>Show Implied</a>'
else:
	print '<a target=_top href="/System/CGI-Executables/main_gui.cgi?Path=' + Path + '&Summary=' + Summary + '&Fragment=' + Fragment + '&Representation=' + Representation + '&Mode=' + CurrentMode + '&ShowUses=' + ShowUses + '&View=' + View + '&Tab=Links&ShowImplied=No"/>Hide Implied</a>'
print '</td>'

print '</tr></table>'
print '</BODY></HTML>'
