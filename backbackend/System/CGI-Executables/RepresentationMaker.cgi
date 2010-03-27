#!/usr/bin/python


'''
cgi script to dynamically generate file representation
Called by main_gui.cgi and from links in fragment_maker.cgi
The heart is a call to System.Web.WebRepresentations.MakeHTMLRepresentationFast
'''
import time
import cgi, os, platform
import cgitb; cgitb.enable()

def RedirectPage(RedirectTo):
	Link = '<a href = "' + RedirectTo + '">here</a>'	
	return '<html>\n<head>\n<script type="text/javascript">\n<!--\nwindow.location = "' + RedirectTo + '"\n//-->\n</script></head><body>If you can see this page, a redirect has failed. (Is JavaScript enabled on your browser?)  Click ' + Link + '.\n</body></html>'
	
	
print 'Content-Type: text/html; charset=utf-8\n\n'
os.chdir('../../Temp')


if os.path.exists('../System/initialize_GUI'):

	execfile('../System/initialize_GUI')
	import sys
	from System.Utils import PathExists,IsDir,MakeDirs
	import System.Web.WebRepresentations
	from System.MetaData import metadatapath
	Variable = cgi.FieldStorage()
	if Variable.has_key('Representation'):
		Path = Variable['Representation'].value
	else:
		Path = '../'
		
		
	if 'OpenFile' in Variable.keys():
		OpenFile = Variable['OpenFile'].value
	else:
		OpenFile = 'No'
			
	PathBase = Path.split('@')[0]
	metapathdir = metadatapath(PathBase) 
	if not PathExists(metapathdir):
		MakeDirs(metapathdir)
	[metapath,redirect] = System.Web.WebRepresentations.MakeHTMLRepresentationFast(PathBase,metapathdir,OpenFile=OpenFile)	
	
	if PathExists(metapath):
		if redirect or '@' in Path:
			PathAnchor = Path[Path.find('@')+1:] if '@' in Path else ''
			RedirectTo = '/' + metapath[3:] + '#' + PathAnchor
			print RedirectPage(RedirectTo)
		else:
			#print metapath
			print open(metapath).read()
	else:
		print '<HTML><BODY>\n'
		if IsDir(Path):
			if (platform.platform().startswith('Darwin') or platform.mac_ver()[0] != ''):
				command = 'open ' + Path
				theword = 'finder'
			elif platform.platform().startswith('Windows') or platform.platform().startswith('CYGWIN'):
				command = 'explorer ' + Path.replace('/',r'\\')
				theword = 'Windows Explorer'
			else:
				command = ''
				theword = ''
			if OpenFile == 'Yes' and command:
				os.system(command)
			if theword:
				print Path +  ' is a directory.  To open the directory in the ' + theword + ', <a href="/System/CGI-Executables/RepresentationMaker.cgi?Representation=' + Path + '&OpenFile=Yes">click here.</a>'
			else:
				print Path +  ' is a directory.  However, your system has no connection to a local file browser.'
		else:
			print 'No Representation for ' + Path + '\n'
		print '</BODY></HTML>'
else:
	print 'No GUI File configured (it should be located at ../System/initialize_GUI)'
