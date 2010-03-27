#!/usr/bin/python

'''
dynamically generated file system browser
'''

import sys, cgi, cgitb, os
cgitb.enable()
print 'Content-Type: text/html\n\n'

os.chdir('../../Temp')

if os.path.exists('../System/initialize_GUI'):
	execfile('../System/initialize_GUI')

	from System.Utils import *
	import System.SystemGraphOperations
	from System.MetaData import metadatapath, opmetadatapath

	os.chdir('../System/CGI-Executables/')

	IFBpath = '/System/CGI-Executables/IntegrateFileBrowser.cgi?Path='
	MGpath = '/System/CGI-Executables/main_gui.cgi?'
	
	Variable = cgi.FieldStorage()
	if Variable.has_key('Path'):
		Path = Variable['Path'].value
		if Path[-1] != '/':
			Path = Path + '/'
	else:
		Path = '../'
	if Variable.has_key('Mode'):
		Mode = Variable['Mode'].value
	else:
		Mode = 'ColDir'
	if Variable.has_key('ShowUses'):
		ShowUses = Variable['ShowUses'].value
	else:
		ShowUses = 'No'
	if Variable.has_key('ShowImplied'):
		ShowImplied = Variable['ShowImplied'].value
	else:
		ShowImplied = 'No'	
	
	#Path = 'big'
	
	ignore = ['.DS_Store','.svn','.pyc','__init__.py']
	RealPath = '../' + Path
	ImageExtensions = [('png','jpg.gif'),('jpeg','jpg.gif'),('bmp','jpg.gif'),('jpg','jpg.gif'),('gif','gif.gif'),('zip','archive.png'),('rar','archive.png'),('exe','exe.gif'),('setup','setup.gif'),('txt','text.png'),('htm','html.gif'),('html','html.gif'),('fla','fla.gif'),('swf','swf.gif'),('xls','xls.gif'),('doc','doc.gif'),('sig','sig.gif'),('fh10','fh10.gif'),('pdf','pdf.gif'),('psd','psd.gif'),('rm','real.gif'),('mpg','video.gif'),('mpeg','video.gif'),('mov','video2.gif'),('avi','video.gif'),('eps','eps.gif'),('gz','archive.png'),('asc','sig.gif'),('dot','graphviz_small.png'),('tsv','data.png'),('csv','data.png'),('tex','tex.png'),('py','python.png')]
	
	Images = dict([(a,'dlf/' + b) for (a,b) in ImageExtensions])
	
	print '<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">'
	print '<html xmlns="http://www.w3.org/1999/xhtml">'
	print '<html>'
	print '<head>'
	print '<meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1" />'
#	print '<title>Directory Listing of ' + Path + '</title>'
	print '<link rel="stylesheet" type="text/css" href="dlf/styles.css" />'
	print '</head>'
	print '<body>'
	print '<div id="container">'
#	print '<h1>Directory Listing of ' + Path + '</h1>'
	print '<div id="breadcrumbs">'
	PathSplit = Path.split('/')
	subpaths = ['/'.join(PathSplit[:j]) for j in range(1,len(PathSplit))]
	
	for i in range(len(subpaths)):
		print '<a href="' + MGpath + 'Path=' + subpaths[i] + '&Summary=' + metadatapath(subpaths[i]) + '&Fragment=' + subpaths[i] + '&Representation=' + subpaths[i] + '&Mode=' + Mode + '&ShowUses=' + ShowUses +'&ShowImplied=' + ShowImplied + '" target = _top > ' + PathSplit[i] + '</a> >'
	
	print '</div>'
	
	print '<div id="listingcontainer">'
	print '<div id="listingheader">'
#	print '<div id="headerfile">File</div><br />'
	print '</div>'
	print '<div id="listing">'
	
	if len(subpaths) > 1:
		print '<div><a href="' +  MGpath + 'Path=' + subpaths[-2] + '&Summary=' + metadatapath(subpaths[-2]) + '&Fragment=' + subpaths[-2] + '&Representation=' + subpaths[-2] + '&Mode=' + Mode + '&ShowUses=' + ShowUses +'&ShowImplied=' + ShowImplied + '" target = _top <img src="dlf/AquaUpSmall.png" alt="Folder" /><strong>..</strong></a></div><br />'
	
	if len(subpaths) > 0:
		print '<div><a href="' +  MGpath + 'Path=' + subpaths[-1] + '&Summary=' + metadatapath(subpaths[-1]) + '&Fragment=' + subpaths[-1] + '&Representation=' + subpaths[-1] + '&Mode=' + Mode + '&ShowUses=' + ShowUses +'&ShowImplied=' + ShowImplied + '" target = _top <img src="dlf/AquaSmall.png" alt="Folder" /><strong>.</strong></a></div><br />'
	
	if PathExists(RealPath):
		List = os.listdir(RealPath)
		List.sort()
		Dirs = [x for x in List if os.path.isdir(RealPath + x) and x not in ignore]
		Files = [x for x in List if os.path.isfile(RealPath + x) and x not in ignore]
		
		for dir in Dirs:
			newpath = Path+dir + '/'
			if dir.split('.')[-1] == 'data':
				print '<div><a href="' + MGpath + 'Path=' + newpath + '&Summary=' + metadatapath(newpath) + '&Fragment=' + newpath + '&Representation=' + newpath + '&Mode=' + Mode + '&ShowUses=' + ShowUses + '&ShowImplied=' + ShowImplied + '" target = _top <img src="dlf/data.png" alt = "' + dir + '" />' + dir + '</a></div><br />'
			else:
				print '<div><a href="'  + MGpath + 'Path=' + newpath + '&Summary=' + metadatapath(newpath) + '&Fragment=' + newpath + '&Representation=' + newpath + '&Mode=' + Mode +  '&ShowUses=' + ShowUses + '&ShowImplied=' + ShowImplied + '" target = _top <img src="dlf/AquaSmall.png" alt = "' + dir + '" />' + dir + '</a></div><br />'
		
		for i in range(len(Files)):
			file = Files[i]
			extension = file.split('.')[-1]
			newpath = Path+file
			imagepath = Images[extension] if extension in Images.keys() else 'dlf/unknown.png'
			thumb = '<span><img src="dlf/trans.gif" alt="' + file + '" name="thumb' + str(i) + '" /></span>'
			if extension != 'pyc':
				print '<div><a href="' + MGpath + 'Path=' + Path + '&Summary=' + metadatapath(newpath) + '&Fragment=' + newpath + '&Representation=' + newpath +  '&Mode=' + Mode +  '&ShowUses=' + ShowUses + '&ShowImplied=' + ShowImplied + '" target = _top <img src = "' + imagepath + '" alt = "' + file + '" />' + file + '</a></div><br />'
	else:
		print 'This paths appears not to exist in the filesystem.'
	
	print '</div></div></body></html>'

else:
	print 'No GUI File configured (it should be at ../System/initialize_GUI)'
	print '</body></html>'
