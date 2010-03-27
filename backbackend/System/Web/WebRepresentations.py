'''
Unified web representation caller.  Basically this strings together 
a number of different X2html functions for X = various file formats. 
'''

import System.Web.py2html, System.Web.text2html, subprocess
import tabular as tb
from System.Utils import *
from System.Storage import FindMtime
from System.MetaData import metadatapath
from traceback import print_exc
import cPickle
			
def MakeHTMLRepresentationFast(inpath,outpathdir,OpenFile = 'No', depends_on=('../System/Web/BasicHTMLTemplate',)):
	''' This looks at the path extension of a file and determines which
	(if any) web representation to apply to it.
	'''
	outpathdir += '/' if outpathdir[-1] != '/' else ''
	outpath = outpathdir + 'WebRepresentation.html'
	redirect = False
	
	if PathExists(inpath):
		extension = inpath.split('.')[-1].lower()
		
		if extension in ['csv','hsv','tsv','hsv/','txt']:
		
			if not PathExists(outpath) or FindMtime(inpath,Simple=False) > FindMtime(outpath):
				print 'Recomputing . . .'
				try:
					if extension in ['csv','tsv','txt']:
						tb.web.tabular2html(fname=outpath,SVfile=inpath,printheader = True,title=inpath)
					else:
						tb.web.tabular2html(fname=outpath,HSVfile=inpath,printheader = True,title=inpath)
				except:
					print 'Treating item as text file.\n'#
					System.Web.text2html.MakeTextRepresentation(inpath,outpath)
			else:
				print 'Not recomputing this ... <br/>'
			
		elif extension == 'py':
			X = os.environ
			
			if 'VersionOfPyDocs' in X.keys():
				Version = X['VersionOfPyDocs']
			else:
				Version = 'SIMPLE'
				
			if Version == 'EPYDOC':
				epydocdir = outpathdir + '__epydocs__/'  
				
				Remake = False
				if not IsDir(epydocdir):
					Remake = True
				else:
					LL = [epydocdir + x for x in os.listdir(epydocdir) if x.endswith('-pysrc.html')]
					if len(LL) == 0:
						Remake = True
					else:
						if FindMtime(inpath) > min([FindMtime(l) for l in LL]):
							Remake = True
				
				if Remake:
					Process = subprocess.Popen('epydoc --html -o ' + epydocdir + ' --parse-only ' + inpath,shell=True)
					Status = os.waitpid(Process.pid,0)
				else:
					print 'Not recomputing this ... <br/>'
					
				LL  = [epydocdir + x for x in os.listdir(epydocdir) if x.endswith('-pysrc.html')]
				outpath = LL[0]
				redirect = True
				FF = open('runtest.txt','w')
				FF.write(outpath)
				FF.close()

			else:
				if OpenFile == 'Yes':
					if 'Editor' in X.keys():
						editor = X['Editor']
						os.system(editor + ' ' + inpath)
					else:
						print 'No text editor command specified.'		
			
				System.Web.py2html.MakePyRepresentation(inpath,outpath)
				
		elif extension in ['tex']:
	
			if OpenFile == 'Yes':
				X = os.environ
				if 'Editor' in X.keys():
					editor = X['Editor']
					os.system(editor + ' ' + inpath)
				else:
					print 'No text editor command specified.'	

			test = not PathExists(outpath) or FindMtime(inpath) > FindMtime(outpath)
			if test:
				print 'Recomputing'
				System.Web.text2html.MakeTextRepresentation(inpath,outpath)

	
		elif extension in ['htm','html']:
			F = open(outpath,'w')
			F.write('<HTML><HEAD><meta HTTP-EQUIV="REFRESH" content="0; url=' + inpath[2:] + '"></HEAD></HTML>')
			F.close()
				
		elif extension in ['jpg', 'jpeg', 'png', 'gif']:
				F = open(outpath,'w')
				F.write('<HTML><BODY><IMG SRC="' + inpath[2:] + '"></IMG></BODY></HTML>')
				F.close()
				
		elif extension == 'pdf':

				F = open(outpath,'w')
				#F.write('<HTML><BODY><iframe src="' + '../' + inpath + '" width="100%" height="100%"/></BODY></HTML>')
				F.write('<HTML><BODY><embed src="' + inpath[2:] + '" type="application/pdf" width="100%" height="100%" /></BODY></HTML>')
				F.close()
		
		elif extension == 'pickle':
			if not PathExists(outpath) or FindMtime(inpath,Simple=False) > FindMtime(outpath):
				F = open(outpath,'w')
				S = cPickle.load(open(inpath,'rb'))
				F.write(repr(S))
				F.close()
				
		else:
			if PathExists(outpath):
				move_to_archive(outpath)
			
	else:
		TemplateInstance('../System/Web/BasicHTMLTemplate', outpath, bodytext = 'The path' + inpath + ' does not exist.')
	
	return [outpath,redirect]
	
	

