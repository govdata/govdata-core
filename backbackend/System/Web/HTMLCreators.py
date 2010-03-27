'''
Convenience functions for html templates
'''

from __future__ import division
import os
from System.Utils import TemplateInstance

def MakeHTMLWithFrameChooser(PageTitle,ChooseBetweenFiles,ChooserLabelList,outpath): 

	s = '<html>\n<body>\n' 
	s += '\n'.join(['<a href="' + ChooseBetweenFiles[i] + '" target="VariableSegment">' + ChooserLabelList[i] + '</a>' for i in range(len(ChooseBetweenFiles))])
	s += '\n</body>\n</html>'
	
	lcpath = outpath + '.LinkChoice.html'
	F = open(lcpath,'w')
	F.write(s)
	F.close()

	MakeHTMLwithFrames([lcpath.split('/')[-1],ChooseBetweenFiles[0]], outpath, PageTitle, segmentnames = [None, 'VariableSegment'], percentages = [.05,.95])
	

def MakeZgrviewerAppletFile(title, svgpath,outpath,target='_blank',depends_on=('../Operations/Web/zgrviewerAppletTemplate.pyt',)):
	X = os.environ
	zvtmpath = X['DataEnvironmentDirectory'] + 'System/zgrviewer/zvtm-0.9.6-SNAPSHOT.jar'
	zgrpath = X['DataEnvironmentDirectory'] + 'System/zgrviewer/zgrviewer-0.8.1.jar'
	absolutesvgpath = X['DataEnvironmentDirectory'] + '/'.join(svgpath.split('/')[1:])
	TemplateInstance('../Operations/Web/zgrviewerAppletTemplate.pyt',outpath,TITLE=title,ZVTMPATH=zvtmpath,ZGRPATH=zgrpath,SVGPATH = absolutesvgpath,TARGET=target)


def MakeHTMLwithFrames(inpaths,outpath,pagetitle,segmentnames = None, percentages=None):
	headerlines = ['<HTML>','<HEAD>','<TITLE>' + pagetitle + '</TITLE>','</HEAD>']
	header = '\n'.join(headerlines)
	
	if percentages == None:
		percentages = [1/len(inpaths)] * len(inpaths) 
	if segmentnames == None:
		segmentnames = [None]*len(inpaths)
		
	FramesetLine1 = '<FRAMESET ROWS ="' + ','.join([str(100*x) + '%' for x in percentages])   + '">'
	FrameLines = ['\t<FRAME SRC="' + segmentpath + '"' + (', name = ' + segmentname if segmentname != None else '') + '>' for (segmentpath,segmentname) in zip(inpaths,segmentnames)]
	Frames = FramesetLine1 + '\n' + '\n'.join(FrameLines)
	
	NoFramesLines = ['\t<NOFRAMES>', '\t<H1>No Frames? Fuck You!</H1>','</FRAMESET>']
	NoFrames = '\n'.join(NoFramesLines)
	
	footer = '</HTML>'

	FileText = header + '\n' + Frames + '\n' + NoFrames + '\n' + footer
	
	F = open(outpath,'w')
	F.write(FileText)
	F.close()

