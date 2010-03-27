from __future__ import with_statement
import string, re , os
from os.path import splitext, basename
from System.Utils import DirName, delete, listdir

def GetNumLines(inpath):
	F = open(inpath,'r')
	i = 0
	for line in F:
		i += 1
	return i
	
	
def MakeTextRepresentation(inpath,outpath):
	'''
	Main text2html function. 
	inpath  = txt file to represent
	outpath = html file out
	
	if the size of the file is less than 50 MB, it's put into one HTML page
	if the file is > 50 MB its divided into multiple linked pages 
	(using the MultiPage function)
	'''

#	Dir = DirName(outpath[:-2])
#	L = [l for l in listdir(Dir) if 'WebRepresentation' in l and l.endswith('.html')]
#	for l in L:
#		delete(Dir + '/' + l)

	info = os.stat(inpath)
	size = info.st_size
	if size < 50000000:
		InText = open(inpath,'rU').read()
		OutText = '<HTML><strong>Text of ' + inpath + ' (<a href="/System/CGI-Executables/RepresentationMaker.cgi?Representation=' + inpath +'&OpenFile=Yes">Open File</a>):</strong><br/><br/>' + text2html(InText) + '</HTML>'
		F = open(outpath,'w')
		F.write(OutText)
		F.close()
	else:
		print 'This is a large file, may take a few moments to produce representation.'
		MultiPage(inpath,outpath)


def MultiPage(inpath,outpath):
	'''
	make multiple linked pages representing a large text file
	'''
    
  	NumLines = GetNumLines(inpath)
  	
 	LINES_PER_PAGE = 10000  #this sets the number of records displayed per .html page (if the DotData has more than ROWS_PER_PAGE rows, it will be split into multiple sections on several .html pages	
 	
 	numSections = int( NumLines/LINES_PER_PAGE )  + 1
	
	section2file = lambda sectionNum: outpath if sectionNum == 0 else splitext(outpath)[0] + str( sectionNum ) + splitext(outpath)[1]   #section2file(i) returns the name of the .html file corresponding to section number i 
  
  	F = open(inpath,'rU')
  	pagenum = 0
  	line = F.readline()
	while line != '':
		i = 0
		sectionHtmlFile = section2file(pagenum)  #get the name of output file for the section
		fromLine = pagenum * LINES_PER_PAGE   #beginning record number for this section
		toLine = min((pagenum + 1) * LINES_PER_PAGE,NumLines)  #ending record number for this section

		with open( sectionHtmlFile, 'w' ) as f:  

			f.write( '<html><META HTTP-EQUIV="Content-Type" CONTENT="text/html; charset=utf-8" /><head><title>' + 'Bork' + '</title></head><body>\n' )   
			f.write( '<p><strong>Text of ' + inpath + ' (<a href="/System/CGI-Executables/RepresentationMaker.cgi?Representation=' + inpath +'&OpenFile=Yes">Open File</a>):</strong> (page ' + str(pagenum+1) + ' of ' + str( numSections ) +  ', lines ' + str(fromLine+1) + ' - ' + str(toLine) + ')</p>')
	
			# write out hyperlinks to other sections
			prefix = DirName(outpath[2:]) + '/'
			
			f.write('<p>')
			if pagenum > 0: 
				f.write( '<a href="' + prefix +basename( section2file( pagenum-1 ) ) + '">prev</a> ' ) 
			if pagenum < numSections-1:
				f.write( ' <a href="' + prefix + basename( section2file(  pagenum+1 ) ) + '">next</a> ' ) 
			f.write( '&nbsp &nbsp page ' )	
			for page in range( numSections ):
				f.write( ( ' <a href="' + prefix + basename( section2file( page ) ) + '">' + str( page+1 ) + '</a>' ) if page != pagenum else ' ' + str( page+1 ) )
			f.write( '</p>' )

			while (i < LINES_PER_PAGE) and line != '':   #for each section	
				htmlline = text2html(line)
				htmlline = htmlline.replace('<PRE>','').replace('</PRE>','')
				if htmlline.startswith('<br />') and htmlline.endswith('<br />') and htmlline.count('<br />') > 1:
					htmlline = htmlline[:-6]
				f.write(htmlline)
				i += 1
				line = F.readline()

			# write out hyperlinks to other sections
			f.write('<p>')
			if pagenum > 0: 
				f.write( '<a href="' + prefix +basename( section2file( pagenum-1 ) ) + '">prev</a> ' ) 
			if pagenum < numSections-1:
				f.write( ' <a href="' + prefix + basename( section2file(  pagenum+1 ) ) + '">next</a> ' ) 
			f.write( '&nbsp &nbsp page ' )	
			for page in range( numSections ):
				f.write( ( ' <a href="' + prefix + basename( section2file( page ) ) + '">' + str( page+1 ) + '</a>' ) if page != pagenum else ' ' + str( page+1 ) )
			f.write( '</p>' )
			
			f.write( '</body></html>\n' )
		pagenum += 1


def translate(text, pre=0):
    translate_prog = prog = re.compile(r'\b(http|ftp|https)://\S+(\b|/)|\b[-.\w]+@[-.\w]+')
    i = 0
    list = []
    while 1:
        m = prog.search(text, i)
        if not m:
            break
        j = m.start()
        list.append(escape(text[i:j]))
        i = j
        url = m.group(0)
        while url[-1] in '();:,.?\'"<>':
            url = url[:-1]
        i = i + len(url)
        url = escape(url)
        if not pre:
            if ':' in url:
                repl = '<A HREF="%s">%s</A>' % (url, url)
            else:
                repl = '<A HREF="mailto:%s">&lt;%s&gt;</A>' % (url, url)
        else:
            repl = url
        list.append(repl)
    j = len(text)
    list.append(escape(text[i:j]))
    return string.join(list, '')


def escape(s):
    s = string.replace(s, '&', '&amp;')
    s = string.replace(s, '<', '&lt;')
    s = string.replace(s, '>', '&gt;')
    return s

def escapeq(s):
    s = escape(s)
    s = string.replace(s, '"', '&quot;')
    return s
    
def emphasize(line):
    return re.sub(r'\*([a-zA-Z]+)\*', r'<I>\1</I>', line)
    
def text2html(body):
    res = []
    pre = 0
    raw = 0
    for line in string.split(body, '\n'):
        tag = string.lower(string.rstrip(line))
        if tag == '<html>':
            raw = 1
            continue
        if tag == '</html>':
            raw = 0
            continue
        if raw:
            res.append(line)
            continue
        if not string.strip(line):
            if pre:
                res.append('</PRE>')
                pre = 0
            else:
                res.append('<P>')
        else:
            if line[0] not in string.whitespace:
                if pre:
                    res.append('</PRE>')
                    pre = 0
            else:
                if not pre:
                    res.append('<PRE>')
                    pre = 1
            if '/' in line or '@' in line:
                line = translate(line, pre)
            elif '<' in line or '&' in line:
                line = escape(line)
            if not pre and '*' in line:
                line = emphasize(line)
            res.append(line)
    if pre:
        res.append('</PRE>')
        pre = 0
    return string.join(res,'<br />')


