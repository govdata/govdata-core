#!/usr/bin/python

'''
produces mode chooser bar at top of GUI
'''

import cgi,os, platform
# Required header that tells the browser how to render the HTML.
print 'Content-Type: text/html\n\n'

os.chdir('../../Temp')

if os.path.exists('../System/initialize_GUI'):
	execfile('../System/initialize_GUI')
	import tabular as tb
	import xml.etree.ElementTree as ET
	E = os.system('SGE_ROOT=/opt/sge6 SGE_QMASTER_PORT=63231 qstat -ext -xml > qstat.xml')
	qstat = ET.parse('qstat.xml')
	JL = qstat.findall('queue_info')[0].findall('job_list')
	if len(JL) > 0:
		KV = [[(l.tag,l.text if l.text != None else 'None') for l in jl.getchildren()] for jl in JL]
		T = tb.tabarray(kvpairs=KV)
		s = 'Running jobs:<br/>' + tb.web.tabular2html(X=T,returnstring=True,writecss=False)
	else:
		s = 'Nothing running.'
        JL = qstat.findall('job_info')[0].findall('job_list')
	if len(JL) > 0:
                KV = [[(l.tag,l.text if l.text != None else 'None') for l in jl.getchildren()] for jl in JL]
		T = tb.tabarray(kvpairs=KV)
                s2 = 'Pending jobs:<br/>' + tb.web.tabular2html(X=T,returnstring=True,writecss=False)
		
        else:
                s2 = 'Nothing pending.'

print '<HTML><HEAD><TITLE>QSTAT</TITLE>'
print '<style type="text/css">'
print '.bleft {text-align: left}'
print '.bright {text-align: right}'
print '</style>'
print '</HEAD><BODY>'
print '<a href="/System/CGI-Executables/qstat.cgi">Reload</a><br/><br/>'
print s
print '<br/><br/>'
print s2
print '</BODY></HTML>'
