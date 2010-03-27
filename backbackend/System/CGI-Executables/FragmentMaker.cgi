#!/usr/bin/python


'''
Produces file dependency graph dynamically for use in browser GUI

The heart is a call to System.SystemGraphOperations.MakeLocalLinkGraph.

Aside from that, the only nontrivial thing done here is to determine which viewer to view the graph with.  
If the graph has a large width (e.g. ratio of n/d >= GRAPHISWIDE, where n = # of nodes and 
d = max-all_pairs_shortest_path_length averaged over nodes)  or is large (n >= GRAPHISBIG), 
the ZGRviewer applet is used; else the svg of the graph is directly displayed in the browser. 
[The motivation of this is that opening the ZGRviewer applet takes a long time, much longer 
than the calculation of the graph width; so for small graphs where the ZGRviewer is unnecessary, we don't use it.] 
'''

import time
TT = time.time()
import sys, cgi, cgitb, os
cgitb.enable()

print 'Content-Type: text/html \n\n'
os.chdir('../../Temp')

GRAPHISBIG = 40
GRAPHISWIDE = 5
GRAPHISTINY = 7
zvtmpath = 'zgrviewer/zvtm-0.9.8.jar'
zgrpath = 'zgrviewer/zgrviewer-0.8.2.jar'	

if os.path.exists('../System/initialize_GUI'):
	execfile('../System/initialize_GUI')
	import networkx
	
	from System.Utils import *
	import System.SystemGraphOperations
	from System.MetaData import metadatapath, opmetadatapath
	Variable = cgi.FieldStorage()
	if Variable.has_key('Fragment'):
		Path = Variable['Fragment'].value
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
	T = time.time()
	[message,metapath,GG,linkpathhtml] = System.SystemGraphOperations.MakeLocalLinkGraph(Path,Mode,ShowUses,ShowImplied)	


	if PathExists(metapath) and message == '' and GG != None:
		[Nodes,Edges,NodeAttrs,EdgesAttrs] = GG
		G = networkx.Graph() ; G.add_nodes_from(Nodes) ; G.add_edges_from(Edges)
	
		n = G.number_of_nodes()
		H = networkx.all_pairs_shortest_path_length(G)
		d = sum([max(h.values()) for h in H.values()])/n
		if ((n/d) < GRAPHISWIDE and n < GRAPHISBIG) or n < GRAPHISTINY:
			print '<HTML><BODY>'
			print 'Local Rule Graph for: ' + Path + ' &nbsp&nbsp <a href="/' + linkpathhtml + '" /> Show Link List</a>'
			print '<embed src="' + '../' + metapath + '" width = "100%" height = "100%" type="image/svg+xml" pluginspage="http://www.adobe.com/svg/viewer/install/" />'
			#print '<iframe src="' + '../' + metapath + '" width = "100%" height="100%"/>'
			print '</BODY></HTML>' 	
		else:
			print '<html><head><meta http-equiv="Content-Type" content="text/html; charset=UTF-8"><META HTTP-EQUIV="Expires" CONTENT="Thu, 1 June 2000 23:59:00 GMT"><META HTTP-EQUIV="pragma" CONTENT="no-cache">'
			print '</head><body>'
			print '<a href="/' + linkpathhtml + '" /> Show Link List</a>'
			print '<applet name="zgr" code="net.claribole.zgrviewer.ZGRApplet.class" archive="' + zvtmpath + ',' + zgrpath + '" width="100%" height="100%">'
			print '<param name="type" value="application/x-java-applet;version=1.5" />'
			print '<param name="scriptable" value="true" />'
			print '<param name="mayscript" value="true" />'
			print '<param name="width" value="100%" />'
			print '<param name="height" value="100%" />' 
			print '<param name="svgURL" value="' + '../' + metapath + '" />'
			print '<param name="title" value="Local Rule graph for ' + Path + '" />'
			print '<param name="target" value="_top" />'
			print '<param name="appletBackgroundColor" value="#DDD" />'
			print '<param name="graphBackgroundColor" value="#DDD" />'
			print '<param name="highlightColor" value="red" />'
			print '<param name="antialiasing" value="false" />'
			print '</applet>'
			print '</body></html>'
	else:
		print '<HTML><BODY>\n'
		print message
		if linkpathhtml: print '<a href="/' + linkpathhtml + '" /> Show Link List</a>'
		print '</BODY></HTML>'
else:
	print 'No GUI File configured (it should be at ../System/initialize_GUI)'
