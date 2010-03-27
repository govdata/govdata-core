#!/usr/bin/python

'''
Dynamically generate summary meta data page in GUI
'''

from __future__ import division
import cgi, cgitb, os, time, numpy
cgitb.enable()


def GetCText(Cpath,ShowCFile,ShowEFile):
	if IsFile(Cpath):
		try: 
			Data = tb.tabarray(SVfile = Cpath,delimiter = ',', lineterminator='\n',verbosity=0)
		except:
			CText = 'Creation record won\'t open.'
		else:
			if len(Data) > 0:
				Data.sort(order=['TimeStamp'])
				times = len(Data); sincewhen = time.strftime('%Y:%m:%d:%H:%M:%S',time.gmtime(Data['TimeStamp'][0]))
				CText = 'This path has been operated on ' + str(times) + ' time(s) since ' + sincewhen + '.  '
				if 'Success' in Data['ExitType']:
					mostrecentsuccess = time.strftime('%Y:%m:%d:%H:%M:%S',time.gmtime(Data[Data['ExitType'] == 'Success']['TimeStamp'][-1]))
					mostrecentcreator =  Data[Data['ExitType'] == 'Success']['Operation'][-1]
					CText += 'It was last successfully created, by operation '  + mostrecentcreator + ', at ' + mostrecentsuccess + '.'
				else:
					CText += 'Path has never been successfully created.  '			
				
				if ShowCFile == 'Yes':
					CText += '&nbsp <a href="/System/CGI-Executables/SummaryMetaData.cgi?Summary=' + MetaPath + '&Fragment=' + Fragment + '&ShowCFile=No&ShowEFile=' + ShowEFile + '"/>Hide Creation Record</a>'
					CText += tb.web.tabular2html(SVfile=Cpath,verbosity=0,returnstring=True)
				else:
					CText += '&nbsp <a href="/System/CGI-Executables/SummaryMetaData.cgi?Summary=' + MetaPath + '&Fragment=' + Fragment +  '&ShowCFile=Yes&ShowEFile=' + ShowEFile + '"/>Show Full Creation Record</a>'
								
			else:
				CText = 'Creation record empty.'
	else:
		CText = 'No creation record has been made for this file.'

	return CText
		
def GetEText(Epath,ShowCFile,ShowEFile):		
	if IsFile(Epath):
		try: 
			Data = tb.tabarray(SVfile = Epath,delimiter = ',', lineterminator='\n',verbosity=0) 
		except:
			EText = 'Exit Status record won\'t open.'
		else:
			if len(Data) > 0:
				Data.sort(order=['TimeStamp'])
				times = len(Data); sincewhen = time.strftime('%Y:%m:%d:%H:%M:%S',time.gmtime(Data['TimeStamp'][0]))
				EText = 'As an operation, it has been called ' + str(times) + ' time(s) since ' + sincewhen + '.  '
				
				numberofsuccesses = len(Data[Data['ExitType'] == 'Success']) 
				if numberofsuccesses > 0:
					mostrecentsuccess = time.strftime('%Y:%m:%d:%H:%M:%S',time.gmtime(Data[Data['ExitType'] == 'Success']['TimeStamp'][-1]))					
					numberoffailures = len(Data[Data['ExitType'] == 'Failure'])
					if numberofsuccesses + numberoffailures > 1:
						percentage = str(int(100*numberofsuccesses/(numberofsuccesses + numberoffailures)))
					else:
						percentage = ''

					EText += 'It was last successfully called at ' + mostrecentsuccess + '.  ' + ('It has succeeded ' + percentage + '% of the times its been called.  ' if percentage != '' else '')
					averagetime = str(numpy.mean(Data[Data['ExitType'] == 'Success']['Runtime']))
					EText += 'On average, operation takes ' + averagetime + ' second(s) to run.'
				else:
					mostrecentsuccess = None
					EText += 'Operation has never been successfully called.  '				
					
				if ShowEFile == 'Yes':
					EText += '&nbsp <a href="/System/CGI-Executables/SummaryMetaData.cgi?Summary=' + MetaPath + '&Fragment=' + Fragment + '&ShowCFile=' + ShowCFile + '&ShowEFile=No"/>Hide Exit Status Record</a>'
					EText += tb.web.tabular2html(SVfile=Epath,verbosity=0,returnstring=True)
				else:
					EText += '&nbsp <a href="/System/CGI-Executables/SummaryMetaData.cgi?Summary=' + MetaPath + '&Fragment=' + Fragment +  '&ShowCFile=' + ShowCFile + '&ShowEFile=Yes"/>Show Full Exit Status Record</a>'		
					
			else:
				EText = 'Exit Status record empty.'

	else:
		EText = None

	return EText

os.chdir('../../Temp')
print 'Content-Type: text/html; charset=utf-8\n\n'

if os.path.exists('../System/initialize_GUI'):
	execfile('../System/initialize_GUI')
	import tabular as tb
	from System.Utils import IsFile
	from System.MetaData import ProcessMetaData,MakeAutomaticMetaData
	
	Variable = cgi.FieldStorage()
	if Variable.has_key('Summary'):
		MetaPath = Variable['Summary'].value
		Cpath = MetaPath + '/CreationRecord.csv'
		Epath = MetaPath + '/ExitStatusFile.csv'
		Spath = MetaPath + '/MetaDataSummary.html'

	if Variable.has_key('Fragment'):
		Fragment = Variable['Fragment'].value
		
		if Variable.has_key('ShowCFile'):
			ShowCFile = Variable['ShowCFile'].value
		else:
			ShowCFile = 'No'
		if Variable.has_key('ShowEFile'):
			ShowEFile = Variable['ShowEFile'].value
		else:
			ShowEFile = 'No'		
			
		if Variable.has_key('reprocess'):
			reprocess = Variable['reprocess'].value
		else:
			reprocess = 'No'
		if reprocess=='Yes':
			ProcessMetaData(MetaPath,objname=Fragment)
		if Variable.has_key('reautomatic'):
			reauto = Variable['reautomatic'].value
		else:
			reauto = 'No'
		if reauto=='Yes':
			MakeAutomaticMetaData(Fragment,forced=True)

		
		if IsFile(Spath):
			SText = open(Spath,'r').read()
		else:
			SText = ''
		
		
		
		CText = GetCText(Cpath,ShowCFile,ShowEFile)
		EText = GetEText(Epath,ShowCFile,ShowEFile)
				
		print '<html><META HTTP-EQUIV="Content-Type" '
                'CONTENT="text/html; charset=utf-8" /><head><title>Summary MetaData</title></head><body>\n'
		print '<strong>Metadata summary for ' + Fragment + '</strong> &nbsp&nbsp <a href="/System/CGI-Executables/SummaryMetaData.cgi?Summary=' + MetaPath + '&Fragment=' + Fragment + '&reprocess=Yes"> Reprocess MetaData</a> &nbsp&nbsp <a href="/System/CGI-Executables/SummaryMetaData.cgi?Summary=' + MetaPath + '&Fragment=' + Fragment + '&reautomatic=Yes"> Regenerate Automatic MetaData</a><br/><br/>'

		print SText
		if CText != None:
			print '<br/><br/>'
			print CText
		if EText != None:
			print '<br/><br/>'

			print EText			
		print '</body></html>'			
	else:
		print 'No summary path specified.'
else:
	print 'No GUI File configured (it should be located at ../System/initialize_GUI)'
	
	
