from System.LinkManagement import PropagateUpThroughLinkGraph, LinksFromOperations
from System.Utils import *
import numpy

def Extract(Seed,ToName):
	'''
	Given a seed path (or list of paths), extracts out all raw data in that path, 
	as well as scripts necessary to produce downstream computed data from that raw data.   
	
	The extracted object is just a single "top-level directory" containing files and 
	directories copied from the file system; the directory substructure within the 
	top-level directory replicates the directory structure of the original files. 
	
	Seed = path or list of paths given as a string, or comma-separated list of path strings, 
	or python list of strings.  E.g.:
				'../Data/Dan_Data/RandomData/' 
		or		'../Data/Dan_Data/RandomData/,../Data/Dan_Data/NPR_Puzzle_Solutions/'
		or		['../Data/Dan_Data/RandomData/', '../Data/Dan_Data/NPR_Puzzle_Solutions/']
	
	ToName = Name of top-level directory where extracted files will be copied.   
	This function creates the path ToName if it doesn't exist, or overwrites it if it does. 
		
	'''

	FilesToCopy,AllRelevantFilesExist = Raw(Seed)
	
	#System looks to see if all raw data files indicated by links in LinkList actually exist, and gives option to stop extract if not.
	if not AllRelevantFilesExist:
		go_on = input('Extract anyway?')
	else:
		go_on = True
		
	if go_on:
		CopyOut(FilesToCopy,ToName)
	else:
		print 'Aborting.'
	
	

def Integrate(FolderToIntegrate):
	'''
	Suppose one has done an extraction (e.g., applied the Extract function) of some
	files in one data environment, and then taken that extracted directory and put it
	in the Temp directory of a new, different data environment.  This function is
	meant to integrate the extraction into the new file system so that it mirros the
	placement of the files in the original data environment.

	The top-level extracted directory is assumed to be in the "Temp" directory of
	the new data environment. This top-level directory contains a replica of a
	subportion of the original file system from which the extraction was done (see
	comments to Extract function). The Integrate function merely copies files from
	the extraction directory to the corresponding place in the filesystem. As it does
	so, it asks whether to continue if it determines that the path to which it would
	copy already exists in the new data environment.

	FolderToIntegrate = path name of top-level extraction directory. Required to be
	in "Temp".
	
	'''
	
	FolderToIntegrate = Backslash(FolderToIntegrate)
	
	for r in listdir(FolderToIntegrate):
		CopyIn(FolderToIntegrate + r, '../' + r)



def Raw(Seed):
	'''
	Given a seed path (or list of paths), determines a list of all raw (non-computed) 
	data files in that path,
	as well as scripts necessary to produce downstream computed data from that 
	raw data.  
	
	Seed = path or list of paths given as a string, or comma-separated list of path strings, 
	or python list of strings.  E.g.:
			'../Data/Dan_Data/RandomData/' 
	or		'../Data/Dan_Data/RandomData/,../Data/Dan_Data/NPR_Puzzle_Solutions/'
	or		['../Data/Dan_Data/RandomData/', '../Data/Dan_Data/NPR_Puzzle_Solutions/']
	
	
	Returns: [A,B] where 
	--A = list of path names in the data environment; this list contains all raw 
	(non-computed) data files required to produce the computed data files in Seed
	paths, as well as the paths of all scripts that need to be run to produce the 
	computed files.   
	--B = boolean which is True when all paths listed in A actually exist in the file system. 

	'''
	
	
	if isinstance(Seed,str):
		Seed = Seed.split(',')
	
	AllOpFiles = [r for r in RecursiveFileList('../Operations/') if r.split('.')[-1] == 'py']
	LinkList = LinksFromOperations(AllOpFiles)

	P = PropagateUpThroughLinkGraph(Seed,LinkList)
	
	TargetFiles = Union([set(l['TargetFile']) for l in P])
	CreateTargetFiles = Union([set(l[(l['LinkType'] == 'CreatedBy') | (l['LinkType'] == 'CreatedFrom')]['TargetFile']) for l in P])
	SourceFiles = Union([set(l['SourceFile']) for l in P])
	ScriptFiles = Union([set(l['UpdateScriptFile']) for l in P])
	
	AllFiles = TargetFiles.union(SourceFiles).union(ScriptFiles).union(set(Seed))
	UntargetedFiles = [l for l in AllFiles if not any([PathAlong(l,k) for k in CreateTargetFiles])]
	TargetedFiles = [l for l in AllFiles if any([PathAlong(l,k) for k in CreateTargetFiles])]
	
	PE = numpy.array([PathExists(l) for l in UntargetedFiles])
	Unt = numpy.array(UntargetedFiles)

	status = all(PE)
	print Unt[PE == False]
	if not status:
		print 'The files ' + ','.join(Unt[PE == False].tolist()) + ' appear to be necessary for the indicated package but do not exist.'

	BigFileList = RecursiveFileList(UntargetedFiles)
	BadFileList = RecursiveFileList(TargetedFiles)
	FilesToCopy = set(BigFileList).difference(BadFileList)	

	return FilesToCopy,status



def CopyOut(FilesToCopy,ToName):
	'''
	Copies out files from one location to a "top-level target directory" 
	within the Temp Directory of the Data Environment, preserving the original 
	internal directory structure. 
	
	FilesToCopy  = List of files to copy, given as a python list of path names. 
	
	ToName = name of top-level directory to which the files in FilesToCopy will be 
	copied.   The top-level directory is given as a relative path, and will be placed in 
	the Temp directory of the data environment. 
	
	If ToName does it exist, this function creates it.  If it does exist, it will overwrite 
	it. 
	
	As files are copied into the ToName directory, original internal director
	structure is replicated, e.g. if a file File.txt has path ../A/File.txt relative to 
	Temp, then (if it doesn't yet exist in ToName), an empty directory called 
	ToName/A will be created, and the File.txt will compied to it. 
	'''

	ToName = Backslash(ToName)

	print 'copying', FilesToCopy
	print 'Making directory Temp/' + ToName
	MakeDir(ToName)
	
	for path in FilesToCopy:
		pathlist = path.split('/')[1:]
		for j in range(1,len(pathlist)):
			if not PathExists(ToName + '/'.join(pathlist[:j])):
				MakeDir(ToName + '/'.join(pathlist[:j]))
		strongcopy(path,ToName + '/'.join(pathlist))
	

def CopyIn(ToCopy,Target, overwrite=False):
	'''
	ToCopy = a path name. 
	Target = a path name
	overwrite = Boolean
	
	This function copies the contents of ToCopy to Target, preserving whatever file 
	structure is in ToCopy
	(e.g. just the file if it is a file, and the directories in it if it is a directory.)  
	
	If Target does not exist, it creates it.   If it already exists, CopyIn overwrites 
	Target if overwrite argument is True; 
	if overwrite argument is False, the function aborts. 
		
	'''

	if IsFile(ToCopy):
		if PathExists(Target):
			if not overwrite:
				print Target, 'already exists, not copying.'
			else:
				print Target, 'already exists, overwriting.'
				strongcopy(ToCopy,Target)
		else:
			strongcopy(ToCopy,Target)
	elif IsDir(ToCopy):
		ToCopy = Backslash(ToCopy)
		Target = Backslash(Target)
		if not PathExists(Target):
			strongcopy(ToCopy,Target)
		else:
			if IsDir(Target):
				for l in listdir(ToCopy):
					CopyIn(ToCopy + l, Target + l)
			else:
				if overwrite:
					print Target, 'already exists, but is not directory, overwriting.'
					strongcopy(ToCopy,Target)
				else:
					print Target, 'already exists and is not directory, not writing to it.'




