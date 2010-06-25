import os
import shutil

def MakeDir(DirName,creates = ()):
	'''
	is a "strong" directory maker -- if DirName already exists, this deletes it first 
	'''
	if os.path.exists(DirName):
		delete(DirName)
		os.mkdir(DirName)

def delete(ToDelete):
	'''
	unified "strong" version of delete that uses os.remove for a file 
	and shutil.rmtree for a directory tree
	'''
	if os.path.isfile(ToDelete):
		os.remove(ToDelete)
	elif os.path.isdir(ToDelete):
		shutil.rmtree(ToDelete)

def PathExists(ToCheck):
	'''
		convenient name for os function
		
		The reason it's done this way as opposed to merely setting 
			PathExists = os.path.exists
		in this module is that this will disturb the system i/o intercept because this module needs to be execfiled FIRST before system_io_override. 
		
	'''
	return os.path.exists(ToCheck)


def IsFile(ToCheck):
	'''
		convenient name for os function
		
		The reason it's done this way as opposed to merely setting 
			PathExists = os.path.exists
		in this module is that this will disturb the system i/o intercept 
		because this module needs to be execfiled FIRST before system_io_override. 
		
	'''
	return os.path.isfile(ToCheck)
	
def IsDir(ToCheck):
	'''
		convenient name for os function
		
		The reason it's done this way as opposed to merely setting 
			PathExists = os.path.exists
		in this module is that this will disturb the system i/o intercept because this
		module needs to be execfiled FIRST before system_io_override. 
		
	'''
	return os.path.isdir(ToCheck)
	
def is_string_like(obj):
    """
    Check whether input object behaves like a string.

    From:  _is_string_like in numpy.lib._iotools

    **Parameters**

        **obj** :  string or file object

                Input object to check.

    **Returns**

        **out** :  bool

                Whether or not `obj` behaves like a string.

    """
    try:
        obj + ''
    except (TypeError, ValueError):
        return False
    return True
    
def is_num_like(obj):

    try:
        obj + 0
    except (TypeError, ValueError):
        return False
    return True    
    
    
def ListUnion(ListOfLists):
	'''
	takes python list of python lists
	
	[[l11,l12, ...], [l21,l22, ...], ... , [ln1, ln2, ...]] 
	
	and returns the aggregated list 
	
	[l11,l12, ..., l21, l22 , ...]
	'''
	u = []
	for s in ListOfLists:
		if s != None:
			u.extend(s)
	return u


def rgetattr(r,a):
	for aa in a:
		if aa in r.keys():
			r = r[aa]
		else:
			return None
	return r
	
def Rgetattr(r,a):
	for aa in a:
		if hasattr(r,aa):
			r = getattr(r,aa)
		else:
			return None
	return r


	
def rhasattr(r,a):
	for aa in a:
		if aa in r.keys():
			r = r[aa]
		else:
			return False
	return True

	
def dictListUniqify(D):
	D = uniqify([tuple(x.items()) for x in D])
	return [dict(d) for d in D]
	
	
def Rhasattr(r,a):
	for aa in a:
		if hasattr(r,aa):
			r = getattr(r,aa)
		else:
			return False
	return True

def uniqify(seq, idfun=None): 
	'''
	Relatively fast pure python uniqification function that preservs ordering
	ARGUMENTS:
		seq = sequence object to uniqify
		idfun = optional collapse function to identify items as the same
	RETURNS:
		python list with first occurence of each item in seq, in order
	'''
	

	if idfun is None:
		def idfun(x): return x
	seen = {}
	result = []
	for item in seq:
		marker = idfun(item)
		if marker in seen: continue
		seen[marker] = 1
		result.append(item)

	return result
		
def listdir(ToList):
	'''
		convenient name for os function
		
		The reason it's done this way as opposed to merely setting 
			PathExists = os.path.exists
		in this module is that this will disturb the system i/o intercept
		because this module needs to be execfiled FIRST before system_io_override. 
		
	'''
	return os.listdir(ToList)
	
import random
def createCertificate(path,msg,tol=10000000000):
	F = open(path,'w')
	F.write(msg + ' Random certificate: ' + str(random.randint(0,tol)))
	F.close()

def Flatten(L):
	S = []
	for l in L:
		if isinstance(l,list) or isinstance(l,tuple):
			S += Flatten(l)
		else:
			S.append(l)
	return S
