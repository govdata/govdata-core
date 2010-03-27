import os

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


def uniqify(seq, idfun=None): 
	'''
	Relatively fast pure python uniqification function that preservs ordering
	ARGUMENTS:
		seq = sequence object to uniqify
		idfun = optional collapse function to identify items as the same
	RETURNS:
		python list with first occurence of each item in seq, in order
	'''
	try:

		# order preserving
		if idfun is None:
			def idfun(x): return x
		seen = {}
		result = []
		for item in seq:
			marker = idfun(item)
			# in old Python versions:
			# if seen.has_key(marker)
			# but in new ones:
			if marker in seen: continue
			seen[marker] = 1
			result.append(item)
	except TypeError:
		return [x for (i,x) in enumerate(seq) if x not in seq[:i]]
	else:
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
