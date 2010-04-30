"""This module sets initializes directory structure at high level and has some very basic common parsing utilities. 
"""

import numpy as np
import tabular as tb
import Operations.OpenGovernment.mongoUtils as mU
import backend.indexing as indexing
from System.Protocols import ApplyOperations2
from System.Utils import MakeDir


root = '../Data/OpenGovernment/'
protocolroot = '../Protocol_Instances/OpenGovernment/'
CERT_ROOT = root + 'Certificates/'
CERT_PROTOCOL_ROOT = '../Protocol_Instances/OpenGovernment/Certificates/'

def initialize(creates = protocolroot):
	MakeDir(protocolroot)

def initialize_mongosources(creates = root + 'MongoSources/'):
	MakeDir(creates)
	
def initialize_backendCertificates(creates = CERT_ROOT):
	MakeDir(creates)

def initialize_cert_protocol_root(creates = CERT_PROTOCOL_ROOT):
	MakeDir(creates)

def filldown(x):
	y = np.array([xx.strip() for xx in x])
	nz = np.append((y != '').nonzero()[0],[len(y)])
	return y[nz[:-1]].repeat(nz[1:] - nz[:-1])

	
def gethierarchy(x,f,postprocessor = None):
	hl = np.array([f(xx) for xx in x])
	# normalize 
	ind = np.concatenate([(hl == min(hl)).nonzero()[0], np.array([len(hl)])])
	if ind[0] != 0:
		ind = np.concatenate([np.array([0]), ind])	
	hl2 = []
	for i in range(len(ind)-1):
		hls = hl[ind[i]:ind[i+1]].copy()
		hls.sort()
		hls = tb.utils.uniqify(hls)
		D = dict(zip(hls, range(len(hls))))
		hl2 += [D[h] for h in hl[ind[i]:ind[i+1]]]

	hl = np.array(hl2)
	m = max(hl)
	cols = []
	for v in range(m+1):
		vxo = hl < v
		vx = hl == v
		if vx.any():
			nzv = np.append(vx.nonzero()[0],[len(x)])
			col = np.append(['']*nzv[0],x[nzv[:-1]].repeat(nzv[1:] - nzv[:-1]))
			col[vxo] = ''
			cols.append(col)
		else:
			cols.append(np.array(['']*len(x)))
	
	if postprocessor:
		for i in range(len(cols)):
			cols[i] = np.array([postprocessor(y) for y in cols[i]])
			
	return [cols,hl]
	

def backendProtocol(collectionName,certdir = None, createCertDir = False, createPath = None, slicePath = None, indexPath = None, hashSlices=True, write = True,ID = None):
	"""This protocol is the workflow for getting source data into the backend.  It sets up three steps:  1) add collection to DB
	2) makes the queryDB and 3) indexing the collection.    It writes certificates at each step to reflect completion in the filesystem. 
	"""
	if ID == None:
		ID = collectionName
	if ID and not ID.endswith('_'):
		ID += '_'
	path = mU.MONGOSOURCES_PATH + collectionName

	D = []
	
	if certdir == None and any([x == None for x in [createPath,slicePath,indexPath]]):
		certdir = CERT_ROOT
		
	if certdir:
		if createCertDir:
			D += [(ID + 'initialize',MakeDir,(certdir,))]
		if createPath == None:
			createPath = certdir + ID + 'createCertificate.txt'
		if slicePath == None:
			slicePath = certdir + ID + 'sliceCertificate.txt'
		if indexPath == None:
			indexPath = certdir + ID + 'indexCertificate.txt'
		
	D += [(ID + 'createCollection',mU.createCollection,(path,createPath)),
	(ID + 'makeQueryDB',indexing.makeQueryDB,[(collectionName,createPath,slicePath),{'hashSlices':hashSlices}]),
	(ID + 'indexCollection',indexing.indexCollection,(collectionName,slicePath,indexPath))]

	if write:
		outfile = CERT_PROTOCOL_ROOT + collectionName + '.py'
		ApplyOperations2(outfile,D)
	
	return D
	