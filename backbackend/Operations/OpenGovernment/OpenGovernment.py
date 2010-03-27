import numpy as np
import tabular as tb

from System.Utils import MakeDir

root = '../Data/OpenGovernment/'
protocolroot = '../Protocol_Instances/OpenGovernment/'


def initialize(creates = (root, protocolroot)):
	MakeDir(root)
	MakeDir(protocolroot)


def initialize_mongosources(creates = root + 'MongoSources/'):
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