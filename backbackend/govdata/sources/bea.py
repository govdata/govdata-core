import os
import pickle

from govdata.sources.core import SOURCE_COMPONENTS_DIR

def make_bea_sources(creates = os.path.join(SOURCE_COMPONENTS_DIR,'bea.pickle')):
	
	s = {}
	##NIPA
	s['BEA_NIPA'] = [('agency',{'name':'Department of Commerce','shortName':'DOC'}),
	                 ('subagency',{'name':'Bureau of Economic Analysis','shortName':'BEA'}),
	                 ('program',{'shortName':'REA','name':'National Economic Accounts'}), 
	                 ('dataset',{'shortName':'NIPA','name':'National Income and Product Account Tables'})]
	
	#FAT
	s['BEA_FAT'] = [('agency',{'name':'Department of Commerce','shortName':'DOC'}),
	                ('subagency',{'name':'Bureau of Economic Analysis','shortName':'BEA'}),
	                ('program',{'shortName':'NEA','name':'National Economic Accounts'}), 
	                ('dataset',{'shortName':'FAT','name':'Fixed Asset Tables'})]
	
	#REG
	s['BEA_RegionalGDP'] = [('agency',{'name':'Department of Commerce','shortName':'DOC'}),
	                        ('subagency',{'name':'Bureau of Economic Analysis','shortName':'BEA'}),
	                        ('program',{'shortName':'REA','name':'Regional Economic Accounts'}), 
	                        ('dataset',{'shortName':'RegGDP','name':'Regional GDP Data'})]
	
	#PI
	s['BEA_PersonaIncome'] = [('agency',{'name':'Department of Commerce','shortName':'DOC'}),
	                          ('subagency',{'name':'Bureau of Economic Analysis','shortName':'BEA'}),
	                          ('program',{'shortName':'REA','name':'Regional Economic Accounts'}), 
	                          ('dataset',{'shortName':'PI','name':'Personal Income'})]
	
	#II
	s['BEA_InternationalInvestment'] = [('agency',{'name':'Department of Commerce','shortName':'DOC'}),
	                                    ('subagency',{'name':'Bureau of Economic Analysis','shortName':'BEA'}),
	                                    ('program',{'shortName':'IEA','name':'International Economic Accounts'}),
	                                    ('dataset',{'shortName':'OMC','name':'Operations of Multinational Companies'})]
	
	
	
	F = open(creates,'w')
	pickle.dump(s,F)
	F.close()