import os
from django.contrib.gis.utils import LayerMapping
from models import *


def run(obj,shp_file,mapping,verbose=True):
    lm = LayerMapping(obj,shp_file,mapping,
                      transform=False, encoding='iso-8859-1')

    lm.save(strict=True, verbose=verbose)


def run_counties():
	run(USCounties,USCounties_shp,USCounties_mapping)
	
def run_states():
	run(USStates,USStates_shp,USStates_mapping)

def run_FiveDigitZCTAs():
	for x in FiveDigitZCTAs_shp:
		print x
	
		num = x.split('/')[-1].split('.')[0].split('_')[0][2:4]	
		mapping = FiveDigitZCTAs_mapping.copy()
		mapping['zt_field'] = 'ZT' + num + '_D00_'
		mapping['zt_i'] = 'ZT' + num + '_D00_I'
		run(FiveDigitZCTAs,x,mapping)
		
def run_cbsa():
	run(CBSA,CBSA_shp,CBSA_mapping)
		
def run_csa():
	run(CSA,CSA_shp,CSA_mapping)
				
def run_metdiv():
	run(METDIV,METDIV_shp,METDIV_mapping)
	
def run_CensusTracts():
	for x in CensusTracts_shp:
		print x
		run(CensusTracts,x,CensusTracts_mapping)
		
				
def run_CensusRegions():
	run(CensusRegions,CensusRegions_shp,CensusRegions_mapping)
	
def run_CensusDivisions():
	run(CensusDivisions,CensusDivisions_shp,CensusDivisions_mapping)		
	