from govdata.deploy import backendProtocol, CERT_PROTOCOL_ROOT

from parsers import bea

FAT_NAME = bea.FAT_PARSER.collectionName
def backend_BEA_FAT(creates = CERT_PROTOCOL_ROOT + FAT_NAME + '/',Fast = True):
    backendProtocol(bea.FAT_PARSER)
    
NIPA_NAME = bea.NIPA_PARSER.collectionName
def backend_BEA_NIPA(creates = CERT_PROTOCOL_ROOT + NIPA_NAME + '/',Fast = True):
    backendProtocol(bea.NIPA_PARSE)
  
REG_NAME = bea.REG_PARSER.collectionName
def backend_BEA_RegionalGDP(creates = CERT_PROTOCOL_ROOT + REG_NAME + '/',Fast = True):
    backendProtocol(bea.REG_PARSER)
  
PI_NAME = bea.PI_PARSER.collectionName  
def backend_BEA_PersonalIncome(creates = CERT_PROTOCOL_ROOT + PI_NAME + '/',Fast = True):
    backendProtocol(bea.PI_PARSER)

ITRADE_NAME = bea.ITRADE_PARSER.collectionName
def backend_BEA_ITrade(creates = CERT_PROTOCOL_ROOT + ITRADE_NAME + '/'):
    backendProtocol( bea.ITRADE_PARSER, uptostep='download_check')

ITRANS_NAME = bea.ITRANS_PARSER.collectionName
def backend_BEA_ITrans(creates = CERT_PROTOCOL_ROOT + ITRANS_NAME + '/'):
    backendProtocol(bea.ITRANS_PARSER, uptostep='download_check')
    
II_NAME = bea.II_PARSER.collectionName
def backend_BEA_II(creates = CERT_PROTOCOL_ROOT + II_NAME + '/'):    
    backendProtocol(bea.II_PARSER)
