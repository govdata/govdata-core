from govdata.deploy import backendProtocol, CERT_PROTOCOL_ROOT    
    
from parsers import bls

def backendBLS_ap(creates = CERT_PROTOCOL_ROOT + bls.PARSER_DICT['ap'].collectionName + '/'):
    backendProtocol(bls.PARSER_DICT['ap'])
    
def backendBLS_bd(creates = CERT_PROTOCOL_ROOT + bls.PARSER_DICT['bd'].collectionName + '/'):
    backendProtocol(bls.PARSER_DICT['bd'])    
   
def backendBLS_cw(creates = CERT_PROTOCOL_ROOT + bls.PARSER_DICT['cw'].collectionName + '/'):
    backendProtocol(bls.PARSER_DICT['cw'])
    
def backendBLS_li(creates = CERT_PROTOCOL_ROOT + bls.PARSER_DICT['li'].collectionName + '/'):
    backendProtocol(bls.PARSER_DICT['li'])
    
def backendBLS_pc(creates = CERT_PROTOCOL_ROOT + bls.PARSER_DICT['pc'].collectionName + '/'):
    backendProtocol(bls.PARSER_DICT['pc'])

def backendBLS_wp(creates = CERT_PROTOCOL_ROOT + bls.PARSER_DICT['wp'].collectionName + '/'):
    backendProtocol(bls.PARSER_DICT['wp'])
    
def backendBLS_ce(creates = CERT_PROTOCOL_ROOT + bls.PARSER_DICT['ce'].collectionName + '/'):
    backendProtocol(bls.PARSER_DICT['ce'])
    
def backendBLS_sm(creates = CERT_PROTOCOL_ROOT + bls.PARSER_DICT['sm'].collectionName + '/'):
    backendProtocol(bls.PARSER_DICT['sm'])
    
def backendBLS_jt(creates = CERT_PROTOCOL_ROOT + bls.PARSER_DICT['jt'].collectionName + '/'):
    backendProtocol(bls.PARSER_DICT['jt'])

def backendBLS_la(creates = CERT_PROTOCOL_ROOT + bls.PARSER_DICT['la'].collectionName + '/'):
    backendProtocol(bls.PARSER_DICT['la'])
    
def backendBLS_lu(creates = CERT_PROTOCOL_ROOT + bls.LU_PARSER.collectionName + '/'):
    backendProtocol(bls.LU_PARSER)
