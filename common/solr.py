"""
    Common solr-related things that are not handled well by existing solr-python clients. 
"""

import urllib
import urllib2
from common.utils import is_string_like
    
def query(q, **params):
    """
        query the solr index.  I wrote this because I found many flaws with the query methods in solrpy. 
    """
    return urllib2.urlopen(queryUrl(q,**params)).read()
    

def queryUrl(q, hlParams=None,facetParams=None,mltParams = None, host = 'localhost', port = '8983', **params):
    """
        query the solr index, using "select" (standard) searchComponent. Return URL only
    """
    
    paramsets = [('',params),('facet',facetParams),('hl',hlParams),('mlt',mltParams)]
    
    return solrURL('select',paramsets, q = q, host = host, port = port)
  
  
def solrURL(component, paramsets, q = None, host = 'localhost', port = '8983'):
    """
        query the solr index, general searchComponent.  Return URL only
    """

    qstring = processSolrArg('q','',q) if q else ''    
    
    paramstrings = [processSolrArgList(name,params) for (name,params) in paramsets]
    
    URL =  'http://' + host + ':' + port + '/solr/' + component + '?' + qstring + ''.join(paramstrings)
    
    print 'solrURL=',URL
    
    return URL
    
           
def processSolrArgList(base,valdict):
    """
        helper for solrURL
    """
    
    return ('&' + ((base + '=true&') if base and '' not in valdict.keys() else '') + '&'.join([processSolrArg(base,key,valdict[key]) for key in valdict])) if valdict else ''       
    
def processSolrArg(base,key,value):
    """
        helper for solrURL
    """
    
    return base + ('.' if key and base else '') + key + '=' + urllib.quote(value) if is_string_like(value) else '&'.join([base + ('.' if key and base else '') + key + '=' + urllib.quote(v) for v in value])
