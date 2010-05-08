"""
    Common solr-related things that are not handled well by existing solr-python clients. 
"""

import urllib
import urllib2
from common.utils import is_string_like
    
def query(q, **params):
    """
        query the solr index.  I wrote this because I found many flows with the query methods in solrpy. 
    """
    return urllib2.urlopen(queryUrl(q,**params)).read()

def queryUrl(q, hlParams=None,facetParams=None,mltParams = None, host = 'localhost', port = '8983', **params):
    """
        query the solr index.  Return URL only
    """
    paramstring = processSolrArgList('',params)
    facetstring = processSolrArgList('facet',facetParams)
    hlstring = processSolrArgList('hl',hlParams)
    mltstring = processSolrArgList('mlt',mltParams)

    URL = 'http://' + host + ':' + port + '/solr/select?q=' + urllib.quote(q) + paramstring + facetstring + hlstring + mltstring
    return URL
           
def processSolrArgList(base,valdict):
    """
        helper for query
    """
    
    return ('&' + ((base + '=true&') if base and '' not in valdict.keys() else '') + '&'.join([processSolrArg(base,key,valdict[key]) for key in valdict])) if valdict else ''       
    
def processSolrArg(base,key,value):
    """
        helper for query
    """
    
    return base + ('.' if key and base else '') + key + '=' + urllib.quote(value) if is_string_like(value) else '&'.join([base + ('.' if key and base else '') + key + '=' + urllib.quote(v) for v in value])
