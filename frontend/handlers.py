import tornado.httpclient
import tornado.escape
import tornado.web
from tornado.options import define, options

import re
import unicodedata
import pymongo as pm
import pymongo.json_util
import json
from urllib import quote, unquote
import urllib2

from utils import *
import uimodules

import os
import sys
sys.path.append(os.path.join(".."))
from common.utils import uniqify
from common import commonjs
import collections

def make_metadata_dict(metadata):
    return dict([(m["name"],m["metadata"]) for m in metadata])

def make_metadata_value_render(metadata_dict):
    for collectionName,value in metadata_dict.iteritems():
        vp = value['valueProcessors']
        cg = value['columnGroups']
        cgkeys_in_vp = [k for k in cg if k in vp.keys()]
        ctx = commonjs.pyV8CommonJS()
        for k in vp:
            fn = "var %s_%s = function (value){ %s };" % (collectionName,k,vp[k])
            ctx.eval(str(fn))
    def render(collectionName,key,value):
        if key in vp.keys():
            fname = key
        else:
            possibles = [k for k in cgkeys_in_vp if key in cg[k]]
            if possibles:
                fname = possibles[0]
            else:
                fname = None
        if fname:
            fn = "%s_%s(%s)"%(collectionName,fname,json.dumps(value))
            return ctx.eval(str(fn))
        else:
            return str(value)        
    return render


class ShowHandler(tornado.web.RequestHandler):
    def get(self):
        q = self.get_argument("q",None)
        collection = self.get_argument("collection",None)
        self.render("show.html",q=q,collection=collection)

class TableHandler(tornado.web.RequestHandler):

    COUNT_CACHE = {}
    DEFAULT_DISPLAY_LENGTH = 50
     
    @tornado.web.asynchronous 
    def get(self):
        base_query_string = self.get_argument("q",None)
        base_query = json.loads(base_query_string,object_pairs_hook=collections.OrderedDict)
        query_seq = [{"action": "find", "args": [base_query.copy()]}]
        filter_query = base_query.copy()
        collection = self.get_argument("collection",None)
         
        args = self.request.arguments
        for k in args:
            args[k] = args[k][0]      
            
        sEcho = self.get_argument("sEcho",0) 
       
        #add skip for iDisplayStart
        iDisplayStart = self.get_argument("iDisplayStart",0)
        query_seq.append({"action":"skip","args":[iDisplayStart]})
        #add limit for iDisplayLength
        iDisplayLength = self.get_argument("iDisplayLength",self.DEFAULT_DISPLAY_LENGTH)
        query_seq.append({"action":"limit","args":[iDisplayLength]})

        iSortCol = self.get_argument("iSortCol","")
           
        #add to find for sSearch
        sSearch = self.get_argument("sSearch",'')
        searchables = [int(k.split('_')[1]) for k in args if k.startswith('bSearchable') and args[k]]
        filters = dict([(int(k.split('_')[1]),args[k]) for k in args if k.startswith('bSearch_') and args[k]])
        if sSearch:                 
            query_seq[0]["args"][0].append({"$or":dict([(s, re.compile(sSearch,re.I)) for s in searchables])})
            filter_query["$or"] = collections.OrderedDict([(s, re.compile(sSearch,re.I)) for s in searchables])
        for k,search in filters.items():
            if search:
                query_seq[0]["args"][0].append({k:re.compile(search,re.I)})
                filter_query["$or"] = {k:re.compile(search,re.I)}
            
        sortables = [int(k.split('_')[1]) for k in args if k.startswith('bSortable') and args[k]]
        #add sort if sortable from iSortCol and iSortDir
        if iSortCol != '' and iSortCol in sortables:
            direction = pm.ASCENDING if iSortDir == "asc" else pm.DESCENDING
            query.append({"action":sort,"kwargs":{"direction":direction}})

        #compute countss        
        query_string = json.dumps(base_query)
        if (query_string,collection) in self.COUNT_CACHE:
            iTotalRecords= self.COUNT_CACHE[(query_string,collection)]
        else:
            count_query = [{"action":"find", "args":[base_query]},{"action":"count"}]
            base_query_count_string = json.dumps({"query":count_query,"collection":collection})
            request1 = options.api_url + '/get?q=' + quote(base_query_count_string)
            iTotalRecords = json.loads(urllib2.urlopen(request1).read())['data']
            self.COUNT_CACHE[(query_string,collection)] = iTotalRecords

        filter_query_string = json.dumps(filter_query)
        if (filter_query_string,collection) in self.COUNT_CACHE:
             iTotalDisplayRecords = self.COUNT_CACHE[(filter_query_string,collection)]
        else:
            filter_count_query = [{"action":"find","args":[filter_query.copy()]},{"action":"count"}]
            filter_query_count_string = json.dumps({"query":filter_count_query,"collection":collection})
            request2 = options.api_url + '/get?q=' + quote(filter_query_count_string)
            iTotalDisplayRecords = json.loads(urllib2.urlopen(request2).read())['data']
            self.COUNT_CACHE[(filter_query_string,collection)] = iTotalDisplayRecords      
            
        #parse the request from dataTables and make the request 
        http = tornado.httpclient.AsyncHTTPClient()
        
        backend_request = '/table?q=%s' % (quote(json.dumps({"query":query_seq,"collection":collection})),)
        http.fetch(options.api_url + backend_request, callback = self.async_callback(self.on_response,
                    sEcho=sEcho,iTotalRecords=iTotalRecords,iTotalDisplayRecords=iTotalDisplayRecords))

    def on_response(self,response,sEcho,iTotalRecords,iTotalDisplayRecords):
        X = json.loads(response.body,object_hook=pm.json_util.object_hook, object_pairs_hook=collections.OrderedDict)
        X = X["data"]
        columns = ','.join([a['label'] for a in X['cols']])
        response_data = {'sEcho': sEcho, 'iTotalRecords': iTotalRecords, 'iTotalDisplayRecords':iTotalDisplayRecords, 'sColumns':columns, 'aaData': X['rows']}
        self.write(json.dumps(response_data))
        self.finish()

class FindHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        q = self.get_argument("q",None)
        page = tornado.escape.json_decode(self.get_argument("page","0"))
        partial = tornado.escape.json_decode(self.get_argument("partial","false"))
        filters = self.get_arguments("filter")
        queries = self.get_arguments("filterq")
        if q == None:
            self.render("welcome.html",q="search here")
        else:
            http = tornado.httpclient.AsyncHTTPClient()
            params = {
                "q" : "%s %s"%(q," ".join(queries)),
                "fq" : filters,
                "start" : page * options.per_page,
                "rows" : options.per_page,
                # "facet.field" : [""]
            }
            query = urlencode(params)
            http.fetch(options.api_url+"/find?"+query,
                       callback=self.async_callback(self.on_response, partial=partial, 
                       queries=queries, filters=filters, jsonfilters=json.dumps(filters), q=q))
    def on_response(self, response, **kwargs):
        if response.error: raise tornado.web.HTTPError(500)
        data = {}
        try:
            data = json.loads(response.body,object_hook=pm.json_util.object_hook, object_pairs_hook=collections.OrderedDict)
        except:
            print "error loading data %s"%(response.body)
            raise tornado.web.HTTPError(500)
        kwargs['data'] = data
        colls = uniqify([x['collectionName'][0] for x in data['response']['docs']])
        querySequence = [["find",[[{"name":{"$in":colls}}],{"fields":["metadata.valueProcessors","name","metadata.columnGroups","metadata.source"]}]]]
        http = tornado.httpclient.AsyncHTTPClient()
        http.fetch(options.api_url+"/sources?querySequence="+quote(json.dumps(querySequence)), callback=self.async_callback(self.render_with_metadata, **kwargs))
    def render_with_metadata(self, metadata, partial, **kwargs):
        metadata = json.loads(metadata.body,object_hook=pm.json_util.object_hook, object_pairs_hook=collections.OrderedDict)
        metadata_dict = make_metadata_dict(metadata)
        value_renderer = make_metadata_value_render(metadata_dict)
        if partial:
            self.render("_find.html",renderer=value_renderer,**kwargs)
        else:
            kwargs['per_page'] = options.per_page
            self.render("find.html",renderer=value_renderer,**kwargs)


class MetadataHandler(tornado.web.RequestHandler):
    """ name = name
        source = dict of agency topics etc
        bunch more
        """
    pass


