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

from utils import *
import uimodules

import os
import sys
sys.path.append(os.path.join(".."))
from common.utils import uniqify
from common import commonjs


def make_metadata_dict(metadata):
    return dict([(m["name"],m["metadata"]) for m in metadata])

def make_metadata_render(metadata_dict):
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
            print(json.dumps(value))
            fn = "%s_%s(%s)"%(collectionName,fname,json.dumps(value))
            return ctx.eval(str(fn))
        else:
            return str(value)        
    return render

class GetHandler(tornado.web.RequestHandler):
    def get(self):
        q = self.get_argument("q",None)
        self.render("get.html",q=q)

class FindHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        q = self.get_argument("q",None)
        page = tornado.escape.json_decode(self.get_argument("page","0"))
        partial = tornado.escape.json_decode(self.get_argument("partial","false"))
        filters = self.get_arguments("filter")
        print("FILTERS %s" % filters)
        filterstr = " ".join(filters)
        if q == None:
            self.render("welcome.html",q="search here")
        else:
            http = tornado.httpclient.AsyncHTTPClient()
            params = {
                "q" : q+" "+filterstr,
                "start" : page * options.per_page,
                "rows" : options.per_page,
                # "facet.field" : [""]
            }
            query = urlencode(params)
            http.fetch(options.api_url+"/find?"+query,
                       callback=self.async_callback(self.on_response, partial=partial, filters=filters, jsonfilters=json.dumps(filters), q=q))
    def on_response(self, response, **kwargs):
        if response.error: raise tornado.web.HTTPError(500)
        data = {}
        try:
            data = json.loads(response.body,object_hook=pm.json_util.object_hook)
            # data = tornado.escape.json_decode(response.body)
        except:
            print "error loading data %s"%(response.body)
            raise tornado.web.HTTPError(500)
        kwargs['data'] = data
        collections = uniqify([x['collectionName'][0] for x in data['response']['docs']])
        querySequence = [["find",[[{"name":{"$in":collections}}],{"fields":["metadata.valueProcessors", "name","metadata.columnGroups"]}]]]
        http = tornado.httpclient.AsyncHTTPClient()
        http.fetch(options.api_url+"/sources?querySequence="+quote(json.dumps(querySequence)), callback=self.async_callback(self.render_with_metadata, **kwargs))
    def render_with_metadata(self, metadata, partial, **kwargs):
        metadata = json.loads(metadata.body)
        metadata_dict = make_metadata_dict(metadata)
        renderer = make_metadata_render(metadata_dict)
        if partial:
            self.render("_find.html",renderer=renderer,**kwargs)
        else:
            kwargs['per_page'] = options.per_page
            self.render("find.html",renderer=renderer,**kwargs)

class FindPartialHandler(tornado.web.RequestHandler):
    def get(self):
        self.render
    pass

class MetadataHandler(tornado.web.RequestHandler):
    """ name = name
        source = dict of agency topics etc
        bunch more
        """
    pass


