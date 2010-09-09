import tornado.httpclient
import tornado.escape
import tornado.web
from tornado.options import define, options

import os
import re
import unicodedata
import pymongo as pm
import pymongo.json_util
import json
from urllib import quote, unquote

from utils import *
import uimodules

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
                "rows" : options.per_page
            }
            query = urlencode(params)
            http.fetch(options.api_url+"/find?"+query,
                       callback=self.async_callback(self.on_response, partial=partial, filters=filters, jsonfilters=json.dumps(filters), q=q))
    def on_response(self, response, partial, **kwargs):
        if response.error: raise tornado.web.HTTPError(500)
        data = {}
        try:
            data = json.loads(response.body,object_hook=pm.json_util.object_hook)
            # data = tornado.escape.json_decode(response.body)
        except:
            print "error loading data %s"%(response.body)
            raise tornado.web.HTTPError(500)
        kwargs['response'] = data['response']
        if partial:
            self.render("_find.html",**kwargs)
        else:
            kwargs['facets'] = data['facet_counts']
            kwargs['per_page'] = options.per_page
            self.render("find.html",**kwargs)

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


