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
        partial = tornado.escape.json_decode(self.get_argument("partial","false"))
        page = tornado.escape.json_decode(self.get_argument("page","0"))
        if q == None:
            self.render("welcome.html",q="search here")
        else:
            http = tornado.httpclient.AsyncHTTPClient()
            params = {
                "q" : q,
                "start" : page * options.per_page,
                "rows" : options.per_page
            }
            query = urlencode(params)
            http.fetch(options.api_url+"/find?"+query,
                       callback=self.async_callback(self.on_response, q=q, partial=partial))
    def on_response(self, response, q="", partial=False):
        if response.error: raise tornado.web.HTTPError(500)
        data = {}
        try:
            data = json.loads(response.body,object_hook=pm.json_util.object_hook)
            # data = tornado.escape.json_decode(response.body)
        except:
            print "error loading data %s"%(response.body)
            raise tornado.web.HTTPError(500)
        if partial:
            self.render("_find.html",q=q,response=data["response"])
        else:
            self.render("find.html",q=q,response=data["response"],facets=data["facet_counts"],per_page=options.per_page)

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


