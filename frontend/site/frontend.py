import tornado.httpserver
import tornado.httpclient
import tornado.escape
import tornado.ioloop
import tornado.web
import tornado.options
import os
import re
import unicodedata
import pymongo as pm
import pymongo.json_util
import json
import tornado.autoreload

from tornado.options import define, options

define("port", default=8000, help="run on the given port", type=int)
define("api_url", default="http://ec2-184-73-61-176.compute-1.amazonaws.com", help="use this api url", type=str)


def group_by(data,grouping=None):
    pass

def urlencode(d):
    q = ""
    for k,v in d.iteritems():
        if isinstance(v,list):
            for vv in v:
                q += "&%s=%s"%(tornado.escape.url_escape(k),tornado.escape.url_escape(vv))
        else:
            q += "&%s=%s"%(tornado.escape.url_escape(k),tornado.escape.url_escape(v))
    if len(q) > 0:
        q = q[1:]
    return q
        
class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/", IndexHandler),
            (r"/get", GetHandler),
            (r"/find", FindHandler)
        ]
        settings = dict(
            template_path=os.path.join(os.path.dirname(__file__), "templates"),
            static_path=os.path.join(os.path.dirname(__file__), "static"),
            xsrf_cookies=True,
            cookie_secret="13oETzXKQAGaYdkL5gEmGeJJFuYh7EQnp2XdTP1o/Vo=",
        )
        tornado.web.Application.__init__(self, handlers, **settings)


class GetHandler(tornado.web.RequestHandler):
    def get(self):
        q = self.get_argument("q",None)
        title = self.get_argument("title",None)
        self.render("get.html",title=title,q=q)

class ClusteredFindHandler(tornado.web.RequestHandler):
    pass

class FindHandler(tornado.web.RequestHandler):
    
    # facet_fields = ["agency","subagency","SourceSpec","spatialDivisionsTight","spatialPhrasesTight","dateDivisionsTight","datePhrasesTight","datasetTight"]
    facet_fields = ["spatialDivisionsTight","spatialPhrasesTight","dateDivisionsTight","datePhrasesTight"]
    # display_keys = ["source","description","keywords","columnNames"]
    
    @tornado.web.asynchronous
    def get(self):
        http = tornado.httpclient.AsyncHTTPClient()
        q = self.get_argument("q",None)
        params = {
            "q" : q,
            "facet" : "true",
            "facet.field" : self.facet_fields
        }
        query = urlencode(params)
        http.fetch(options.api_url+"/find?"+query,
                   callback=self.async_callback(self.on_response))
    def on_response(self, response):
        if response.error: raise tornado.web.HTTPError(500)
        data = {}
        try:
            data = json.loads(response.body,object_hook=pm.json_util.object_hook)
            # data = tornado.escape.json_decode(response.body)
        except:
            print "error loading data %s"%(response.body)
            raise tornado.web.HTTPError(500)
        q = self.get_argument("q",None)
        docs = data["response"]["docs"]
        clustereddocs = {}
        for doc in docs:
            mongoQuery = "("+doc["query"][0]+")"
            collectionName = doc["collectionName"][0]
            getquery = {"collectionName":collectionName,
                        "querySequence":[["find",mongoQuery]]}
            doc["getquery"] = tornado.escape.url_escape(json.dumps(getquery))
            source_spec = json.loads(doc["SourceSpec"][0])
            print source_spec
            agency = source_spec.get("Agency")
            subagency = source_spec.get("Subagency")
            if not(clustereddocs.has_key(agency)):
                clustereddocs[agency] = {}
            agencydict = clustereddocs.get(agency)
            if not(agencydict.has_key(subagency)):
                clustereddocs[agency][subagency] = []
            clustereddocs[agency][subagency].append(doc)            
        self.render("find.html",raw=response.body,q=q,data=data,clustereddocs=clustereddocs,facet_fields=self.facet_fields)

class MetadataHandler(tornado.web.RequestHandler):
    """ name = name
        source = dict of agency topics etc
        bunch more
        """
    pass

class IndexHandler(tornado.web.RequestHandler):
    def get(self):
        self.render("index.html")

def main():
    tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(Application())
    print("Starting on 0.0.0.0:%s"%(options.port))
    http_server.listen(options.port)
    tornado.autoreload.start()
    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    main()
