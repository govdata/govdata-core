import tornado.web
import json
import pymongo as pm
import pymongo.json_util
import copy
from utils import *


def genLinkFn(q,filters):
    def linkFn(k,v):
        filtercopy = copy.deepcopy(filters)
        filtercopy.append("(%s):(%s)"%(k,v))
        return "/?q=%s&%s"%(q,urlencode({"filter":filtercopy}))
    return linkFn

def genRmLinkFn(q,filters):
    def linkFn(k,v):
        filtercopy = copy.deepcopy(filters)
        filtercopy.remove("(%s):(%s)"%(k,v))
        return "/?q=%s&%s"%(q,urlencode({"filter":filtercopy}))
    return linkFn

def cleanFilter(f):
    return f.replace("(","").replace(")","").split(":")

class Result(tornado.web.UIModule):
    def render(self, result, q="", filters=[]):
        linkFn = genLinkFn(q,filters)
        return self.render_string("modules/result.html", result=result, linkFn=linkFn)

class Search(tornado.web.UIModule):
    def render(self, value=""):
        return self.render_string("modules/search.html", value=value)

class Facet(tornado.web.UIModule):
    def render(self, facets={}, q="", filters=[], **kwargs):
        linkFn = genLinkFn(q,filters)
        rmLinkFn = genRmLinkFn(q,filters)
        facets = facets.get('facet_fields',None)
        formated_filters = map(cleanFilter, filters)
        assert(facets != None)
        return self.render_string("modules/facet.html", facets=facets, linkFn=linkFn, rmLinkFn=rmLinkFn, filters=formated_filters, **kwargs)
                
class Find(tornado.web.UIModule):
    def render(self, results=[], **kwargs):
        modresults = []
        last = {}
        for r in results:
            dimension = r["dimension"][0]
            source = json.loads(r["sourceSpec"][0])
            query = json.loads(r["query"][0])
            current = {
                'dimension' : { 'data' : dimension },
                'sourceSpec' : {
                    'data': {
                        'agency': source['agency'],
                        'subagency': source['subagency'] }},
                'dataset' : {'data' : source['subagency'] },
                'query' : { 'data' : query }
            }
            for k in current.keys():
                last_value = last.get(k,{})
                if current.get(k,{}).get('data') == last_value.get('data'):
                    lastColor = last_value.get("color",True)
                    current[k]["color"] = lastColor
                    current[k]["same"] = True
                else:
                    lastColor = last_value.get("color",True)
                    current[k]["color"] = not(lastColor)
                    current[k]["same"] = False
            current['mongoID'] = r["mongoID"][0]
            current['collectionName'] = r["collectionName"][0]
            modresults.append(current)
            last = current
        return self.render_string("modules/find.html", results=modresults, **kwargs)

class Value(tornado.web.UIModule):
    def render(self, value_type, value):
        def renderLocation(v):
            if type(v) == dict:
                return v.get("s","")+" "+v.get("X","")
            else:
                return v
        rendered_string = {
            'Location' : renderLocation(value)
        }.get(value_type, value)
        return self.render_string("modules/value.html", value=rendered_string)
        