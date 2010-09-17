import tornado.web
import json
import pymongo as pm
import pymongo.json_util
import copy
from utils import *


def difference(obj1,obj2):
   if hasattr(obj2,"keys"):
       if hasattr(obj1,"keys"):
           res = [(k,difference(obj1[k],obj2[k])) if k in obj1.keys() else (k,obj2[k]) for k in obj2]
           res = dict([(x,y) for (x,y) in res if y])
       else:
           res = obj2
   elif isinstance(obj2,list):
       if isinstance(obj1,list):
           diffs = [difference(x,y) for (x,y) in zip(obj1,obj2)] + obj2[len(obj1):]
           res =  [t for (i,t) in enumerate(diffs) if t != None and t not in diffs[:i]]
       else:
           res =  obj2
   elif obj1 != obj2:
       res = obj2
   else:
       res = None
   if res != None and res != [] and res != {}:
       return res

def intersection(obj1,obj2):
  if hasattr(obj2,"keys"):
      if hasattr(obj1,"keys"):
          res = [(k,intersection(obj1[k],obj2[k])) for k in obj2 if k in obj1.keys()]
          res = dict([(x,y) for (x,y) in res if y])
      else:
          res = None
  elif isinstance(obj2,list):
      if isinstance(obj1,list):
          intersections = [intersection(x,y) for (x,y) in zip(obj1,obj2)]
          res =  [t for (i,t) in enumerate(intersections) if t != None and t in intersections[:i]]
      else:
          res =  None
  elif obj1 == obj2:
      res = obj1
  else:
      res = None
  if res != None and res != [] and res != {}:
      return res


def genFilterFn(q,filters):
    def filterFn(k,v):
        filtercopy = copy.deepcopy(filters)
        filtercopy.append("(%s):(%s)"%(k,v))
        return "/?q=%s&%s"%(q,urlencode({"filter":filtercopy}))
    return filterFn

def genRmFilterFn(q,filters):
    def filterFn(k,v):
        filtercopy = copy.deepcopy(filters)
        filtercopy.remove("(%s):(%s)"%(k,v))
        return "/?q=%s&%s"%(q,urlencode({"filter":filtercopy}))
    return filterFn

def genGet(result):
    def get(result):
        return ("/get?q=%s" % (result['mongoID'],))
    return get

def cleanFilter(f):
    return f.replace("(","").replace(")","").split(":")

class Result(tornado.web.UIModule):
    def render(self, result, q="", filters=[], **kwargs):
        filterFn = genFilterFn(q,filters)
        get = genGet(result)
        return self.render_string("modules/result.html", result=result, filterFn=filterFn, get=get, **kwargs)

class Search(tornado.web.UIModule):
    def render(self, value=""):
        return self.render_string("modules/search.html", value=value)

class Facet(tornado.web.UIModule):
    def render(self, facets={}, q="", filters=[], **kwargs):
        filterFn = genFilterFn(q,filters)
        rmFilterFn = genRmFilterFn(q,filters)
        facets = facets.get('facet_fields',None)
        formated_filters = map(cleanFilter, filters)
        assert(facets != None)
        return self.render_string("modules/facet.html", facets=facets, filterFn=filterFn, rmFilterFn=rmFilterFn, filters=formated_filters, **kwargs)
                
class Find(tornado.web.UIModule):
    def render(self, results=[], **kwargs):
        modresults = []
        last = {}
        for r in results:
            volume = r["volume"][0]
            source = json.loads(r["sourceSpec"][0])
            query = json.loads(r["query"][0])
            current = {
                'volume' : { 'data' : volume },
                'sourceSpec' : {
                    'data': {
                        'agency': source['agency'],
                        'subagency': source['subagency'] }},
                'dataset' : {'data' : source['dataset'] },
                'query' : { 'data' : query }
            }
            for k in current.keys():
                last_value = last.get(k,{})
                a = last_value.get('data')
                b = current.get(k,{}).get('data')
                diff = difference(a,b)
                current[k]["diff"] = diff
                current[k]["intersection"] = intersection(a,b)
                lastColor = last_value.get("color",True)
                if diff == None:
                    current[k]["color"] = lastColor
                else:
                    current[k]["color"] = not(lastColor)
            current['mongoID'] = r["mongoID"][0]
            current['collectionName'] = r["collectionName"][0]
            modresults.append(current)
            last = current
        return self.render_string("modules/find.html", results=modresults, **kwargs)
