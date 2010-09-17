import tornado.web
import json
import pymongo as pm
import pymongo.json_util
import copy
from utils import *
from collections import OrderedDict

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


def genFilterFn(q,filters,queries):
    def filterFn(k,v,queryFn=False):
        if queryFn:
            queriescpy = copy.deepcopy(queries)
            to_append = "%s=\"%s\""%(k,v)
            queriescpy.append(to_append)
            return "/?q=%s&%s&%s"%(q,urlencode({"filter":filters}),urlencode({"filterq":queriescpy}))
        else:
            filterscpy = copy.deepcopy(filters)
            to_append = "%s:\"%s\""%(k,v)
            filterscpy.append(to_append)
            return "/?q=%s&%s&%s"%(q,urlencode({"filter":filterscpy}),urlencode({"filterq":queries}))
    return filterFn

def genRmFilterFn(q,filters,queries):
    def filterFn(k,v,queryFn=False):
        if queryFn:
            queriescpy = copy.deepcopy(queries)
            to_remove = "%s=\"%s\""%(k,v)
            queriescpy.remove(to_remove)
            return "/?q=%s&%s&%s"%(q,urlencode({"filter":filters}),urlencode({"filterq":queriescpy}))
        else:
            filterscpy = copy.deepcopy(filters)
            to_remove = "%s:\"%s\""%(k,v)
            filterscpy.remove(to_remove)
            return "/?q=%s&%s&%s"%(q,urlencode({"filter":filterscpy}),urlencode({"filterq":queries}))
    return filterFn

def genShow(result):
    def show(result):
        # return ("/show?q=%s" % (result['mongoID'],))
        return ("/table?q=%s&collection=%s" % (json.dumps(result['query']['data']),result['collection']))
    return show

def cleanFilter(f):
    return f.replace("\"","").split(":")

def cleanFilterq(f):
    return f.replace("\"","").split("=")

class Result(tornado.web.UIModule):
    def render(self, result, q="", filters=[], queries=[], **kwargs):
        filterFn = genFilterFn(q,filters,queries)
        show = genShow(result)
        return self.render_string("modules/result.html", result=result, filterFn=filterFn, show=show, **kwargs)

class Search(tornado.web.UIModule):
    def render(self, value=""):
        return self.render_string("modules/search.html", value=value)

class Facet(tornado.web.UIModule):
    def render(self, facets={}, q="", filters=[], queries=[], **kwargs):
        filterFn = genFilterFn(q,filters,queries)
        rmFilterFn = genRmFilterFn(q,filters,queries)
        facets = facets.get('facet_fields',None)
        formated_filters = map(cleanFilter, filters)
        formated_queries = map(cleanFilterq, queries)
        print formated_queries
        assert(facets != None)
        return self.render_string("modules/facet.html", facets=facets, filterFn=filterFn, rmFilterFn=rmFilterFn, queries=formated_queries, filters=formated_filters, **kwargs)
                
class Find(tornado.web.UIModule):
    def render(self, results=[], **kwargs):
        modresults = []
        last = {}
        for r in results:
            volume = r["volume"][0]
            source = json.loads(r["sourceSpec"][0],object_hook=pm.json_util.object_hook, object_pairs_hook=OrderedDict)
            query = json.loads(r["query"][0],object_hook=pm.json_util.object_hook, object_pairs_hook=OrderedDict)
            dataset = source.pop('dataset')
            collection = r["collection"][0]
            current = {
                'volume' : { 'data' : volume },
                'sourceSpec' : { 'data': source },
                'dataset' : {'data' : dataset },
                'query' : { 'data' : query },
                'collection' : collection
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
