import tornado.web
import json
import pymongo as pm
import pymongo.json_util

class Result(tornado.web.UIModule):
    def render(self, result, q=""):
        return self.render_string("modules/result.html", result=result, q=q)

class Search(tornado.web.UIModule):
    def render(self, value=""):
        return self.render_string("modules/search.html", value=value)

class Facet(tornado.web.UIModule):
    def render(self, facets={}, q=""):
        facets = facets.get('facet_fields',None)
        assert(facets != None)
        print(facets)
        return self.render_string("modules/facet.html", facets=facets, q=q)
                
class Find(tornado.web.UIModule):
    def render(self, results=[], q=""):
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
        return self.render_string("modules/find.html", results=modresults, q=q)

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
        