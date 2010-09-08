import tornado.escape

def group_by(data,grouping=None):
    pass

def urlencode(d):
    q = ""
    for k,v in d.iteritems():
        if isinstance(v,list):
            for vv in v:
                q += "&%s=%s"%(tornado.escape.url_escape(str(k)),tornado.escape.url_escape(str(vv)))
        else:
            q += "&%s=%s"%(tornado.escape.url_escape(str(k)),tornado.escape.url_escape(str(v)))
    if len(q) > 0:
        q = q[1:]
    return q
