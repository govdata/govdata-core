#!/usr/bin/env python

import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
import api
import os
import tornado.autoreload

from tornado.options import define, options

define("port", default=8888, help="run on the given port", type=int)
define("processes", default=4, help="number of threads in the pool", type=int)

class GovLove(tornado.web.Application):
    def __init__(self,ioloop):
        handlers = [
            (r"/", MainHandler),
            (r"/uberget", api.GetHandler),
            (r"/find", api.FindHandler),
            (r"/get", api.SimpleDataHandler),
            (r"/gtable", api.TableHandler),
            (r"/gtimeline", api.TimelineHandler),
            (r"/sources", api.SourceHandler),
        ]
        settings = dict(
            template_path=os.path.join(os.path.dirname(__file__), "templates"),
            static_path=os.path.join(os.path.dirname(__file__), "static"),
            debug=True,
            io_loop=ioloop
        )
        tornado.web.Application.__init__(self, handlers, **settings)

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("hello world!")

def main():
    tornado.options.parse_command_line()
    ioloop = tornado.ioloop.IOLoop.instance()
    http_server = tornado.httpserver.HTTPServer(GovLove(ioloop))
    http_server.listen(options.port)
    tornado.autoreload.start()
    ioloop.start()

if __name__ == "__main__":
    main()
