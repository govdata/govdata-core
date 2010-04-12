#!/usr/bin/env python

import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
import api
import os

from tornado.options import define, options

define("port", default=8888, help="run on the given port", type=int)

class BetterData(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/", MainHandler),
            (r"/get", api.GetHandler),
            (r"/find", api.FindHandler)
        ]
        settings = dict(
            template_path=os.path.join(os.path.dirname(__file__), "templates"),
            static_path=os.path.join(os.path.dirname(__file__), "static"),
            debug=True,
        )
        tornado.web.Application.__init__(self, handlers, **settings)

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("hello world!")

def main():
    tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(BetterData())
    print("Tornado started on %s"%(options.port))
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    main()
