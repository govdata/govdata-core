#!/usr/bin/env python

import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
import api
import os
from multiprocessing import Pool, Queue

from tornado.options import define, options

define("port", default=8888, help="run on the given port", type=int)
define("num_threads", default=4, help="number of threads in the pool", type=int)

class GovLove(tornado.web.Application):
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
            pool=Pool(options.num_threads), 
            queue=Queue()
        )
        tornado.web.Application.__init__(self, handlers, **settings)

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("hello world!")

def main():
    tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(GovLove())
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    main()
