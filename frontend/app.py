import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.options
import tornado.autoreload
from tornado.options import define, options

import uimodules
from handlers import *

define("port", default=8000, help="run on the given port", type=int)
define("api_url", default="http://ec2-67-202-31-123.compute-1.amazonaws.com", help="use this api url", type=str)
define("per_page", default=30, help="items per page in solr", type=int)

class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/", FindHandler),
            (r"/get", GetHandler),
            (r"/find", FindPartialHandler)
        ]
        settings = {
            "template_path": os.path.join(os.path.dirname(__file__), "templates"),
            "static_path": os.path.join(os.path.dirname(__file__), "static"),
            "ui_modules": uimodules,
            "xsrf_cookies": True,
            "cookie_secret": "13oETzXKQAGaYdkL5gEmGeJJFuYh7EQnp2XdTP1o/Vo=",
        }
        tornado.web.Application.__init__(self, handlers, **settings)

def main():
    tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(Application())
    print("Starting on localhost:%s"%(options.port))
    http_server.listen(options.port)
    tornado.autoreload.start()
    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    main()