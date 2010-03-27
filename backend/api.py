#!/usr/bin/env python

import tornado.web

class GetHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello, get")

class FindHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello, find")
