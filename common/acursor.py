# Copyright 2009-2010 10gen, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Cursor class to iterate over Mongo query results."""

import struct
import warnings
from pymongo import helpers
import pymongo.message as Message
from pymongo.code import Code
from pymongo.errors import (InvalidOperation,
                            AutoReconnect)
from pymongo.son import SON
import pymongo as pm
import pymongo.json_util
from pymongo.objectid import ObjectId
import socket
import tornado.httpclient
import tornado.web
import tornado.iostream
import os
import json

from common.utils import IsFile, listdir, is_string_like, ListUnion, Flatten, is_num_like, uniqify
import functools


_QUERY_OPTIONS = {
    "tailable_cursor": 2,
    "slave_okay": 4,
    "oplog_replay": 8,
    "no_timeout": 16
}

class Cursor(object):

    """A cursor / iterator over Mongo query results.
    """

    def __init__(self,collection, 
                 spec, 
                 fields, 
                 skip, 
                 limit, 
                 slave_okay,
                 timeout, 
                 tailable, 
                 snapshot=False,
                 _IOStream=None, 
                 _must_use_master=False, 
                 _is_command=False,
                 as_class=None,
                 sort = None):

        if as_class is None:
            as_class = collection.database.connection.document_class

        self.__collection = collection
        self.__spec = spec
        self.__fields = fields
        self.__skip = skip
        self.__limit = limit
        self.__slave_okay = slave_okay
        self.__timeout = timeout
        self.__tailable = tailable
        self.__snapshot = snapshot
        self.__ordering = sort and helpers._index_document(sort) or None
        self.__explain = False
        self.__hint = None
        self.__IOStream = _IOStream
        self.__must_use_master = _must_use_master
        self.__is_command = _is_command
        self.__as_class = as_class

        self.__data = []
        self.__id = None
        self.__connection_id = None
        self.__retrieved = 0
        self.__killed = False

 
    @property
    def collection(self):
        """The :class:`~pymongo.collection.Collection` that this
        :class:`Cursor` is iterating.

        .. versionadded:: 1.1
        """
        return self.__collection

    def __del__(self):
        if self.__id and not self.__killed:
            self.__die()

    def rewind(self):
        """Rewind this cursor to it's unevaluated state.

        Reset this cursor if it has been partially or completely evaluated.
        Any options that are present on the cursor will remain in effect.
        Future iterating performed on this cursor will cause new queries to
        be sent to the server, even if the resultant data has already been
        retrieved by this cursor.
        """
        self.__data = []
        self.__id = None
        self.__connection_id = None
        self.__retrieved = 0
        self.__killed = False

        return self

    def clone(self):
        """Get a clone of this cursor.

        Returns a new Cursor instance with options matching those that have
        been set on the current instance. The clone will be completely
        unevaluated, even if the current instance has been partially or
        completely evaluated.
        """
        copy = Cursor(self.__collection, self.__spec, self.__fields,
                      self.__skip, self.__limit, self.__slave_okay,
                      self.__timeout, self.__tailable, self.__snapshot)
        copy.__ordering = self.__ordering
        copy.__explain = self.__explain
        copy.__hint = self.__hint
        copy.__IOStream = self.__IOStream
        return copy

    def __die(self):
        """Closes this cursor.
        """
        if self.__id and not self.__killed:
            connection = self.__collection.database.connection
            if self.__connection_id is not None:
                connection.close_cursor(self.__id, self.__connection_id)
            else:
                connection.close_cursor(self.__id)
        self.__killed = True

    def __query_spec(self):
        """Get the spec to use for a query.
        """
        if self.__is_command or "$query" in self.__spec:
            return self.__spec
        spec = SON({"$query": self.__spec})
        if self.__ordering:
            spec["$orderby"] = self.__ordering
        if self.__explain:
            spec["$explain"] = True
        if self.__hint:
            spec["$hint"] = self.__hint
        if self.__snapshot:
            spec["$snapshot"] = True
        return spec

    def __query_options(self):
        """Get the query options string to use for this query.
        """
        options = 0
        if self.__tailable:
            options |= _QUERY_OPTIONS["tailable_cursor"]
        if self.__slave_okay:
            options |= _QUERY_OPTIONS["slave_okay"]
        if not self.__timeout:
            options |= _QUERY_OPTIONS["no_timeout"]
        return options

    def __check_okay_to_chain(self):
        """Check if it is okay to chain more options onto this cursor.
        """
        if self.__retrieved or self.__id is not None:
            raise InvalidOperation("cannot set options after executing query")

    def limit(self, limit):
        """Limits the number of results to be returned by this cursor.

        Raises TypeError if limit is not an instance of int. Raises
        InvalidOperation if this cursor has already been used. The last `limit`
        applied to this cursor takes precedence.

        :Parameters:
          - `limit`: the number of results to return

        .. mongodoc:: limit
        """
        if not isinstance(limit, int):
            raise TypeError("limit must be an int")
        self.__check_okay_to_chain()

        self.__limit = limit
        return self

    def skip(self, skip):
        """Skips the first `skip` results of this cursor.

        Raises TypeError if skip is not an instance of int. Raises
        InvalidOperation if this cursor has already been used. The last `skip`
        applied to this cursor takes precedence.

        :Parameters:
          - `skip`: the number of results to skip
        """
        if not isinstance(skip, (int, long)):
            raise TypeError("skip must be an int")
        self.__check_okay_to_chain()

        self.__skip = skip
        return self


    def sort(self, key_or_list, direction=None):
        """Sorts this cursor's results.

        Takes either a single key and a direction, or a list of (key,
        direction) pairs. The key(s) must be an instance of ``(str,
        unicode)``, and the direction(s) must be one of
        (:data:`~pymongo.ASCENDING`,
        :data:`~pymongo.DESCENDING`). Raises
        :class:`~pymongo.errors.InvalidOperation` if this cursor has
        already been used. Only the last :meth:`sort` applied to this
        cursor has any effect.

        :Parameters:
          - `key_or_list`: a single key or a list of (key, direction)
            pairs specifying the keys to sort on
          - `direction` (optional): only used if `key_or_list` is a single
            key, if not given :data:`~pymongo.ASCENDING` is assumed
        """
        self.__check_okay_to_chain()
        keys = helpers._index_list(key_or_list, direction)
        self.__ordering = helpers._index_document(keys)
        return self

 
        

    def hint(self, index):
        """Adds a 'hint', telling Mongo the proper index to use for the query.

        Judicious use of hints can greatly improve query
        performance. When doing a query on multiple fields (at least
        one of which is indexed) pass the indexed field as a hint to
        the query. Hinting will not do anything if the corresponding
        index does not exist. Raises
        :class:`~pymongo.errors.InvalidOperation` if this cursor has
        already been used.

        `index` should be an index as passed to
        :meth:`~pymongo.collection.Collection.create_index`
        (e.g. ``[('field', ASCENDING)]``). If `index`
        is ``None`` any existing hints for this query are cleared. The
        last hint applied to this cursor takes precedence over all
        others.

        :Parameters:
          - `index`: index to hint on (as an index specifier)
        """
        self.__check_okay_to_chain()
        if index is None:
            self.__hint = None
            return self

        self.__hint = helpers._index_document(index)
        return self

    def where(self, code):
        """Adds a $where clause to this query.

        The `code` argument must be an instance of :class:`basestring`
        or :class:`~pymongo.code.Code` containing a JavaScript
        expression. This expression will be evaluated for each
        document scanned. Only those documents for which the
        expression evaluates to *true* will be returned as
        results. The keyword *this* refers to the object currently
        being scanned.

        Raises :class:`TypeError` if `code` is not an instance of
        :class:`basestring`. Raises
        :class:`~pymongo.errors.InvalidOperation` if this
        :class:`Cursor` has already been used. Only the last call to
        :meth:`where` applied to a :class:`Cursor` has any effect.

        :Parameters:
          - `code`: JavaScript expression to use as a filter
        """
        self.__check_okay_to_chain()
        if not isinstance(code, Code):
            code = Code(code)

        self.__spec["$where"] = code
        return self
        
        

    def __send_message(self, message):
        """Send a message on the given socket in a nonblocking fashion -- nothing is returned
        """
        IOStream = self.__IOStream
        (request_id, data) = message        
        IOStream.write(data)



    def next(self,callback):

        nextback_curry = functools.partial(self.nextback,callback)
        
        IOStream = self.__IOStream
       
        if not len(self.__data):
            self._refresh();
            request_id = self.__request_id
            self.__receive_message_on_stream(1, request_id, IOStream, nextback_curry)      
        else:
            self.nextback(callback,None)

      

    def _refresh(self):
        """Refreshes the cursor with more data from Mongo.

        Returns the length of self.__data after refresh. Will exit early if
        self.__data is already non-empty. Raises OperationFailure when the
        cursor cannot be refreshed due to an error on the query.
        """
        
        if len(self.__data) or self.__killed:
            return
            
        if self.__id is None:
            message = Message.query(self.__query_options(),
                              self.__collection.full_name,
                              self.__skip, self.__limit,
                              self.__query_spec(), self.__fields)
                 
            self.__request_id = message[0]  
            self.__send_message(message)
        elif self.__id:
            limit = 0
            if self.__limit:
                if self.__limit > self.__retrieved:
                    limit = self.__limit - self.__retrieved
                else:
                    self.__killed = True

            message = Message.get_more(self.__collection.full_name,
                                 limit, self.__id)                      
            self.__request_id = message[0]
            print(self.__id)
            self.__send_message(message)

        


    @property
    def alive(self):
        """Does this cursor have the potential to return more data?

        This is mostly useful with `tailable cursors
        <http://www.mongodb.org/display/DOCS/Tailable+Cursors>`_
        since they will stop iterating even though they *may* return more
        results in the future.

        .. versionadded:: 1.5
        """
        return bool(len(self.__data) or (not self.__killed))

    def __iter__(self):
        return self
  
    def nextback(self,callback,response):
       
        if response is not None:
            if isinstance(response, tuple):
                (connection_id, response) = response
            else:
                connection_id = None
    
            self.__connection_id = connection_id
    
            try:
                response = helpers._unpack_response(response, self.__id,as_class=self.__as_class)
            except AutoReconnect:
                db.connection._reset()
                raise
                
     
            self.__id = response["cursor_id"]
            
            # starting from doesn't get set on getmore's for tailable selfs
            if not self.__tailable:
                assert response["starting_from"] == self.__retrieved
    
            self.__retrieved += response["number_returned"]

            self.__data = response["data"]
         
            
            self.__id = response["cursor_id"]
    
    
        if self.__limit and self.__id and self.__limit <= self.__retrieved or not self.__id:
            self.__die()
    
        db = self.__collection.database
        
        if len(self.__data):
            callback(db._fix_outgoing(self.__data.pop(0), self.__collection)) 
        else:
            callback(StopIteration)
  
       
    def __receive_message_on_stream(self, operation, request_id, IOStream,callback):
        """Receive a message in response to `request_id` on `iostream`.

        Returns the response data with the header removed.
        """
        receive_body_curry = functools.partial(receive_body_on_stream,operation,request_id,IOStream,callback)
        IOStream.read_bytes(16,receive_body_curry)
    
    



class AsyncCursorHandler(tornado.web.RequestHandler):


    def organize_fields(self,fields):
        pass
        
        
    def begin(self):    
        if self.jsonPcallback:
            self.write(self.jsonPcallback + '(')

        if self.returnObj:
            self.data = []
 
 
    def end(self):

        if self.returnObj and not self.stream:
            self.write(json.dumps(self.data,default=pm.json_util.default))

        if self.jsonPcallback:
            self.write(')')
           
        self.finish()
        

    def add_handler(self,collection,spec,fields,skip,limit,callback,_must_use_master=False,_is_command=False):
    
        if fields is not None:
            if not fields:
                fields = ["_id"]
            fields = helpers._fields_list_to_dict(fields)
    
        slave_okay = collection.database.connection.slave_okay
        timeout = True
        tailable = False
        
        io_loop = self.settings['io_loop']
    
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        IOStream = tornado.iostream.IOStream(sock,io_loop)
        self.__IOStream = IOStream
        
                   
        self.cursor = Cursor(collection,
                        spec,
                        fields,
                        skip,
                        limit,
                        slave_okay, 
                        timeout,
                        tailable,
                        _IOStream=IOStream,
                        _must_use_master=_must_use_master, 
                        _is_command=_is_command,
                        sort = self.__ordering.items() if self.__ordering else None
                        )


        callback = functools.partial(callback,self)
        
        start_callback = functools.partial(start,self,callback)

        IOStream.connect(('localhost',27017),callback=start_callback)
       
     
    
    
    def add_command_handler(self,collection,command,processor):
    
        cmdCollection = collection.database['$cmd']
        
        callback = functools.partial(processor_handler,processor)
        
        self.add_handler(cmdCollection,command,None,0,-1,callback,_must_use_master=True,_is_command= True)
        

    def add_async_cursor(self,collection,querySequence):
        
        if not hasattr(self,'processor'):
            self.processor = None
        if not hasattr(self,'stream'):
            self.stream = False
        if not hasattr(self,'returnObj'):
            self.returnObj = True
        
        self.writing = False
        
        self.begin()        
        
        R = collection 
        
        self.__ordering = None
        for (a,(p,k)) in querySequence[:-1]:
            assert a not in ['count','distinct','find_one','group']
            k = dict([(str(kk),v) for (kk,v) in k.items()])
            R = getattr(R,a)(*p,**k)   
            self.__ordering = R._Cursor__ordering
            
        (act,(arg,karg)) = querySequence[-1]
    
    
        if act == 'count':
        
            spec,fields,skip,limit = R._Cursor__spec,R._Cursor__fields,R._Cursor__skip,R._Cursor__limit
            
            command = {"query":spec,"fields":fields,"count":collection.name}
            
            with_limit_and_skip = karg.get('with_limit_and_skip',False)
            
            if with_limit_and_skip:
                if limit:
                    command["limit"] = limit
                if skip:
                    command["skip"] = handler.__skip           
                    
            self.add_command_handler(collection,command,process_count)        
    
    
        elif act == 'distinct':
        
            spec = R._Cursor__spec
            
            command = {"distinct":collection.name,"key":arg[0]}
            if spec:
                command['query'] = spec
            
            self.add_command_handler(collection,command,process_distinct)
       
        elif act == 'find_one':
        
            spec = arg[0]
            fields = karg.get('fields',None)
            
            if spec is None:
                spec = SON()
            elif isinstance(spec, ObjectId):
                spec = SON({"_id": spec})
            
            callback = functools.partial(processor_handler,None)
            
            self.add_handler(collection,spec,fields,0,-1,callback)
            
        elif act == 'group':
        
            key,condition,initial,reduce = arg
            finalize = karg.get('finalize')
            group = {}
            if isinstance(key, basestring):
                group["$keyf"] = Code(processJSValue(key,collection))
            elif key is not None:
                group = {"key": collection._fields_list_to_dict(key)}
            group["ns"] = collection.name
            group["$reduce"] = Code(processJSValue(reduce,collection))
            group["cond"] = condition
            group["initial"] = initial
            if finalize is not None:
                group["finalize"] = Code(finalize)            
                
            command = {"group":group}
    
            self.add_command_handler(collection,command,process_group) 
            
        else:
            R = getattr(R,act)(*arg,**karg)
            self.__ordering = R._Cursor__ordering

            if isinstance(R,pm.cursor.Cursor):
    
                if self.stream:
                    self.write('[')
                    
                spec,fields,skip,limit = R._Cursor__spec,R._Cursor__fields,R._Cursor__skip,R._Cursor__limit
           
                self.organize_fields(fields)

                self.add_handler(collection,spec,fields,skip,limit,loop)
         
            else:    
                if self.stream:
                    self.write(json.dumps(R,default=pm.json_util.default))
                self.end()
               
           
    def done(self):
        self.__IOStream.close()        
        self.end()    
    
    
def processor_handler(processor,handler,resp):
   
    if resp == StopIteration:
        r = None
    else:
        r = resp
        
    if processor != None:
        result = processor(r)
    else:
        result = r
    
    if handler.stream:
        handler.write(json.dumps(result,default=pm.json_util.default))
    if handler.returnObj:
        handler.data = result
        
    handler.done()    
    

def process_count(r):

    if r.get("errmsg", "") == "ns missing":
        result = 0
    elif r is None:
        result = 0
    else:
        result = int(r["n"])
        
    return result
       
def process_distinct(r):
    if r is not None:
        return r["values"]
    else:
        return []
    
def process_group(r):
    if r is not None:
        return r["retval"]    
    else:
        return None

             
             
def receive_body_on_stream(operation,request_id,IOStream,callback,header):
                            
        length = struct.unpack("<i", header[:4])[0]
        assert request_id == struct.unpack("<i", header[8:12])[0], \
            "ids don't match %r %r" % (request_id,
                                       struct.unpack("<i", header[8:12])[0])
        assert operation == struct.unpack("<i", header[12:])[0]

        IOStream.read_bytes(length-16,callback)
        


def loop(handler,r):
      
    cursor = handler.cursor
    db = cursor._Cursor__collection.database

    if r == StopIteration:
         cursor._Cursor__die()
    else:
        processor = handler.processor 
        collection = cursor._Cursor__collection
        
        while True:
              
            if processor:
                r = processor(r,handler,collection)
                 
            if handler.stream:
                handler.write((',' if handler.writing else '') + json.dumps(r,default=pm.json_util.default))
                handler.flush()
                if not handler.writing:
                    handler.writing = True
            if handler.returnObj:
                handler.data.append(r)
                
            if len(cursor._Cursor__data):
                r = db._fix_outgoing(cursor._Cursor__data.pop(0), collection)
            else:
                break
                        
    if cursor.alive:
        cursor.next(functools.partial(loop,handler))      
  
    else:
        if handler.stream:
            handler.write(']')
        
        handler.done()


        


def start(handler,callback):
    handler.cursor.next(callback)        
    

