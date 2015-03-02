# worker.py - Master used for MapReduce
#
# To run this code you need to install ZeroMQ, Gevent, and ZeroRPC
#
# http://zeromq.org
# http://www.gevent.org
# http://zerorpc.dotcloud.com
# 
# These will require some additional libraries
# In order to run an election you need to create a config file:
# 
# $python worker.py 127.0.0.1:9000
# 
# You can kill (CTRL-C) a server to see the election run again.
# This file was change from bully.py

import sys
import imp
import gevent
import zerorpc

from gevent.event import Event

class Worker(object):

    def __init__(self, addr, config_file='config'):
        self.addr = addr

        


    def start(self, master_addr):
        ####################################################################
        #
        #We will try to add some ssh code to start the workers automatically
        #
        ####################################################################
        self.master_addr = master_addr
        #self.c = zerorpc.Client()
        #self.c.connect('tcp://' + master_addr)

        ###########################################
        #self.pool.spawn(client_task).join()
        #Access client task
        ###########################################

    def get_job(self, mr_job, role, worker_index):
        '''Get the MapReduce job informantion'''
        self.mr_job = mr_job
        self.role = role
        self.index = worker_index
        #print self.mr_job
        self.m = self.importCode("client_job")
    
    def get_chunk_index():
        '''Get the index of the chunk which this worker should handle'''


    def start_mapper(self):
        '''map_result = {chunk_index : map(chunk)}'''

        self.map_result = {}
        '''start the map job'''
        f = open(self.mr_job["input_file"])
        for i, chunk in enumerate(get_chunks(f, self.mr_job["split_size"], self.mr_job["file_type"])):
            if i % self.mr_job["mapper_num"] == self.index
                self.map_result[i] = self.m.map(chunk)
            self.chunk_num = i + 1

        ########################################################
        ## Here we should use different hash function 
        ## to split the result of map task to for different reducers
        ## Hash func should defined as a user interface in wordcount.py
        ########################################################


    def start_reducer(self):
        '''start the reduce job'''
        pass

    def are_you_there(self):
        ans = {}
        ans["addr"] = self.addr

        ############################################
        # add otehr ping response informations here
        ############################################
        return ans

    def importCode(self, name):
        '''
        Reference: http://code.activestate.com/recipes/82234-importing-a-dynamically-generated-module/
        '''

        module = imp.new_module(name)
        exec self.mr_job["code"] in module.__dict__
        return module

    def get_chunks(file_object, split_size = 1024, data_type = "lines"):
        """
        This function will return a file_object as chunk list

        Example:
        f = open('filename')
        for chunk in get_chunks(f, 1024, "lines"):
            process_data(chunk)
        """

        if data_type == "binary":
            """Lazy function (generator) to read a file piece by piece.
            Default chunk size: 1k."""
            while True:
                chunk = file_object.read(split_size)
                if not chunk:
                    break
                yield chunk
        elif data_type == "lines":
            while True:
                chunk = list(itertools.islice(file_object, split_size))
                if not chunk:
                    break
                yield chunk



if __name__ == '__main__':
    addr = sys.argv[1]
    worker = Worker(addr)
    s = zerorpc.Server(worker)
    s.bind('tcp://' + addr)
    # Start server
    s.run()
    
