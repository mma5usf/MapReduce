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

    def get_job(self, mr_job):
        '''Get the MapReduce job informantion'''
        self.mr_job = mr_job
        #print self.mr_job
        self.m = self.importCode("client_job")
        

    def start_mapper(self):
        '''start the map job'''       
        self.m.map(self.mr_job["input_file"])


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

    def get_chunk(file_name, split_size, chunk_index):
        pass



if __name__ == '__main__':
    addr = sys.argv[1]
    worker = Worker(addr)
    s = zerorpc.Server(worker)
    s.bind('tcp://' + addr)
    # Start server
    s.run()
    
