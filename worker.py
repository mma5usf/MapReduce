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

import logging
import sys
import gevent
import zerorpc

from gevent.event import Event

class Worker(object):

    def __init__(self, addr, config_file='config'):
        self.addr = addr

    def start(self):
        ####################################################################
        #
        #We will try to add some ssh code to start the workers automatically
        #
        ####################################################################
        pass

        ###########################################
        #self.pool.spawn(client_task).join()
        #Access client task
        ###########################################
        

    def are_you_there(self):
        ans = {}
        ans["addr"] = self.addr

        ############################################
        # add otehr ping response informations here
        ############################################
        return ans

    def get_chunk(file_name, split_size, chunk_index):
        pass



if __name__ == '__main__':
    
    logging.basicConfig(level=logging.INFO)
    
    addr = sys.argv[1]
    worker = Worker(addr)
    s = zerorpc.Server(worker)
    s.bind('tcp://' + addr)
    # Start server
    s.run()
    worker.start()
