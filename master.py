# master.py - Master used for MapReduce
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
# $ cat config
# 127.0.0.1:9000
# 127.0.0.1:9001
# 127.0.0.1:9002
#
# You can kill (CTRL-C) a server to see the election run again.
# This file was change from bully.py


import sys
import gevent
import zerorpc

from gevent.event import Event

class Master(object):

    def __init__(self, addr, config_file='config'):
        self.addr = addr

        # The index of alive workers
        self.workers = []

        # the worker server list from config file
        self.servers = []

        # the connection list of the workers
        self.connections = []

        f = open(config_file, 'r')
        self.servers = [l.rstrip() for l in f.readlines() if l.rstrip() != addr]
        print 'My addr: %s' % (self.addr)
        print 'Server list: %s' % (str(self.servers))

        # number of worker servers
        self.n = len(self.servers)

        for i, server in enumerate(self.servers):
            c = zerorpc.Client(timeout=1)
            c.connect('tcp://' + server)
            self.connections.append(c)
            #print "connetions: " + server

    def start(self):
        ####################################################################
        #
        #We will try to add some ssh code to start the workers automatically
        #
        ####################################################################
        self.setup()
        self.check_greenlet = gevent.Greenlet.spawn(self.ping).join()

        ###########################################
        #self.pool = gevent.pool.Group()
        #self.pool.spawn(client_task).join()
        #Access client task
        ###########################################
        
        

    def setup(self):
        '''After the set up process, start the working process'''
        self.get_workers()
        self.worker_num = len(self.workers)
        if self.worker_num == 0:
            print "No workers running. Please check your servers!!!"
            sys.exit()

        print "Setup process succeed  :-)"
        print "Worker list:"
        for i in self.workers:
            print self.servers[i]
        print "Now you can run your MapReduce jobs."




    def get_workers(self):
        '''Get all alive workers'''

        for j in set(range(self.n)):
            try:
                ans = self.connections[j].are_you_there()
                if not ans:
                    continue
                self.workers.append(j)
            except zerorpc.TimeoutExpired:
                continue

    def ping(self):
        '''Called periodically, check the status of the workers'''

        while True:
            gevent.sleep(1)
            
            for i in self.workers:
                try:
                    ans = self.connections[i].are_you_there()
                    print "Get response from: " + ans["addr"]
                    if not ans:
                        print "No response from " + self.servers[i]
                    ################################
                    # worker_fail()
                    #################################
                except zerorpc.TimeoutExpired:
                    print "Timeout: " + self.servers[i]
                    continue
                    ################################
                    # worker_fail()
                    #################################

    def are_you_there(self):
        ans = {}
        ans["addr"] = self.addr

        ############################################
        # add otehr ping response informations here
        ############################################
        return ans

    def assign_tasks(self):
        '''Deploy mapreduce jobs to workers'''

        for i in self.workers:
            try:
                self.connections[i].get_job(self.mr_job)
            except zerorpc.TimeoutExpired:
                print "Timeout: " + self.servers[i]
                ##################################
                #handle worker failure
                ##################################
                continue


    def mr_job(self, mr_code, job_info):
        self.mr_job = job_info
        self.mr_job["code"] = mr_code
        print self.mr_job

        self.assign_tasks()
        gevent.sleep(0)
        


if __name__ == '__main__':
    
    addr = sys.argv[1]
    master = Master(addr)

    s = zerorpc.Server(master)
    s.bind('tcp://' + addr)
    # Start server
    gevent.Greenlet.spawn(s.run)
    master.start()


