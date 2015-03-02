import sys

#import mapreduce_class as mapreduce
import mapreduce

class WordCountMap(mapreduce.Map):

    def map(self, k, v):
        words = v.split()
        for w in words:
            self.emit(w, '1')

class WordCountReduce(mapreduce.Reduce):

    def reduce(self, k, vlist):
        count = 0
        for v in vlist:
            count = count + int(v)
        self.emit(k + ':' + str(count))



def map(filename):
    
    ############################
    #This part should be change not read the whole file but the relevent parts of the file
    ############################

    f = open(filename)
    input_list = f.readlines()
    f.close()
    # Map phase
    mapper = WordCountMap()
    for i, v in enumerate(input_list):
        mapper.map(i, v)

    # Sort intermediate keys
    table = mapper.get_table()
    keys = table.keys()
    keys.sort()
    print table
 

def reduce():

    ###############################################
    #Get map result from all the mappers
    #Combine the map result together into one table
    #We need this table
    ################################################

    reducer = WordCountReduce()
    for k in keys:
        reducer.reduce(k, table[k])
    result_list = reducer.get_result_list()
    


