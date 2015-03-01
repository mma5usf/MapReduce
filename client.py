import sys
import gevent
import zerorpc

class Client(object):
	def __init__(self, addr, code_file, split_size, num, input_file, output_file):
		'''filename is the file of the mapreduce code'''
        self.master_addr = addr 
        #self.mr_job["code_file"] = code_file
        self.mr_job["split_size"] = split_size
        slef.mr_job["num_reducer"] = num
        slef.mr_job["input_file"] = input_file
        self.mr_job["output_file"] = output_file

        with open(code_file, 'r') as mapreduce_file:
            self.code = mapreduce_file.read()
        
    def get_code():
    	return self.code

if __name__ == '__main__':
    
    addr = sys.argv[1]
    code_file = sys.argv[2]
    split_size = int(sys.argv[3])
    num = int(sys.argv[4])
    input_file = sys.argv[5]
    output_file = sys.argv[6]

    client = Client(addr, code_file, split_size, num, input_file, output_file)

    c.zerorpc.Client()
    c.connect('tcp://' + addr)
    mr_code = client.get_code()
    c.mr_job(mr_code, Client.mr_job)







