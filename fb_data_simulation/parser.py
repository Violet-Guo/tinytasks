from Queue import Queue
from task import *
from data_simulation import *

import math

class Parser:
    '''
    A parser that takes in a file and converts it into 
    a queue of tasks.
    '''

    def __init__(self, disk_throughput, network_bandwidth):
        self.disk_throughput = disk_throughput
        self.network_bandwidth = network_bandwidth

    def parse_tasks(self, data_file):
        tasks = Queue()
        for line in data_file:
            split_line = line.split("\t")
            job_type = split_line[2]
            if job_type == 'MapAttempt' or job_type == 'ReduceAttempt':
                job_name = split_line[1]
                cpu_time = int(split_line[11])
                if job_type == 'MapAttempt':
                    input_size = split_line[12] 
                    output_size = split_line[15] 
                    read_speed = self.disk_throughput
                    write_speed = self.disk_throughput
                    if cpu_time != '' and input_size != '' and output_size != '':
                        new_task = MapTask(job_name, math.ceil(read_time_microseconds(input_size, read_speed)), cpu_time,\
                            math.ceil(read_time_microseconds(output_size, write_speed)))
                        logging.debug("Adding " + str(new_task))
                        tasks.put(new_task)
                else:
                    input_size = split_line[10] 
                    output_size = split_line[13]
                    read_speed = self.network_bandwidth
                    write_speed = self.disk_throughput 
                    if cpu_time != '' and input_size != '' and output_size != '':
                        new_task = ReduceTask(job_name, math.ceil(read_time_microseconds(input_size, read_speed)), cpu_time,\
                            math.ceil(read_time_microseconds(output_size, write_speed)))
                        logging.debug("Adding " + str(new_task))
                        tasks.put(new_task)
        return tasks

def read_time_microseconds(size, rate=10.0):
    ''' 
    Given a size in bytes, converts it to the number of microseconds it'd take 
    to process that size of data. Default rate is 10 MB/ sec.
    '''
    size = int(size)
    result = size / (10.0**6)
    result = (result / rate) * 1000000
    return result