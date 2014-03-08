import argparse
import numpy
import matplotlib.pyplot as plt
import math
from Queue import Queue
from task import *
from machine import *

RESULT_FILENAME = "results/test_two_machines/machine"

class Simulator:

	def __init__(self, num_machines, num_slots, disk_throughput, network_bandwidth, tasks, debug):
		self.debug_flag = debug
		self.num_machines = num_machines
		self.num_slots = num_slots
		self.disk_throughput = disk_throughput
		self.network_bandwidth = network_bandwidth
		self.tasks = tasks
		self.machines = list()
		count = 0
		while count < self.num_machines:
			self.machines.append(Machine(count, num_slots, self.debug, tasks))
			count += 1
		self.assign_tasks_to_machines()

	def assign_tasks_to_machines(self):
		continue_assignment = True
		while continue_assignment:
			continue_assignment = False
			for machine in self.machines:
				if self.tasks.qsize() == 0:
					return
				if not machine.is_full():
					task = self.tasks.get()
					machine.add_task(task)
				if not machine.is_full():
					continue_assignment = True

	def run(self):
		time_count = 0
		continue_simulation = True
		while continue_simulation: #Need each machine to be done AND tasks list to be empty
			continue_simulation = False
			for machine in self.machines:
				if not machine.is_empty():
					machine.run()
				if not machine.is_empty():
					continue_simulation = True
			self.debug("time elapse: " + str(time_count))
			time_count += 1
		print "FINISHED: total time elapsed- ", time_count
		self.plot_graphs()

	def test_run(self):
		time_count = 0
		continue_simulation = True
		while continue_simulation: #Need each machine to be done AND tasks list to be empty
			continue_simulation = False
			for machine in self.machines:
				if not machine.is_empty():
					machine.run()
				if not machine.is_empty():
					continue_simulation = True
			if not continue_simulation:
				break
			time_count += 1
		result = []
		for machine in self.machines:
			result.append(machine.counts)
		return result

	def plot_graphs(self):
		for machine in self.machines:
			self.debug(machine.counts)
			plot_results(machine.counts, machine.machine_num)

	def debug(self, debug_str):
		if self.debug_flag:
			print(debug_str)

def plot_results(result, machine_num):
	input_sum = sum(result[NETWORK_STAGE].values()) + 0.0
	cpu_sum = sum(result[CPU_STAGE].values()) + 0.0
	output_sum = sum(result[DISK_STAGE].values()) + 0.0
	plt.plot(result[NETWORK_STAGE].keys(), reduce_sums(map(lambda x: x/input_sum, result[NETWORK_STAGE].values())), 
		result[CPU_STAGE].keys(), reduce_sums(map(lambda x: x/cpu_sum, result[CPU_STAGE].values())),
		result[DISK_STAGE].keys(), reduce_sums(map(lambda x: x/output_sum, result[DISK_STAGE].values())))
	filename = RESULT_FILENAME +  str(machine_num) + ".png"
	plt.savefig(filename)

def reduce_sums(probabilities):
	count = 0
	curr_sum = 0
	result = []
	while count < len(probabilities):
		curr_sum += probabilities[count]
		result.append(curr_sum)
		count += 1
	return result

def parse_tasks(data_file, disk_throughput, network_bandwidth):
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
				read_speed = disk_throughput
				write_speed = disk_throughput
				if cpu_time != '' and input_size != '' and output_size != '':
					new_task = MapTask(job_name, math.ceil(read_time_milliseconds(input_size, read_speed)), cpu_time,\
						math.ceil(read_time_milliseconds(output_size, write_speed)))
					print(new_task)
					tasks.put(new_task)
			else:
				input_size = split_line[10] 
				output_size = split_line[13]
				read_speed = network_bandwidth
				write_speed = disk_throughput 
				if cpu_time != '' and input_size != '' and output_size != '':
					new_task = ReduceTask(job_name, math.ceil(read_time_milliseconds(input_size, read_speed)), cpu_time,\
						math.ceil(read_time_milliseconds(output_size, write_speed)))
					print(new_task)
					tasks.put(new_task)
	return tasks

def read_time_milliseconds(size, rate=10.0):
	''' 
	Given a size in bytes, converts it to the number of microseconds it'd take 
	to process that size of data. Default rate is 10 MB/ sec.
	'''
	size = int(size)
	result = size / (10.0**6)
	result = (result / rate) * 1000
	return result

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("--debug", action="store_true", help="print debugging statements")
	parser.add_argument("--s", default=1, type=int, help="number of slots per machine")
	parser.add_argument("--m", default=1, type=int, help="number of machines")
	parser.add_argument("--d", default=10, type=int, help="disk throughput in MB/s")
	parser.add_argument("--n", default=10, type=int, help="network bandwidth in MB/s")
	parser.add_argument("file", type=str, help="data file path")
	args = parser.parse_args()
	data_file = open(args.file)
	tasks = parse_tasks(data_file, args.d, args.n)
	simulator = Simulator(args.m, args.s, args.d, args.n, tasks, args.debug)
	simulator.run()
