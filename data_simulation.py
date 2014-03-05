import argparse
import numpy
import matplotlib.pyplot as plt
import math
from Queue import Queue
from task import *
from machine import *

class Simulator:

	def __init__(self, num_machines, num_slots, disk_throughput, network_bandwidth, tasks):
		self.num_machines = num_machines
		self.num_slots = num_slots
		self.disk_throughput = disk_throughput
		self.network_bandwidth = network_bandwidth
		self.tasks = tasks
		self.machines = list()
		count = 0
		while count < self.machines:
			self.machines.append(Machine(num_slots, Queue()))
			count += 1
		self.initialize_machines()

	def initialize_machines(self):
		for machine in self.machines():
			if self.tasks.qsize() == 0:
				return
			if self.all_machines_full():
				return
			task = self.tasks.get()
			machine.add_task(task)

	def all_machines_full(self):
		for machine in self.machines:
			if not machine.is_full():
				return False
		return True

	def run(self):
		result = {NETWORK_STAGE:{}, CPU_STAGE:{}, DISK_STAGE:{}}
		for stage in result:
			new_dict = {}
			count = 0
			while count <= args.n:
				new_dict[count] = 0
				count += 1
			result[stage] = new_dict
		while not self.simulation_finished(): #Need each machine to be done AND tasks list to be empty
			for machine in self.machines:
				stage_counts = machine.run()
				for stage in stage_counts.keys():
					count = stage_counts[stage]
					result[stage][count] += 1
		plot_results(result)

	def simulation_finished(self):
		for machine in self.machines:
			if not machine.is_empty():
				return False
		return self.tasks.qsize() == 0


def plot_results(result):
	input_sum = sum(result[NETWORK_STAGE].values()) + 0.0
	cpu_sum = sum(result[CPU_STAGE].values()) + 0.0
	output_sum = sum(result[DISK_STAGE].values()) + 0.0
	plt.plot(result[NETWORK_STAGE].keys(), reduce_sums(map(lambda x: x/input_sum, result[NETWORK_STAGE].values())), 
		result[CPU_STAGE].keys(), reduce_sums(map(lambda x: x/cpu_sum, result[CPU_STAGE].values())),
		result[DISK_STAGE].keys(), reduce_sums(map(lambda x: x/output_sum, result[DISK_STAGE].values())))
	plt.show()

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
			job_name = split[1]
			cpu_time = split_line[11]
			if job_type == 'MapAttempt':
				input_time = 
				output_time = 
				read_speed = disk_throughput
				write_speed = disk_throughput
			else:
				input_size = 
				output_size =
				read_speed = network_bandwidth
				write_speed = disk_throughput 
			if cpu_time != '' and input_size != '' and output_size != '':
				new_task = Task(job_name, cpu_time, read_time_milliseconds(input_size, read_speed), \
					read_time_milliseconds(output_size, write_speed)) #TODO: Change this
				tasks.put(new_task)
	return tasks

def read_time_milliseconds(size, rate=10.0):
	''' 
	Given a size in bytes, converts it to the number of milliseconds it'd take 
	to process that size of data. Default rate is 10 MB/ sec.
	'''
	result = size / (10.0**6)
	result = (result / rate) * 1000
	return result

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("--s", default=1, type=int, help="number of slots per machine")
	parser.add_argument("--m", default=1, type=int, help="number of machines")
	parser.add_argument("--d", default=10, type=int, help="disk throughput in MB/s")
	parser.add_argument("--n", default=10, type=int, help="network bandwidth in MB/s")
	parser.add_argument("file", type=str, help="data file path")
	args = parser.parse_args()
	data_file = open(args.file)
	tasks = parse_tasks(data_file, args.d, args.n)
	simulator = Simulator(args.m, args.s, args.d, args.n, tasks)
	simulator.run()
