import argparse
import matplotlib.pyplot as plt
import math
import logging

from Queue import Queue
from task import *
from machine import *
from task_handler import *
from parser import *
from event_handler import *

RESULT_FILENAME = "results/test_two_machines/machine"

class Simulator:

	def __init__(self, num_machines, num_slots, disk_throughput, network_bandwidth, tasks):
		self.num_machines = num_machines
		self.num_slots = num_slots
		self.disk_throughput = disk_throughput
		self.network_bandwidth = network_bandwidth
		self.tasks = tasks
		self.event_handler = EventHandler()
		self.machines = list()
		count = 0
		while count < self.num_machines:
			self.machines.append(Machine(count, num_slots, self.event_handler, self.tasks))
			count += 1

	def assign_tasks_to_machines(self):
		for machine in self.machines:
			machine.start()

	def run(self):
		self.event_handler.run()
		print "FINISHED: total time elapsed- ", self.event_handler.curr_time
		self.print_counts()	
		#self.plot_graphs()

	def test_run(self):
		self.run()
		result = []
		for machine in self.machines:
			result.append(machine.counts)
		return result

	def print_counts(self):
		for machine in self.machines:
			print( "Machine " + str(machine.machine_num) + " " + str(machine.counts))

	def plot_graphs(self):
		for machine in self.machines:
			logging.debug("Machine counts = " + str(machine.counts))
			plot_results(machine.counts, machine.machine_num)


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
	
def simulate(args):
	level_type = logging.INFO
	if args.debug:
		level_type = logging.DEBUG
	logging.basicConfig(format='%(levelname)s-%(message)s', level=level_type)
	data_file = open(args.file)
	parser = Parser(args.d, args.n)
	tasks = parser.parse_tasks(data_file)
	simulator = Simulator(args.m, args.s, args.d, args.n, tasks)
	simulator.run()

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("--debug", action="store_true", help="print debugging statements")
	parser.add_argument("--s", default=1, type=int, help="number of slots per machine")
	parser.add_argument("--m", default=1, type=int, help="number of machines")
	parser.add_argument("--d", default=10, type=int, help="disk throughput in MB/s")
	parser.add_argument("--n", default=10, type=int, help="network bandwidth in MB/s")
	parser.add_argument("file", type=str, help="data file path")
	args = parser.parse_args()
	simulate(args)