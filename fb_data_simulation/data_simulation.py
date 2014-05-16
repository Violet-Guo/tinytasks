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

RESULT_FOLDER = "results/test_all_data/"
RESULT_FILENAME =  RESULT_FOLDER + "machine"

class Simulator:

	def __init__(self, num_machines, num_slots, tasks, disk_slots, cpu_slots, network_slots):
		self.num_machines = num_machines
		self.num_slots = num_slots
		self.tasks = tasks
		self.event_handler = EventHandler()
		self.machines = list()
		count = 0
		while count < self.num_machines:
			self.machines.append(Machine(count, num_slots, self.event_handler, self.tasks, disk_slots, cpu_slots, network_slots))
			count += 1

	def assign_tasks_to_machines(self):
		for machine in self.machines:
			machine.start()

	def run(self):
		self.assign_tasks_to_machines()
		self.event_handler.run()
		total_time = self.event_handler.curr_time / 1000.0
		print "FINISHED: total time elapsed (in milliseconds)- ", total_time
		self.process_task_averages()
		self.save_counts()	
		self.plot_graphs()

	def run_no_plot(self):
		self.assign_tasks_to_machines()
		self.event_handler.run()
		total_time = self.event_handler.curr_time / 1000.0
		print "FINISHED: total time elapsed (in milliseconds)- ", total_time
		self.save_counts()	

	def test_run(self):
		self.run_no_plot()
		result = []
		for machine in self.machines:
			result.append(machine.total_counts)
		return result

	def print_counts(self):
		for machine in self.machines:
			print( "Machine " + str(machine.machine_num) + " " + str(machine.total_counts))

	def process_task_averages(self):
		task_times = self.event_handler.task_times
		cumm_times = 0
		count = 0.0
		for job in task_times:
			job_times = task_times[job]
			if START in job_times and END in job_times:
				diff = job_times[END] - job_times[START]
				assert diff > 0
				diff = diff / 1000.0
				cumm_times += diff
				count += 1
		if count > 0:
			average = cumm_times / count
			print "Average time of jobs is (in milliseconds): ", str(average)
			return average

	def save_counts(self):
		with open("results/test_all_data/counts.txt", 'w') as f:
			total_time = self.event_handler.curr_time / 1000.0
			total_time_str =  "FINISHED: total time elapsed (in milliseconds)- " + str(total_time)
			f.write(total_time_str)
			for machine in self.machines:
				new_machine = "Machine " + str(machine.machine_num) + " " + str(machine.total_counts) + "\n\n"
				f.write(new_machine)

	def plot_graphs(self):
		for machine in self.machines:
			logging.debug("Machine counts = " + str(machine.total_counts))
			plot_results(machine.total_counts, machine.machine_num)

def plot_results(result, machine_num):
	input_sum = sum(result[NETWORK_STAGE].values()) + 0.0
	cpu_sum = sum(result[CPU_STAGE].values()) + 0.0
	output_sum = sum(result[DISK_STAGE].values()) + 0.0
	p1, = plt.plot(result[NETWORK_STAGE].keys(), reduce_sums(map(lambda x: x/input_sum, result[NETWORK_STAGE].values())))
	p2, = plt.plot(result[CPU_STAGE].keys(), reduce_sums(map(lambda x: x/cpu_sum, result[CPU_STAGE].values())))
	p3, = plt.plot(result[DISK_STAGE].keys(), reduce_sums(map(lambda x: x/output_sum, result[DISK_STAGE].values())))
	plt.legend([p1, p2, p3], ["network", "cpu", "disk_"], loc=4)
	filename = RESULT_FILENAME +  str(machine_num) + ".png"
	plt.savefig(filename)
	plt.close()

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
	parser = Parser(args.throughput, args.bandwidth)
	tasks = parser.parse_tasks(args.file)
	simulator = Simulator(args.m, args.s, tasks, args.disk, args.cpu, args.bandwidth)
	simulator.run()

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("--debug", action="store_true", help="print debugging statements")
	parser.add_argument("--s", default=1, type=int, help="number of slots per machine")
	parser.add_argument("--disk", default=1, type=int, help="number of disk slots per machine")
	parser.add_argument("--cpu", default=1, type=int, help="number of cpu slots per machine")
	parser.add_argument("--network", default=1, type=int, help="number of network slots per machine")
	parser.add_argument("--m", default=1, type=int, help="number of machines")
	parser.add_argument("--throughput", default=10, type=int, help="disk throughput in MB/s")
	parser.add_argument("--bandwidth", default=10, type=int, help="network bandwidth in MB/s")
	parser.add_argument("file", type=str, help="data file path")
	args = parser.parse_args()
	simulate(args)
