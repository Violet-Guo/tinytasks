import argparse
import numpy
import matplotlib.pyplot as plt
import math
from Queue import Queue
from math import floor

INPUT_STAGE = 0
CPU_STAGE = 1
OUTPUT_STAGE = 2

class Task:
	def __init__(self, job, cpu_time, input_data, output_data):
		self.job = job
		self.cpu_time = cpu_time
		self.input_time = floor(read_time_milliseconds(input_data))
		self.output_time = floor(read_time_milliseconds(output_data))
		self.times = {INPUT_STAGE: self.input_time, CPU_STAGE: self.cpu_time, OUTPUT_STAGE: self.output_time}
		self.curr_stage = INPUT_STAGE

	def is_complete(self):
		return self.times[OUTPUT_STAGE] == 0

	def decrement_len(self):
		self.times[self.curr_stage] -= 1
		if self.times[self.curr_stage] == 0:
			self.curr_stage += 1


def read_time_milliseconds(size, rate=10.0):
	''' 
	Given a size in bytes, converts it to the number of milliseconds it'd take 
	to process that size of data. Default rate is 10 MB/ sec.
	'''
	result = size / (10.0**6)
	result = (result / rate) * 1000
	return result


class Machine:
	def __init__(self, num_slots, tasks):
		self.num_slots = num_slots
		self.current_tasks = Queue()
		self.tasks = tasks
		while self.current_tasks.qsize() < self.num_slots and tasks.qsize() > 0:
			self.current_tasks.put(tasks.get())

	def run(self):
		stage_counts = {INPUT_STAGE: 0, CPU_STAGE: 0, OUTPUT_STAGE: 0}
		new_tasks = Queue()
		while self.current_tasks.qsize() > 0:
			task = self.current_tasks.get()
			stage_counts[task.curr_stage] += 1
			task.decrement_len()
			if task.is_complete():
				if self.tasks.qsize() > 0:
					new_tasks.put(self.tasks.get())
			else:
				new_tasks.put(task)
		self.current_tasks = new_tasks
		return stage_counts

	def is_complete(self):
		return self.tasks.qsize() == 0 and self.current_tasks.qsize() == 0

def plot_results(result):
	input_sum = sum(result[INPUT_STAGE].values()) + 0.0
	cpu_sum = sum(result[CPU_STAGE].values()) + 0.0
	output_sum = sum(result[OUTPUT_STAGE].values()) + 0.0
	plt.plot(result[INPUT_STAGE].keys(), reduce_sums(map(lambda x: x/input_sum, result[INPUT_STAGE].values())), 
		result[CPU_STAGE].keys(), reduce_sums(map(lambda x: x/cpu_sum, result[CPU_STAGE].values())),
		result[OUTPUT_STAGE].keys(), reduce_sums(map(lambda x: x/output_sum, result[OUTPUT_STAGE].values())))
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

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("--n", default=1, type=int, help="number of slots per machine")
	parser.add_argument("file", type=str, help="data file path")
	args = parser.parse_args()
	data_file = open(args.file)
	tasks = Queue()

	for line in data_file:
		split_line = line.split()
		new_task = Task(split_line[0], int(split_line[1]), int(split_line[2]), int(split_line[3])) #TODO: Change this
		tasks.put(new_task)
	machine = Machine(args.n, tasks)

	result = {INPUT_STAGE:{}, CPU_STAGE:{}, OUTPUT_STAGE:{}}
	for stage in result:
		new_dict = {}
		count = 0
		while count <= args.n:
			new_dict[count] = 0
			count += 1
		result[stage] = new_dict
	while not machine.is_complete():
		stage_counts = machine.run()
		for stage in stage_counts.keys():
			count = stage_counts[stage]
			result[stage][count] += 1
	plot_results(result)
