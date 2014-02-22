import argparse
import numpy
import matplotlib.pyplot as plt
import math
from Queue import Queue
from task import *
from machine import *

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
	parser.add_argument("--s", default=1, type=int, help="number of slots per machine")
	parser.add_argument("--m", default=1, type=int, help="number of machines")
	parser.add_argument("--d", default=10, type=int, help="disk throughput in MB/s")
	parser.add_argument("--n", default=10, type=int, help="network bandwidth in 10")
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
