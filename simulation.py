import argparse
import numpy
import matplotlib.pyplot as plt
import math

DISK_STAGE = 0
CPU_STAGE = 1
NETWORK_STAGE = 2

class Task:
	def __init__(self, task_len):
		divided_len = task_len / 3
		self.times = {DISK_STAGE: divided_len, CPU_STAGE: divided_len, NETWORK_STAGE: divided_len}
		self.curr_stage = DISK_STAGE

	def is_complete(self):
		return self.times[NETWORK_STAGE] == 0

	def decrement_len(self):
		self.times[self.curr_stage] -= 1
		if self.times[self.curr_stage] == 0:
			self.curr_stage += 1


class Machine:
	def __init__(self, num_slots):
		self.num_slots = num_slots
		self.current_tasks = []
		while len(self.current_tasks) < self.num_slots:
			task_time = math.ceil(numpy.random.exponential(10)) * 3
			self.current_tasks.append(Task(task_time))

	def run(self):
		count = 0
		stage_counts = {DISK_STAGE: 0, CPU_STAGE: 0, NETWORK_STAGE: 0}
		while count < len(self.current_tasks):
			task = self.current_tasks[count]
			stage_counts[task.curr_stage] += 1
			task.decrement_len()
			if task.is_complete():
				task_time = math.ceil(numpy.random.exponential(10)) * 3
				self.current_tasks[count] = Task(task_time)
			count += 1
		return stage_counts

def plot_results(result):
	disk_sum = sum(result[DISK_STAGE].values()) + 0.0
	cpu_sum = sum(result[CPU_STAGE].values()) + 0.0
	network_sum = sum(result[NETWORK_STAGE].values()) + 0.0
	plt.plot(result[DISK_STAGE].keys(), reduce_sums(map(lambda x: x/disk_sum, result[DISK_STAGE].values())), 
		result[CPU_STAGE].keys(), reduce_sums(map(lambda x: x/cpu_sum, result[CPU_STAGE].values())),
		result[NETWORK_STAGE].keys(), reduce_sums(map(lambda x: x/network_sum, result[NETWORK_STAGE].values())))
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
	parser.add_argument("--t", default=100, type=int, help="length of simulation")
	args = parser.parse_args()
	machine = Machine(args.n)
	curr_time = 0
	result = {DISK_STAGE:{}, CPU_STAGE:{}, NETWORK_STAGE:{}}
	for stage in result:
		new_dict = {}
		count = 0
		while count <= args.n:
			new_dict[count] = 0
			count += 1
		result[stage] = new_dict
	while curr_time < args.t:
		stage_counts = machine.run()
		for stage in stage_counts.keys():
			count = stage_counts[stage]
			result[stage][count] += 1
		curr_time += 1
	plot_results(result)
