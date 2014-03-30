from math import floor

NETWORK_STAGE = "network"
CPU_STAGE = "cpu"
DISK_STAGE = "disk"

INPUT_STAGE = 0
COMPUTING_STAGE = 1
OUTPUT_STAGE = 2

class Task:
	'''
	Represents a task object, which contains information about the length of time
	needed to complete each stage of the task. 
	'''
	def __init__(self, job, input_time, cpu_time, output_time, input_stage, output_stage):
		self.job = job
		self.input_time = input_time
		self.cpu_time = cpu_time
		self.output_time = output_time
		self.stages = {INPUT_STAGE: input_stage, COMPUTING_STAGE: CPU_STAGE, OUTPUT_STAGE: output_stage}
		self.times = [input_time, cpu_time, output_time]
		self.curr_stage = INPUT_STAGE

	def get_curr_stage(self):
		return self.stages[self.curr_stage]

	def get_curr_stage_time(self):
		return self.times[self.curr_stage]

	def time_left(self):
		return self.times[INPUT_STAGE] + self.times[COMPUTING_STAGE] + self.times[OUTPUT_STAGE]

	def is_complete(self):
		return self.times[OUTPUT_STAGE] == 0

	def decrement_one(self):
		if self.time_left() == 0:
			raise Exception("Trying to decrement more time than what's left")
		self.times[self.curr_stage] -= 1
		if self.times[self.curr_stage] == 0:
			self.curr_stage += 1

	def decrement_len(self, length):
		if self.times[self.curr_stage] < length:
			raise Exception("Trying to decrement more time than what's left")
		self.times[self.curr_stage] -= length
		if self.times[self.curr_stage] == 0:
			self.curr_stage += 1




class MapTask(Task):
	'''
	Represents a reduce ask, where the input stage is disk and
	the output stage is also disk.
	'''
	def __init__(self, job, input_time, cpu_time, output_time):
		Task.__init__(self, job, input_time, cpu_time, output_time, DISK_STAGE, DISK_STAGE)

	def __str__(self):
		result = "MapTask with job name: " + self.job + ", input time of " + str(self.input_time) + \
		", cpu time of " + str(self.cpu_time) + ", and output time of " + str(self.output_time)
		return result



class ReduceTask(Task):
	'''
	Represents a reduce task, where the input stage is the network and
	the output stage is also disk.
	'''
	def __init__(self, job, input_time, cpu_time, output_time):
		Task.__init__(self, job, input_time, cpu_time, output_time, NETWORK_STAGE, DISK_STAGE)

	def __str__(self):
		result = "ReduceTask with job name: " + self.job + ", input time of " + str(self.input_time) + \
		", cpu time of " + str(self.cpu_time) + ", and output time of " + str(self.output_time)
		return result


