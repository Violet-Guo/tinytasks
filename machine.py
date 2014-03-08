from task import *
from Queue import Queue

class Machine:
	'''
	Represents a machine object, which contains a queue of tasks assigned to it.
	Upon each run, the machine will check if any tasks are complete and update
	accordingly.
	'''
	def __init__(self, machine_num, num_slots, debug):
		self.debug_flag = debug
		self.machine_num = machine_num
		self.num_slots = num_slots
		self.current_tasks = Queue()
		self.counts = {NETWORK_STAGE:{}, CPU_STAGE:{}, DISK_STAGE:{}}
		for stage in self.counts:
			new_dict = {}
			count = 0
			while count <= num_slots:
				new_dict[count] = 0
				count += 1
			self.counts[stage] = new_dict

	def run(self):
		stage_counts = {DISK_STAGE: 0, CPU_STAGE: 0, NETWORK_STAGE: 0}
		new_tasks = Queue()
		task_count = self.current_tasks.qsize()
		while task_count > 0:
			task = self.current_tasks.get()
			stage_counts[task.get_curr_stage()] += 1
			task.decrement_len()
			if not task.is_complete():
				new_tasks.put(task)
			task_count -= 1
		self.current_tasks = new_tasks
		self.update_counts(stage_counts)

	def update_counts(self, stage_counts):
		self.debug("stage_counts: " + str(stage_counts))
		for stage in stage_counts.keys():
			count = stage_counts[stage]
			self.debug("stage: " + str(stage)+ ", count: " + str(count))
			self.counts[stage][count] += 1	

	def add_task(self, task):
		self.current_tasks.put(task)

	def is_full(self):
		return self.current_tasks.qsize() == self.num_slots

	def is_empty(self):
		return self.current_tasks.qsize() == 0 

	def debug(self, debug_str):
		if self.debug_flag:
			print(debug_str)