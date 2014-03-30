from task import *
from Queue import Queue
from data_simulation import *
from task_handler import *

class Machine:
	'''
	Represents a machine object, which contains a queue of tasks assigned to it.
	Upon each run, the machine will check if any tasks are complete and update
	accordingly.
	'''
	def __init__(self, machine_num, num_slots, task_handler):
		self.machine_num = machine_num
		self.num_slots = num_slots
		self.current_tasks = Queue()
		self.task_handler = task_handler
		self.counts = {NETWORK_STAGE:{}, CPU_STAGE:{}, DISK_STAGE:{}}
		for stage in self.counts:
			new_dict = {}
			count = 0
			while count <= num_slots:
				new_dict[count] = 0
				count += 1
			self.counts[stage] = new_dict

	def run(self, run_time):
		logging.debug("Machine " + str(self.machine_num) + " running")
		stage_counts = {DISK_STAGE: 0, CPU_STAGE: 0, NETWORK_STAGE: 0}
		new_tasks = Queue()
		task_count = self.current_tasks.qsize()
		while task_count > 0:
			task = self.current_tasks.get()
			stage_counts[task.get_curr_stage()] += 1
			task.decrement_len(run_time)
			if not task.is_complete():
				new_tasks.put(task)
			task_count -= 1
		while new_tasks.qsize() < self.num_slots and not self.task_handler.empty_tasks():
			task = self.task_handler.get_new_task()
			new_tasks.put(task)
		self.current_tasks = new_tasks
		self.update_counts(stage_counts, run_time)

	def update_counts(self, stage_counts, num_seconds):
		logging.debug("BEFORE stage_counts: " + str(stage_counts))
		for stage in stage_counts.keys():
			count = stage_counts[stage]
			self.counts[stage][count] += num_seconds	
		logging.debug("AFTER stage_counts: " + str(stage)+ ", counts: " + str(self.counts))

	def add_task(self, task):
		self.current_tasks.put(task)

	def is_full(self):
		return self.current_tasks.qsize() == self.num_slots

	def is_empty(self):
		return self.current_tasks.qsize() == 0 
