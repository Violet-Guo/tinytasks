from task import *
from Queue import Queue
from data_simulation import *
from task_handler import *
from event_handler import *
from event import *

class Machine:
	'''
	Represents a machine object, which contains a queue of tasks assigned to it.
	Upon each run, the machine will check if any tasks are complete and update
	accordingly.
	'''
	def __init__(self, machine_num, num_slots, event_handler, all_tasks):
		self.machine_num = machine_num
		self.num_slots = num_slots
		self.counts = self.instantiate_counts()
		self.event_handler = event_handler
		self.all_tasks = all_tasks
		self.curr_counts = {NETWORK_STAGE:0, CPU_STAGE:0, DISK_STAGE:0}
		self.time = 0

	def instantiate_counts(self):
		new_counts = {NETWORK_STAGE:{}, CPU_STAGE:{}, DISK_STAGE:{}}
		for stage in new_counts:
			new_dict = {}
			count = 0
			while count <= self.num_slots:
				new_dict[count] = 0
				count += 1
			new_counts[stage] = new_dict
		return new_counts

	def start(self):
		count = 0
		while count < self.num_slots:
			new_event = StartEvent(self, 0)
			self.event_handler.add_event(new_event, 0)
			count += 1

	def update_counts(self, stage_counts, num_seconds):
		logging.debug("BEFORE stage_counts: " + str(stage_counts))
		for stage in stage_counts.keys():
			count = stage_counts[stage]
			self.counts[stage][count] += num_seconds	
		logging.debug("AFTER stage_counts: " + str(stage)+ ", counts: " + str(self.counts))

	def task_transition(self, new_time, task):
		time_change = new_time - self.time
		old_stage = task.get_curr_stage()
		self.update_counts(self.curr_counts, time_change)
		task.decrement_len(time_change)
		new_stage = task.get_curr_stage()
		if new_stage == old_stage:
			raise Exception("Machine.py: task_transition should lead to a new stage")
		self.curr_counts[old_stage] -= 1
		self.curr_counts[new_stage] += 1
		self.time = new_time

	def add_task(self, new_time):
		if self.all_tasks.empty():
			return
		new_task = self.all_tasks.get()
		current_stage = new_task.get_curr_stage()
		time_change = new_time - self.time
		self.update_counts(self.curr_counts, time_change)
		self.time = new_time
		self.curr_counts[current_stage] += 1
		return new_task

	def remove_task(self, task, new_time):
		current_stage = task.get_curr_stage()
		time_change = new_time - self.time
		self.update_counts(self.curr_counts, time_change)
		self.time = new_time
		self.curr_counts[current_stage] -= 1

	def is_empty(self):
		num_jobs = sum(d.itervalues())
		return num_jobs == 0

	def is_full(self):
		num_jobs = sum(d.itervalues())
		return num_jobs == self.num_slots



