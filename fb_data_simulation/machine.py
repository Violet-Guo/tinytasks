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
	def __init__(self, machine_num, num_slots, event_handler, all_tasks, \
		num_disk_slots, num_cpu_slots, num_network_slots):
		self.machine_num = machine_num
		self.num_slots = num_slots
		self.num_slots_dict = {NETWORK_STAGE: num_network_slots, CPU_STAGE: num_cpu_slots, DISK_STAGE: num_disk_slots}
		self.event_handler = event_handler
		self.all_tasks = all_tasks
		self.curr_counts = {NETWORK_STAGE:0, CPU_STAGE:0, DISK_STAGE:0}
		self.time = 0
		self.num_disk_slots = num_disk_slots
		self.num_cpu_slots = num_cpu_slots
		self.num_network_slots = num_network_slots
		self.total_counts = self.instantiate_counts()
		self.disk_queue = Queue()
		self.cpu_queue = Queue()
		self.network_queue = Queue()
	
	def instantiate_counts(self):
		new_counts = {NETWORK_STAGE:{}, CPU_STAGE:{}, DISK_STAGE:{}}
		for stage in new_counts:
			num_slots = self.num_slots_dict[stage]
			new_dict = {}
			count = 0
			while count <= num_slots:
				ratio = (count + 0.0) / num_slots
				new_dict[ratio] = 0
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
		logging.debug("COUNTS: " + str(stage_counts))
		for stage in stage_counts.keys():
			num_slots = self.num_slots_dict[stage]
			count = stage_counts[stage]
			ratio = (count + 0.0) / num_slots
			self.total_counts[stage][ratio] += num_seconds
		logging.debug("self.total_counts: " + str(self.total_counts))

	def task_transition(self, new_time, task):
		time_change = new_time - self.time
		old_stage = task.get_curr_stage()
		self.update_counts(self.curr_counts, time_change)
		task.transition_stage()
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
		num_jobs = sum(self.curr_counts.itervalues())
		return num_jobs == 0

	def is_full(self):
		num_jobs = sum(self.curr_counts.itervalues())
		return num_jobs == self.num_slots



