from event import *
from Queue import *
from task import *
from data_simulation import *
START = "start"
END = "end"

class EventHandler:
	def __init__(self):
		self.event_queue = PriorityQueue()
		self.curr_time = 0
		self.task_times = {}

	def run(self):
		while not self.event_queue.empty():
			event_info = self.event_queue.get()
			new_time = event_info[0]
			event = event_info[1]
			logging.debug("Event retrieved: " + str(event))
			if not isinstance(event, StartEvent):
				logging.debug("TASK: " + str(event.task))
			self.curr_time = new_time
			new_events = event.run()
			self.record_task_time(event, new_time)
			if new_events != None:
				for event in new_events: 
					self.event_queue.put(event)
			logging.debug("\n")

	def record_task_time(self, event, time):
		task = event.task
		if isinstance(event, StartEvent) and isinstance(task, MapTask):
			if task.job not in self.task_times:
				self.task_times[task.job] = {START: time}
		if isinstance(event, EndEvent) and isinstance(task, ReduceTask):
			if task.job in self.task_times:
				self.task_times[task.job][END] = time


	def add_event(self, event, time):
		new_event = (time, event)
		self.event_queue.put(new_event)