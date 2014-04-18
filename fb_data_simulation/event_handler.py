from event import *
from Queue import *
from data_simulation import *

class EventHandler:
	def __init__(self):
		self.event_queue = PriorityQueue()
		self.curr_time = 0

	def run(self):
		while not self.event_queue.empty():
			event_info = self.event_queue.get()
			new_time = event_info[0]
			event = event_info[1]
			logging.debug("Event retrieved: " + str(event))
			if not isinstance(event, StartEvent):
				logging.debug("TASK: " + str(event.task))
			self.curr_time = new_time
			new_event = event.run()
			if new_event != None:
				self.event_queue.put(new_event)
			logging.debug("\n")

	def add_event(self, event, time):
		new_event = (time, event)
		self.event_queue.put(new_event)