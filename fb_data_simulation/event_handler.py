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
			logging.info("Event retrieved: " + str(event))
			self.curr_time = new_time
			new_event = event.run()
			if new_event != None:
				self.event_queue.put(new_event)

	def add_event(self, event, time):
		new_event = (time, event)
		self.event_queue.put(new_event)