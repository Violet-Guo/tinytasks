from event import *
from Queue import Queue

class EventHandler:
	def __init__(self):
		self.event_queue = Queue.PriorityQueue()
		self.curr_time = 0

	def run(self):
		while not self.event_queue.empty():
			event_info = self.event_queue.get()
			new_time = event_info[0]
			event = event_info[1]
			self.curr_time = new_time
			new_event = event.run()
			if new_event != null:
				self.event_queue.put(new_event)

	def add_event(self, event):
		self.event_queue.put(new_event)