import Queue as queue
from heapq import *
from data_simulation import *

class TaskHandler:
    '''
    Represents a machine object, which contains a queue of tasks assigned to it.
    Upon each run, the machine will check if any tasks are complete and update
    accordingly.
    '''
    def __init__(self, tasks):
        self.unassigned_tasks = tasks
        self.current_time = 0
        self.task_heap = []
        self.times_set = set()

    def get_shortest_task_time(self):
        shortest_time = heappop(self.task_heap)
        self.current_time = shortest_time
        self.times_set.remove(shortest_time)
        logging.debug("Times set: " + str(self.times_set))
        logging.debug("Task heap: " + str(self.task_heap))
        return shortest_time

    def get_new_task(self):
        if self.unassigned_tasks.qsize() == 0:
            return 
        else:
            new_task = self.unassigned_tasks.get()
            new_time = self.current_time + new_task.time_left()
            if new_time not in self.times_set:
                self.times_set.add(new_time)
                heappush(self.task_heap, new_time)
            logging.debug("Times set: " + str(self.times_set))
            logging.debug("Task heap: " + str(self.task_heap))
            return new_task
            
