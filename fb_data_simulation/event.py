from machine import *
from task import *
from data_simulation import *

class Event:
    def __init__(self, machine, time):
        self.machine = machine
        self.time = time

    def run(self, current_time):
        raise NotImplementedError

class StartEvent(Event):
    def run(self):
        new_task = self.machine.add_task(self.time)
        new_events = []
        self.task = new_task
        if new_task != None:
            new_time = self.time + new_task.get_curr_stage_time()
            new_event = TransitionEvent(self.machine, new_time, new_task)
            new_events.append((new_time, new_event))
        return new_events

    def __str__(self):
        result = "StartEvent with a time of: " + str(self.time) \
        + " for machine " + str(self.machine.machine_num)
        return result

class TransitionEvent(Event):
    def __init__(self, machine, time, task):
        Event.__init__(self, machine, time)
        self.task = task

    def run(self):
        new_tasks = self.machine.task_transition(self.time, self.task)
        new_events = []
        if len(new_tasks) == 0:
            return
        for task in new_tasks:
            new_time = self.time + task.get_curr_stage_time()
            if task.curr_stage == OUTPUT_STAGE:
                new_event = EndEvent(self.machine, new_time, task)
            else:
                new_event = TransitionEvent(self.machine, new_time, self.task)
            new_events.append((new_time, new_event))
            logging.debug("ADDING TASK, at time " + str(new_time) + " " + "with task " + str(new_event))
        return new_events

    def __str__(self):
        result = "TransitionEvent with a time of: " + str(self.time) \
        + " for machine " + str(self.machine.machine_num)
        return result

class EndEvent(Event):
    def __init__(self, machine, time, task):
        Event.__init__(self, machine, time)
        self.task = task

    def run(self):
        new_task = self.machine.remove_task(self.task, self.time)
        new_events = []
        if new_task != None:
            new_time = self.time + new_task.get_curr_stage_time()
            if task.curr_stage == OUTPUT_STAGE:
                new_event = EndEvent(self.machine, new_time, task)
            else:
                new_event = TransitionEvent(self.machine, new_time, self.task)
            new_events.append((new_time, new_event))
        new_events.append((self.time, StartEvent(self.machine, self.time)))
        return new_events

    def __str__(self):
        result = "EndEvent with a time of: " + str(self.time) \
        + " for machine " + str(self.machine.machine_num)
        return result