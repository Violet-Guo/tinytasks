import unittest
from task import *
from data_simulation import *
from task_handler import *
from parser import *

class TestingSimulation(unittest.TestCase):
    
    def test_task(self):
        map_task = MapTask("test map task", 1, 2, 3)
        self.assertFalse(map_task.is_complete())
        self.assertEqual(map_task.get_curr_stage(), DISK_STAGE)
        map_task.decrement_one()
        self.assertFalse(map_task.is_complete())
        self.assertEqual(map_task.get_curr_stage(), CPU_STAGE)
        map_task.decrement_one()
        self.assertFalse(map_task.is_complete())
        self.assertEqual(map_task.get_curr_stage(), CPU_STAGE)
        map_task.decrement_one()
        self.assertFalse(map_task.is_complete())
        self.assertEqual(map_task.get_curr_stage(), DISK_STAGE)
        map_task.decrement_one()
        map_task.decrement_one()
        map_task.decrement_one()
        self.assertTrue(map_task.is_complete())
    
    def test_reduce_task(self):
        reduce_task = ReduceTask("test reduce task", 1, 2, 3)
        self.assertFalse(reduce_task.is_complete())
        self.assertEqual(reduce_task.get_curr_stage(), NETWORK_STAGE)
        reduce_task.decrement_one()
        self.assertFalse(reduce_task.is_complete())
        self.assertEqual(reduce_task.get_curr_stage(), CPU_STAGE)
        reduce_task.decrement_one()
        self.assertFalse(reduce_task.is_complete())
        self.assertEqual(reduce_task.get_curr_stage(), CPU_STAGE)
        reduce_task.decrement_one()
        self.assertFalse(reduce_task.is_complete())
        self.assertEqual(reduce_task.get_curr_stage(), DISK_STAGE)
        reduce_task.decrement_one()
        reduce_task.decrement_one()
        reduce_task.decrement_one()
        self.assertTrue(reduce_task.is_complete())

    def test_one_machine(self):
        event_handler = EventHandler()
        task_queue = Queue()
        task_queue.put(MapTask("test map task", 1, 1, 1))
        task_queue.put(ReduceTask("test reduce task", 1, 1, 1))
        machine = Machine(0, 2, event_handler, task_queue)
        machine.start()
        event_handler.run()
        three_run = {DISK_STAGE: {0: 2, 1: 1, 2:1}, CPU_STAGE: {0: 3, 1: 0, 2:1}, NETWORK_STAGE: {0: 3, 1: 1, 2:0}} 
        machine.run(1)
        self.assertEqual(machine.counts, three_run)
        self.assertFalse(machine.is_full())
        self.assertTrue(machine.is_empty())
'''
    def test_map_tasks(self):
        data_file = open("data/test_map.data")
        parser = Parser(10, 10)
        tasks = parser.parse_tasks(data_file)
        simulator = Simulator(1, 1, 10, 10, tasks)
        result = simulator.test_run()
        expected_result = [{'disk': {0:8, 1:16}, 'cpu':{0:16, 1:8}, 'network':{0:24, 1:0}}]
        self.assertEqual(expected_result, result)

    def test_map_multiple_slots(self):
        data_file = open("data/test_map.data")
        parser = Parser(10, 10)
        tasks = parser.parse_tasks(data_file) 
        simulator = Simulator(1, 2, 10, 10, tasks)
        result = simulator.test_run()
        expected_result = [{'disk': {0:4, 1:0, 2:8}, 'cpu':{0:8, 1:0, 2:4}, 'network':{0:12, 1:0, 2:0}}]
        self.assertEqual(expected_result, result)

    def test_map_multiple_machines(self):
        data_file = open("data/test_map.data")
        parser = Parser(10, 10)
        tasks = parser.parse_tasks(data_file)
        simulator = Simulator(2, 1, 10, 10, tasks)
        result = simulator.test_run()
        expected_result = [{'disk': {0:4, 1:8}, 'cpu':{0:8, 1:4}, 'network':{0:12, 1:0}}]
        expected_result.append({'disk': {0:4, 1:8}, 'cpu':{0:8, 1:4}, 'network':{0:12, 1:0}})
        self.assertEqual(expected_result, result)

    def test_taskhandler(self):
        data_file = open("data/test_map.data")
        parser = Parser(10, 10)
        tasks = parser.parse_tasks(data_file)
        task_handler = TaskHandler(tasks)
        comp_set = set()
        comp_set.add(1)
        comp_set.add(2)
        comp_set.add(3)
        comp_list = [1, 2, 3]
        #putting in a new task
        new_task = task_handler.get_new_task()
        self.assertEqual(task_handler.task_heap, comp_list)
        self.assertEqual(task_handler.times_set, comp_set)
        new_task = task_handler.get_new_task()
        self.assertEqual(task_handler.task_heap, comp_list)
        self.assertEqual(task_handler.times_set, comp_set)
        shortest_task_time = task_handler.get_shortest_task_time()
        #now the times should be 6
        comp_set.remove(1)
        comp_set.add(4)
        comp_list = [2, 3, 4]
        new_task = task_handler.get_new_task()
        self.assertEqual(task_handler.task_heap, comp_list)
        self.assertEqual(task_handler.times_set, comp_set)
    
    def test_taskhandler_two(self):
        data_file = open("data/variable_test_map.data")
        parser = Parser(10, 10)
        tasks = parser.parse_tasks(data_file)
        task_handler = TaskHandler(tasks)
        comp_set = set()
        comp_set.add(1)
        comp_set.add(2)
        comp_set.add(3)
        #putting in a new task
        new_task = task_handler.get_new_task()
        self.assertEqual(task_handler.times_set, comp_set)
        comp_set.add(14)
        comp_list = [1, 2, 3, 14]
        new_task = task_handler.get_new_task()
        self.assertEqual(task_handler.task_heap, comp_list)
        self.assertEqual(task_handler.times_set, comp_set)
        shortest_task_time = task_handler.get_shortest_task_time()
        self.assertEqual(shortest_task_time, 1)
        #now the times should be 6
        comp_set.remove(1)
        self.assertEqual(task_handler.times_set, comp_set)
        new_task = task_handler.get_new_task()
        comp_set.add(12)
        comp_set.add(11)
        comp_set.add(13)
        self.assertEqual(task_handler.times_set, comp_set)
        shortest_task_time = task_handler.get_shortest_task_time()
        self.assertEqual(shortest_task_time, 1)
        shortest_task_time = task_handler.get_shortest_task_time()
        self.assertEqual(shortest_task_time, 1)

    def test_empty_task_handler(self):
        task_handler = TaskHandler(Queue())
        new_task = task_handler.get_new_task()
        self.assertEqual(new_task, None)
'''

if __name__ == '__main__':
    #logging.basicConfig(format='%(levelname)s-%(message)s', level=logging.DEBUG)
    unittest.main()

