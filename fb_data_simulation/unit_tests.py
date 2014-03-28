import unittest
from task import *
from data_simulation import *
from task_handler import *

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
        machine_queue = Queue()
        machine_queue.put(MapTask("test map task", 1, 1, 1))
        machine_queue.put(ReduceTask("test reduce task", 1, 1, 1))
        machine = Machine(0, 2, machine_queue)
        machine.run()
        machine.run()
        one_run = {DISK_STAGE: {0: 1, 1: 1, 2:0}, CPU_STAGE: {0: 2, 1: 0, 2:0}, NETWORK_STAGE: {0: 1, 1: 1, 2:0}} 
        self.assertEqual(machine.counts, one_run)
        self.assertTrue(machine.is_full())
        self.assertFalse(machine.is_empty())
        two_run = {DISK_STAGE: {0: 2, 1: 1, 2:0}, CPU_STAGE: {0: 2, 1: 0, 2:1}, NETWORK_STAGE: {0: 2, 1: 1, 2:0}} 
        machine.run()
        self.assertEqual(machine.counts, two_run)
        self.assertTrue(machine.is_full())
        self.assertFalse(machine.is_empty())
        three_run = {DISK_STAGE: {0: 2, 1: 1, 2:1}, CPU_STAGE: {0: 3, 1: 0, 2:1}, NETWORK_STAGE: {0: 3, 1: 1, 2:0}} 
        machine.run()
        self.assertEqual(machine.counts, three_run)
        self.assertFalse(machine.is_full())
        self.assertTrue(machine.is_empty())

    def test_map_tasks(self):
        data_file = open("data/test_map.data")
        tasks = parse_tasks(data_file, 10, 10)
        simulator = Simulator(1, 1, 10, 10, tasks)
        result = simulator.test_run()
        expected_result = [{'disk': {0:8, 1:16}, 'cpu':{0:16, 1:8}, 'network':{0:24, 1:0}}]
        self.assertEqual(expected_result, result)

    def test_map_multiple_slots(self):
        data_file = open("data/test_map.data")
        tasks = parse_tasks(data_file, 10, 10)
        simulator = Simulator(1, 2, 10, 10, tasks)
        result = simulator.test_run()
        expected_result = [{'disk': {0:4, 1:0, 2:8}, 'cpu':{0:8, 1:0, 2:4}, 'network':{0:12, 1:0, 2:0}}]
        self.assertEqual(expected_result, result)

    def test_map_multiple_machines(self):
        data_file = open("data/test_map.data")
        tasks = parse_tasks(data_file, 10, 10)
        simulator = Simulator(2, 1, 10, 10, tasks)
        result = simulator.test_run()
        expected_result = [{'disk': {0:4, 1:8}, 'cpu':{0:8, 1:4}, 'network':{0:12, 1:0}}]
        expected_result.append({'disk': {0:4, 1:8}, 'cpu':{0:8, 1:4}, 'network':{0:12, 1:0}})
        self.assertEqual(expected_result, result)

    def test_decrement_one(self):
        task = Task("test_task", 1, 1, 1, 'disk', 'network')
        task.decrement_length(3)
        self.assertTrue(task.is_complete())
        self.assertEqual(task.times, [0, 0, 0])
        task = Task("test_task", 2, 2, 2, 'disk', 'network')
        task.decrement_length(3)
        self.assertEqual(task.get_curr_stage(), 'cpu')
        self.assertEqual(task.times, [0, 1, 2])
        task = Task("test_task", 3, 2, 1, 'disk', 'network')
        task.decrement_length(3)
        self.assertEqual(task.get_curr_stage(), 'cpu')
        self.assertEqual(task.times, [0, 2, 1])

    def test_taskhandler(self):
        data_file = open("data/test_map.data")
        tasks = parse_tasks(data_file, 10, 10)
        task_handler = TaskHandler(tasks)
        comp_set = set()
        comp_set.add(3)
        comp_list = [3,]
        #putting in a new task
        new_task = task_handler.get_new_task()
        self.assertEqual(task_handler.task_heap, comp_list)
        self.assertEqual(task_handler.times_set, comp_set)
        new_task = task_handler.get_new_task()
        self.assertEqual(task_handler.task_heap, comp_list)
        self.assertEqual(task_handler.times_set, comp_set)
        shortest_task_time = task_handler.get_shortest_task_time()
        #now the times should be 6
        comp_set = set()
        comp_set.add(6)
        comp_list = [6,]
        new_task = task_handler.get_new_task()
        self.assertEqual(task_handler.task_heap, comp_list)
        self.assertEqual(task_handler.times_set, comp_set)

    def test_taskhandler_two(self):
        data_file = open("data/variable_test_map.data")
        tasks = parse_tasks(data_file, 10, 10)
        task_handler = TaskHandler(tasks)
        comp_set = set()
        comp_set.add(3)
        comp_list = [3,]
        #putting in a new task
        new_task = task_handler.get_new_task()
        self.assertEqual(task_handler.task_heap, comp_list)
        self.assertEqual(task_handler.times_set, comp_set)
        comp_set.add(14)
        comp_list = [3, 14]
        new_task = task_handler.get_new_task()
        self.assertEqual(task_handler.task_heap, comp_list)
        self.assertEqual(task_handler.times_set, comp_set)
        shortest_task_time = task_handler.get_shortest_task_time()
        self.assertEqual(shortest_task_time, 3)
        #now the times should be 6
        comp_set = set()
        comp_set.add(14)
        comp_list = [14,]
        self.assertEqual(task_handler.task_heap, comp_list)
        self.assertEqual(task_handler.times_set, comp_set)
        new_task = task_handler.get_new_task()
        comp_set.add(15)
        self.assertEqual(task_handler.times_set, comp_set)
        shortest_task_time = task_handler.get_shortest_task_time()
        self.assertEqual(shortest_task_time, 14)


if __name__ == '__main__':
    unittest.main()
