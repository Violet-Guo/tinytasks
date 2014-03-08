import unittest
from task import *
from machine import *
from data_simulation import *

class TestingSimulation(unittest.TestCase):

    def test_task(self):
        map_task = MapTask("test map task", 1, 2, 3)
        self.assertFalse(map_task.is_complete())
        self.assertEqual(map_task.get_curr_stage(), DISK_STAGE)
        map_task.decrement_len()
        self.assertFalse(map_task.is_complete())
        self.assertEqual(map_task.get_curr_stage(), CPU_STAGE)
        map_task.decrement_len()
        self.assertFalse(map_task.is_complete())
        self.assertEqual(map_task.get_curr_stage(), CPU_STAGE)
        map_task.decrement_len()
        self.assertFalse(map_task.is_complete())
        self.assertEqual(map_task.get_curr_stage(), DISK_STAGE)
        map_task.decrement_len()
        map_task.decrement_len()
        map_task.decrement_len()
        self.assertTrue(map_task.is_complete())
    
    def test_reduce_task(self):
        reduce_task = ReduceTask("test reduce task", 1, 2, 3)
        self.assertFalse(reduce_task.is_complete())
        self.assertEqual(reduce_task.get_curr_stage(), NETWORK_STAGE)
        reduce_task.decrement_len()
        self.assertFalse(reduce_task.is_complete())
        self.assertEqual(reduce_task.get_curr_stage(), CPU_STAGE)
        reduce_task.decrement_len()
        self.assertFalse(reduce_task.is_complete())
        self.assertEqual(reduce_task.get_curr_stage(), CPU_STAGE)
        reduce_task.decrement_len()
        self.assertFalse(reduce_task.is_complete())
        self.assertEqual(reduce_task.get_curr_stage(), DISK_STAGE)
        reduce_task.decrement_len()
        reduce_task.decrement_len()
        reduce_task.decrement_len()
        self.assertTrue(reduce_task.is_complete())

    def test_one_machine(self):
        machine = Machine(2)
        map_task = MapTask("test map task", 1, 1, 1)
        reduce_task = ReduceTask("test reduce task", 1, 1, 1)
        machine.add_task(map_task)
        machine.add_task(reduce_task)
        machine.run()
        one_run = {DISK_STAGE: {0: 0, 1: 1, 2:0}, CPU_STAGE: {0: 1, 1: 0, 2:0}, NETWORK_STAGE: {0: 0, 1: 1, 2:0}} 
        self.assertEqual(machine.counts, one_run)
        self.assertTrue(machine.is_full())
        self.assertFalse(machine.is_empty())
        two_run = {DISK_STAGE: {0: 1, 1: 1, 2:0}, CPU_STAGE: {0: 1, 1: 0, 2:1}, NETWORK_STAGE: {0: 1, 1: 1, 2:0}} 
        machine.run()
        self.assertEqual(machine.counts, two_run)
        self.assertTrue(machine.is_full())
        self.assertFalse(machine.is_empty())
        three_run = {DISK_STAGE: {0: 1, 1: 1, 2:1}, CPU_STAGE: {0: 2, 1: 0, 2:1}, NETWORK_STAGE: {0: 2, 1: 1, 2:0}} 
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

if __name__ == '__main__':
    unittest.main()
