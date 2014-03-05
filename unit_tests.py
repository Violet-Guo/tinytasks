import unittest
from task import *
from machine import *

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
        one_run = {DISK_STAGE: 1, CPU_STAGE: 0, NETWORK_STAGE: 1} 
        self.assertEqual(machine.run(), one_run)
        self.assertTrue(machine.is_full())
        self.assertFalse(machine.is_empty())
        two_run = {DISK_STAGE: 0, CPU_STAGE: 2, NETWORK_STAGE: 0} 
        self.assertEqual(machine.run(), two_run)
        self.assertTrue(machine.is_full())
        self.assertFalse(machine.is_empty())
        three_run = {DISK_STAGE: 2, CPU_STAGE: 0, NETWORK_STAGE: 0} 
        self.assertEqual(machine.run(), three_run)
        self.assertFalse(machine.is_full())
        self.assertTrue(machine.is_empty())
'''
    def test_multiple_tasks(self):

    def test_two_machines(self):
'''
if __name__ == '__main__':
    unittest.main()
