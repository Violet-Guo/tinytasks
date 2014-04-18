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
        three_run = {DISK_STAGE: {0: 1, 1: 1, 2:1}, CPU_STAGE: {0: 2, 1: 0, 2:1}, NETWORK_STAGE: {0: 2, 1: 1, 2:0}} 
        self.assertEqual(machine.counts, three_run)
        self.assertFalse(machine.is_full())
        self.assertTrue(machine.is_empty())

    def test_map_tasks(self):
        data_file = open("data/test_map.data")
        parser = Parser(10, 10)
        tasks = parser.parse_tasks(data_file)
        simulator = Simulator(1, 1, 10, 10, tasks)
        result = simulator.test_run()
        expected_result = [{'disk': {0:8, 1:16000}, 'cpu':{0:16000, 1:8}, 'network':{0:16008, 1:0}}]
        self.assertEqual(expected_result, result)
        for machine in simulator.machines:
            self.assertTrue(machine.is_empty())

    def test_map_multiple_slots(self):
        data_file = open("data/test_map.data")
        parser = Parser(10, 10)
        tasks = parser.parse_tasks(data_file) 
        simulator = Simulator(1, 2, 10, 10, tasks)
        result = simulator.test_run()
        expected_result = [{'disk': {0:4, 1:0, 2:8000}, 'cpu':{0:8000, 1:0, 2:4}, 'network':{0:8004, 1:0, 2:0}}]
        self.assertEqual(expected_result, result)
        for machine in simulator.machines:
            self.assertTrue(machine.is_empty())


    def test_map_multiple_machines(self):
        data_file = open("data/test_map.data")
        parser = Parser(10, 10)
        tasks = parser.parse_tasks(data_file)
        simulator = Simulator(2, 1, 10, 10, tasks)
        result = simulator.test_run()
        expected_result = [{'disk': {0:4, 1:8000}, 'cpu':{0:8000, 1:4}, 'network':{0:8004, 1:0}}]
        expected_result.append({'disk': {0:4, 1:8000}, 'cpu':{0:8000, 1:4}, 'network':{0:8004, 1:0}})
        self.assertEqual(expected_result, result)
        for machine in simulator.machines:
            self.assertTrue(machine.is_empty())        

if __name__ == '__main__':
    #logging.basicConfig(format='%(levelname)s-%(message)s', level=logging.DEBUG)
    unittest.main()

