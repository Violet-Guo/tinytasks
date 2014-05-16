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
        machine = Machine(0, 2, event_handler, task_queue, 2, 2, 2)
        machine.start()
        event_handler.run()
        three_run = {DISK_STAGE: {0: 1, 0.5: 1, 1:1}, CPU_STAGE: {0: 2, 0.5: 0, 1:1}, NETWORK_STAGE: {0: 2, 0.5: 1, 1:0}} 
        self.assertEqual(machine.total_counts, three_run)
        self.assertFalse(machine.is_full())
        self.assertTrue(machine.is_empty())

    def test_map_tasks(self):
        data_file = "data/test_map.data"
        parser = Parser(10, 10)
        tasks = parser.parse_tasks(data_file)
        simulator = Simulator(1, 1, tasks, 1, 1, 1)
        result = simulator.test_run()
        expected_result = [{'disk': {0:8000, 1:16000}, 'cpu':{0:16000, 1:8000}, 'network':{0:24000, 1:0}}]
        self.assertEqual(expected_result, result)
        for machine in simulator.machines:
            self.assertTrue(machine.is_empty())

    def test_map_multiple_slots(self):
        data_file = "data/test_map.data"
        parser = Parser(10, 10)
        tasks = parser.parse_tasks(data_file) 
        simulator = Simulator(1, 2, tasks, 2, 2, 2)
        result = simulator.test_run()
        expected_result = [{'disk': {0:4000, 0.5:0, 1:8000}, 'cpu':{0:8000, 0.5:0, 1:4000}, 'network':{0:12000, 0.5:0, 1:0}}]
        self.assertEqual(expected_result, result)
        for machine in simulator.machines:
            self.assertTrue(machine.is_empty())

    def test_map_multiple_machines(self):
        data_file = "data/test_map.data"
        parser = Parser(10, 10)
        tasks = parser.parse_tasks(data_file)
        simulator = Simulator(2, 1, tasks, 1, 1, 1)
        result = simulator.test_run()
        expected_result = [{'disk': {0:4000, 1:8000}, 'cpu':{0:8000, 1:4000}, 'network':{0:12000, 1:0}}]
        expected_result.append({'disk': {0:4000, 1:8000}, 'cpu':{0:8000, 1:4000}, 'network':{0:12000, 1:0}})
        self.assertEqual(expected_result, result)
        for machine in simulator.machines:
            self.assertTrue(machine.is_empty())

    def test_first_10_tasks(self):
        data_file = "data/first_10_tasks.data"
        parser = Parser(10, 10)
        tasks = parser.parse_tasks(data_file)
        simulator = Simulator(1, 1, tasks, 1, 1, 1)
        result = simulator.test_run()
        expected_result = [{'disk': {0:368960000, 1:72453574}, 'cpu':{0:72453574, 1:368960000}, 'network':{0:441413574, 1:0}}]
        self.assertEqual(expected_result, result)

    def test_first_tasks_two_machines(self):
        data_file = "data/first_10_tasks.data"
        parser = Parser(10, 10)
        tasks = parser.parse_tasks(data_file)
        simulator = Simulator(2, 1, tasks, 1, 1, 1)
        result = simulator.test_run()
        expected_result = [{'disk': {0:28550000, 1:1016791}, 'cpu':{0:1016791, 1:28550000}, 'network':{0:29566791, 1:0}}]
        expected_result.append({'disk': {0:340410000, 1:71436783}, 'cpu':{0:71436783, 1:340410000}, 'network':{0:411846783, 1:0}})
        if expected_result[1] == result[1] and expected_result[0] == result[0]:
            return
        if expected_result[0] == result[1] and expected_result[1] == result[0]:
            return
        else:
            assertEqual(False, True)

    def test_last12_tasks(self):
        data_file = "data/last_12_tasks.data"
        parser = Parser(10, 10)
        tasks = parser.parse_tasks(data_file)
        simulator = Simulator(1, 1, tasks, 1, 1, 1)
        result = simulator.test_run()
        expected_result = [{'network': {0:398480134, 1: 75695}, 'cpu':{0:33835829, 1:364720000}, 'disk':{0:364795695, 1:33760134}}]
        self.assertEqual(expected_result, result)

    def test_last6_two_machines(self):
        data_file = "data/last_6_tasks.data"
        parser = Parser(10, 10)
        tasks = parser.parse_tasks(data_file)
        simulator = Simulator(2, 1, tasks, 1, 1, 1)
        result = simulator.test_run()
        expected_result = [{'network': {0:100549912, 1: 25072}, 'disk':{0:91775072, 1: 8799912}, 'cpu':{0:8824984, 1: 91750000}}]
        expected_result.append({'network': {0:78617406, 1: 25704}, 'cpu':{0:6263110, 1:72380000}, 'disk':{0:72405704, 1: 6237406}})
        if expected_result[1] == result[1] and expected_result[0] == result[0]:
            return
        if expected_result[0] == result[1] and expected_result[1] == result[0]:
            return
        else:
            assertEqual(False, True)

    def test_multiple_slots(self):
        event_handler = EventHandler()
        task_queue = Queue()
        task_queue.put(MapTask("map task 1", 17, 7, 19))
        task_queue.put(ReduceTask("reduce task 1", 3, 16, 7))
        task_queue.put(MapTask("map task 2", 11, 14, 10))
        machine = Machine(0, 2, event_handler, task_queue, 2, 2, 2)
        machine.start()
        event_handler.run()
        three_run = {'disk': {0: 10, 0.5: 38, 1: 13}, 'network': {0: 58, 0.5: 3, 1: 0}, 'cpu': {0: 26, 0.5: 33, 1: 2}}
        self.assertEqual(machine.total_counts, three_run)
        self.assertFalse(machine.is_full())
        self.assertTrue(machine.is_empty())

if __name__ == '__main__':
    #logging.basicConfig(format='%(levelname)s-%(message)s', level=logging.DEBUG)
    unittest.main()

