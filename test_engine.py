import unittest
from engine import Runner, Task, order

class PipelineTests(unittest.TestCase):
    def setUp(self): self.r=Runner(':memory:')
    def tearDown(self): self.r.close()
    def test_cycle_and_missing_dependency(self):
        with self.assertRaises(ValueError): order([Task('a',('a',),lambda _:1)])
        with self.assertRaises(ValueError): order([Task('a',('b',),lambda _:1)])
    def test_resume_does_not_reexecute_success(self):
        calls=[]
        tasks=[Task('a',(),lambda _:calls.append(1) or 5),Task('b',('a',),lambda x:x['a']*2)]
        self.assertEqual(self.r.run('r',tasks)['b'],10)
        self.r.run('r',tasks)
        self.assertEqual(len(calls),1)
    def test_retry(self):
        calls=[]
        def task(_):
            calls.append(1)
            if len(calls)==1: raise OSError('retry')
            return 4
        self.assertEqual(self.r.run('r',[Task('a',(),task,retries=1)])['a'],4)
        self.assertEqual(self.r.audit('r')[0]['attempts'],2)
    def test_downstream_not_run_on_failure(self):
        calls=[]
        def fail(_): raise ValueError('bad data')
        with self.assertRaises(ValueError): self.r.run('r',[Task('a',(),fail),Task('b',('a',),lambda _:calls.append(1))])
        self.assertEqual(calls,[])
    def test_changed_graph_rejected(self):
        self.r.run('r',[Task('a',(),lambda _:1)])
        with self.assertRaises(ValueError): self.r.run('r',[Task('a',(),lambda _:2,version='2')])
    def test_invalid_json_output_is_failure(self):
        with self.assertRaises(ValueError): self.r.run('r',[Task('a',(),lambda _:float('nan'))])
        self.assertEqual(self.r.audit('r')[0]['state'],'failed')
