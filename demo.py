import json
import tempfile
from pathlib import Path
from engine import Runner, Task

attempts={'count':0}
def flaky(inputs):
    attempts['count']+=1
    if attempts['count']==1: raise OSError('transient source failure')
    return [18,20,23,19]
tasks=[Task('extract',(),flaky,retries=1),
       Task('validate',('extract',),lambda x:[v for v in x['extract'] if -90<=v<=60]),
       Task('aggregate',('validate',),lambda x:{'mean_temperature':sum(x['validate'])/len(x['validate'])})]
with tempfile.TemporaryDirectory() as d:
    runner=Runner(Path(d)/'runs.db')
    first=runner.run('demo',tasks)
    resumed=runner.run('demo',tasks)
    print(json.dumps({'result':first,'resumed_result':resumed,'source_attempts':attempts['count'],'audit':runner.audit('demo')},indent=2))
    runner.close()
