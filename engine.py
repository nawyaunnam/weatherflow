"""Small sequential DAG runner; persistent outputs provide run-level resume."""
import hashlib
import json
import sqlite3
from dataclasses import dataclass
from typing import Callable

@dataclass(frozen=True)
class Task:
    name: str
    dependencies: tuple
    function: Callable
    retries: int=0
    version: str='1'

def order(tasks):
    tasks=list(tasks)
    by_name={task.name:task for task in tasks}
    if len(by_name)!=len(tasks): raise ValueError('duplicate task names')
    for task in tasks:
        if not task.name or type(task.retries) is not int or task.retries<0: raise ValueError('invalid task')
        if any(dep not in by_name for dep in task.dependencies): raise ValueError('unknown dependency')
    result,done=[],set()
    while len(done)<len(tasks):
        ready=sorted((t for t in tasks if t.name not in done and set(t.dependencies)<=done),key=lambda t:t.name)
        if not ready: raise ValueError('cycle detected')
        for task in ready: result.append(task); done.add(task.name)
    return result

class Runner:
    def __init__(self,path):
        self.db=sqlite3.connect(path)
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS runs(id TEXT PRIMARY KEY,fingerprint TEXT);
            CREATE TABLE IF NOT EXISTS tasks(run_id TEXT,name TEXT,state TEXT,attempts INTEGER,
                output TEXT,error TEXT,PRIMARY KEY(run_id,name));
        """)
    def close(self): self.db.close()
    def run(self,run_id,tasks):
        if not isinstance(run_id,str) or not run_id: raise ValueError('nonempty run id required')
        ordered=order(tasks)
        spec=[(t.name,sorted(t.dependencies),t.version) for t in ordered]
        digest=hashlib.sha256(json.dumps(spec).encode()).hexdigest()
        prior=self.db.execute('SELECT fingerprint FROM runs WHERE id=?',(run_id,)).fetchone()
        if prior and prior[0]!=digest: raise ValueError('run id already binds to another graph/version')
        with self.db: self.db.execute('INSERT OR IGNORE INTO runs VALUES(?,?)',(run_id,digest))
        outputs={}
        for task in ordered:
            prior=self.db.execute('SELECT state,output FROM tasks WHERE run_id=? AND name=?',(run_id,task.name)).fetchone()
            if prior and prior[0]=='success':
                outputs[task.name]=json.loads(prior[1]); continue
            for attempt in range(task.retries+1):
                with self.db:
                    self.db.execute("""INSERT INTO tasks VALUES(?,?,'running',1,NULL,NULL)
                        ON CONFLICT(run_id,name) DO UPDATE SET state='running',attempts=attempts+1,error=NULL""",(run_id,task.name))
                try:
                    value=task.function({dep:outputs[dep] for dep in task.dependencies})
                    serialized=json.dumps(value,allow_nan=False)
                except Exception as error:
                    with self.db:
                        self.db.execute("UPDATE tasks SET state='failed',error=? WHERE run_id=? AND name=?",(f'{type(error).__name__}: {error}',run_id,task.name))
                    if attempt==task.retries: raise
                else:
                    with self.db:
                        self.db.execute("UPDATE tasks SET state='success',output=?,error=NULL WHERE run_id=? AND name=?",(serialized,run_id,task.name))
                    outputs[task.name]=json.loads(serialized)
                    break
        return outputs
    def audit(self,run_id):
        return [{'task':n,'state':s,'attempts':a,'error':e} for n,s,a,e in self.db.execute('SELECT name,state,attempts,error FROM tasks WHERE run_id=? ORDER BY name',(run_id,))]
