from feeds import fetch,run
from engine import Runner,Task
import hashlib
import json
import math

def acquire():
    return {'sources':[fetch('https://api.open-meteo.com/v1/forecast?latitude=40.71&longitude=-74.01&hourly=temperature_2m,relative_humidity_2m&current=temperature_2m,wind_speed_10m&past_days=7&forecast_days=1&timezone=UTC')]}


def analyze(snapshot):
    data=snapshot['sources'][0]['payload']
    run_id=hashlib.sha256(json.dumps(data,sort_keys=True).encode()).hexdigest()
    def validate(inputs):
        hourly=inputs['extract']['hourly']
        times=hourly['time'];temps=hourly['temperature_2m']
        if len(times)!=len(temps):raise ValueError('hourly arrays have inconsistent lengths')
        rows=[{'time':t,'temperature_c':v} for t,v in zip(times,temps) if v is not None and math.isfinite(v) and -90<=v<=60]
        if not rows:raise ValueError('no valid weather observations')
        return rows
    def aggregate(inputs):
        rows=inputs['validate'];values=[r['temperature_c'] for r in rows]
        return {'hourly_records':len(rows),'mean_temperature_c':sum(values)/len(values),
                'minimum_temperature_c':min(values),'maximum_temperature_c':max(values),
                'first_hour':rows[0]['time'],'last_hour':rows[-1]['time']}
    tasks=[Task('extract',(),lambda _:data),Task('validate',('extract',),validate),Task('aggregate',('validate',),aggregate)]
    runner=Runner('live-pipeline.db')
    try:
        outputs=runner.run(run_id,tasks)
        return dict(outputs['aggregate'],project='WeatherFlow',current=data.get('current'),
                    audit=runner.audit(run_id),run_id=run_id,
                    note='Fixed New York coordinates, not the user location. Open-Meteo values include weather-model estimates and forecast hours; they are not all station measurements.')
    finally:runner.close()


if __name__=='__main__': run(acquire,analyze)
