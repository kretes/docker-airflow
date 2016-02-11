from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators import BashOperator, DummyOperator

dag = DAG("MYDAG",
          default_args={"owner" : "me",
                        "start_date":datetime.now() - timedelta(minutes=100)},
          schedule_interval='*/3 * * * *',
          concurrency=6,
          dagrun_timeout=timedelta(minutes=4)
          )

def command():
    return "echo {{ params.myparam }} {{ (execution_date - macros.timedelta(hours = 1)).strftime('%Y/%m/%d %H %M') }} 2>&1 >> /tmp/logs"

run_this_last = DummyOperator(task_id='run_this_last', dag=dag)

firstTask = BashOperator(
        task_id='first',
        bash_command=command(),
        params= {"myparam":"1"},
        dag=dag)
secondTask = BashOperator(
        task_id='second',
        bash_command=command(),
        params= {"myparam":"2"},
        dag=dag)

firstTask.set_downstream(secondTask)
secondTask.set_downstream(run_this_last)
