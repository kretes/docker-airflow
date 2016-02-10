from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators import BashOperator, DummyOperator

dag = DAG("NDCG_HOUR1",
          default_args={"owner" : "me",
                        "start_date":datetime.now() - timedelta(minutes=10)},
          schedule_interval='* * * * *',
          concurrency=6,
          dagrun_timeout=timedelta(minutes=4)
          )

airflow_logs = "/tmp/scheduler-modelling.log"
default_params = {
    'airflow_logs' : airflow_logs
}

def ndcg_daily_command():
    return ("/usr/local/airflow/dags/echo.sh spark_ndcg " +
            "{{ (execution_date - macros.timedelta(hours = 1)).strftime('%Y/%m/%d %H') }} " +
            " 2>&1 >> %s" % airflow_logs)


def ndcg_add_hive_partition_command():
    return ("/usr/local/airflow/dags/echo.sh hive_ndcg " +
            "{{ (execution_date - macros.timedelta(hours = 1)).strftime('%Y %m %d %H') }} " +
            " 2>&1 >> %s" % airflow_logs)

run_id = datetime.now().strftime("%Y_%m_%d_%H")

run_this_last = DummyOperator(task_id='run_this_last', dag=dag)

ndcgHourly = BashOperator(
        task_id='spark_submit_ndcg_' + str(run_id),
        bash_command=ndcg_daily_command(),
        dag=dag)
ndcgAddHive = BashOperator(
        task_id='hive_partition_ndcg_' + str(run_id),
        bash_command=ndcg_add_hive_partition_command(),
        dag=dag)

ndcgHourly.set_downstream(ndcgAddHive)
ndcgAddHive.set_downstream(run_this_last)
