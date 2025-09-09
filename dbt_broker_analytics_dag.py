from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "retries": 0,
}

with DAG(
    dag_id="dbt_broker_analytics",
    default_args=default_args,
    start_date=datetime(2025, 9, 8),
    schedule_interval=None,  # Запуск вручную
    catchup=False,
) as dag:

       dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command="cd /opt/dbt/broker_analytics && dbt run --profiles-dir /root/.dbt",
       )

       dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command="cd /opt/dbt/broker_analytics && dbt test --profiles-dir /root/.dbt",
      ) 


       dbt_run >> dbt_test
