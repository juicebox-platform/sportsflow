import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
import sportsflow

with DAG(
    dag_id="hello_world",
    start_date=pendulum.datetime(2025, 8, 2, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["example"],
) as dag:
    hello_task = BashOperator(
        task_id="hello_task",
        bash_command='echo "Hello from Airflow!"',
    )
