# workflow_dag.py

# import statements
import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# global variables

# module functions
with DAG(
  dag_id="elt_dag",
    start_date=datetime.datetime(2021, 1, 1),
    schedule="@daily"
):
    EmptyOperator(task_id="task", dag="elt_dag")