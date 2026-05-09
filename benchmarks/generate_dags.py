import json
import os

NUM_TASKS = 50

def generate_ironflow_dag():
    toml_str = f"""[dag]
id = "benchmark_dag_100"
description = "A {NUM_TASKS}-task sequential DAG for benchmarking overhead"

"""
    for i in range(NUM_TASKS):
        toml_str += f"""[[dag.tasks]]
id = "task_{i}"
operator = "bash"
"""
        if i > 0:
            toml_str += f'depends_on = ["task_{i-1}"]\n'
            
        toml_str += f"""[dag.tasks.config]
command = "echo 'Executing task {i}'"

"""
    
    with open("ironflow_dag.toml", "w") as f:
        f.write(toml_str)

def generate_prefect_dag():
    code = f"""from prefect import flow, task
import time

NUM_TASKS = {NUM_TASKS}

@task
def dummy_task(i, prev_result=None):
    return i

@flow(name="benchmark_dag_100")
def benchmark_flow():
    prev = None
    for i in range(NUM_TASKS):
        prev = dummy_task(i, prev_result=prev)
        
if __name__ == "__main__":
    benchmark_flow()
"""
    with open("competitor_dag.py", "w") as f:
        f.write(code)

def generate_airflow_dag():
    os.makedirs("airflow_home/dags", exist_ok=True)
    code = f"""from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

with DAG('benchmark_dag_100', start_date=datetime(2023, 1, 1), schedule=None, catchup=False) as dag:
    prev = None
    for i in range({NUM_TASKS}):
        task = BashOperator(
            task_id=f'task_{{i}}',
            bash_command=f"echo 'Executing task {{i}}'"
        )
        if prev:
            prev >> task
        prev = task
"""
    with open("airflow_home/dags/airflow_dag.py", "w") as f:
        f.write(code)

def generate_dagster_dag():
    code = f"""from dagster import job, op

@op
def dummy_op(prev=None):
    return

@job
def benchmark_dag_100():
    prev = None
    for i in range({NUM_TASKS}):
        if prev is not None:
            prev = dummy_op.alias(f"task_{{i}}")(prev)
        else:
            prev = dummy_op.alias(f"task_{{i}}")()

if __name__ == "__main__":
    benchmark_dag_100.execute_in_process()
"""
    with open("dagster_dag.py", "w") as f:
        f.write(code)

if __name__ == "__main__":
    generate_ironflow_dag()
    generate_prefect_dag()
    generate_airflow_dag()
    generate_dagster_dag()
    print("Generated all DAG files.")
