from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
import sys

# Add the project root to the python path so we can import the pipeline script
# Assuming the DAG is in <project>/dags/ and the script is in <project>/
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

try:
    from dlt_pipeline import load_github_data
except ImportError:
    # Fallback if import fails (e.g. in some airflow environments)
    load_github_data = None

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'github_pipeline_dag',
    default_args=default_args,
    description='A DAG to run dlt github pipeline',
    schedule=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['dlt', 'github'],
)

# Option 1: PythonOperator (requires dlt_pipeline to be importable and dependencies installed in Airflow env)
if load_github_data:
    t1 = PythonOperator(
        task_id='run_dlt_pipeline_python',
        python_callable=load_github_data,
        dag=dag,
    )
else:
    # Option 2: BashOperator (safer if import fails, runs in a separate process)
    # Assumes 'dlt_pipeline.py' is in the project root
    t1 = BashOperator(
        task_id='run_dlt_pipeline_bash',
        bash_command=f'cd {PROJECT_ROOT} && python dlt_pipeline.py',
        dag=dag,
    )
