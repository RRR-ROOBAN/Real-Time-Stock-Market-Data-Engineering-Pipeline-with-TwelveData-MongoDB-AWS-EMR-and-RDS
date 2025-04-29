  GNU nano 6.2                                                                                    connection.py                                                                                             
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import datetime
import subprocess

# Define functions for Python scripts
def run_python_script_1():
    subprocess.run(["python3", "/home/vboxuser/pro_p.py"])

def run_python_script_2():
    subprocess.run(["python3", "/home/vboxuser/test_connection.py"])

# Initialize the DAG
with DAG(
    'project_data',
    default_args={
        "owner": "roobanraj",
        "start_date": datetime.datetime(2025, 1, 10),
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=3)
    },
    schedule_interval='*/5 * * * *',
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id="run_pro_p_script",
        python_callable=run_python_script_1
    )

    t2 = PythonOperator(
        task_id="run_test_connection_script",
        python_callable=run_python_script_2
    )

    t3 = BashOperator(
        task_id="echo_done",
        bash_command="echo 'im done'"
    )

    # Set task dependencies
t1 >> t2 >> t3

