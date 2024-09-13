import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
}

# Define the DAG with its configuration
with DAG(
    "simple_dag_example_10_seconds",
    default_args=default_args,
    description="A simple example DAG running every 10 seconds",
    schedule_interval="* * * * *",
    start_date=days_ago(1),  # DAG start date, can be any past date
    catchup=False,  # Only run the latest schedule
) as dag:

    # Define Python functions to be used by the tasks
    def print_current_date():
        print(f"Current date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    def wait_five_seconds():
        time.sleep(5)
        print("Waited for 5 seconds")

    def print_success():
        print("DAG completed successfully!")

    # Define the tasks
    task1 = PythonOperator(
        task_id="print_current_date",
        python_callable=print_current_date,
    )

    task2 = PythonOperator(
        task_id="wait_five_seconds",
        python_callable=wait_five_seconds,
    )

    task3 = PythonOperator(
        task_id="print_success",
        python_callable=print_success,
    )

    # Set task dependencies
    task1 >> task2 >> task3
