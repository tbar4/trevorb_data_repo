from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import TaskGroup
import pendulum
import logging

# Set log level for this DAG
logging.getLogger("airflow.task").setLevel(logging.INFO)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'docker_test_dag',
    default_args=default_args,
    description='A simple DAG that runs a Docker container',
    schedule=None,  # Run manually for testing
    start_date=pendulum.datetime(2025, 12, 1, tz="UTC"),
    catchup=False,
    tags=['docker', 'test'],
) as dag:

    # Task to run the Docker container
    # Note: For this to work, the Docker socket must be mounted when running Airflow in Docker:
    # docker run ... -v /var/run/docker.sock:/var/run/docker.sock ...
    run_docker_container = DockerOperator(
        task_id='run_test_docker_airflow',
        image='rust_airflow_hello:latest',
        api_version='auto',
        auto_remove="success",
        command='',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        mount_tmp_dir=False,
        # Pass date information as environment variables
        environment={
            'EXECUTION_DATE': '{{ ds }}',  # execution date in YYYY-MM-DD format
            'DAG_RUN_ID': '{{ run_id }}',  # DAG run ID
        },
        dag=dag,
    )    
    # Set log level for this specific task
    # run_docker_container - task objects don't have setLevel() method