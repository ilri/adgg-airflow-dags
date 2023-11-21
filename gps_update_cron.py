import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from utilities.scripts.gps.gps_update import GpsUpdate

# Get the directory of the current DAG file
dag_folder = os.path.dirname(os.path.abspath(__file__))
# Define the paths for scripts and output directories
scripts_dir = os.path.join(dag_folder, 'utilities', 'scripts', 'gps')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('gps_update_cron', default_args=default_args, schedule_interval='@weekly')


def update_gps():
    GpsUpdate.update_gps_coordinates()


# Tasks
start = PythonOperator(
    task_id='start',
    python_callable=lambda: print('Start task'),
    dag=dag
)

gps_update = PythonOperator(
    task_id='gps_update',
    python_callable=update_gps,
    dag=dag
)

end = PythonOperator(
    task_id='end',
    python_callable=lambda: print('End task'),
    dag=dag
)

# Task dependencies
start >> gps_update >> end
