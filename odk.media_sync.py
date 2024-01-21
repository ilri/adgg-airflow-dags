import os
from datetime import datetime,timedelta
from airflow.decorators import dag, task
from airflow.operators.bash_operator import BashOperator

current_file_path = os.path.abspath(__file__)  # Get the current file's path
dag_folder = os.path.dirname(os.path.dirname(current_file_path))
python_script = dag_folder + '/dags/utilities/scripts/odk_media_files/odk_sync.py'


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
@dag(
    dag_id='ODK.Media.Sync',
    default_args=default_args,
    schedule_interval='0 2-16 * * 1-6',  # Mon-Sat 5am to 7pm
    catchup=False,
    max_active_runs=1  # Set the maximum number of active runs to 1
)

def media_sync():
    @task(task_id="Start", provide_context=True)
    def start():
        return "start"


    sync = BashOperator(
        task_id='Sync',
        bash_command = f'python3 {python_script}'
    )

    @task(task_id="Finish")
    def finish():
        return "finish"

    start() >> sync >> finish()

media_sync()
