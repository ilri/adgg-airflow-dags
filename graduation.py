import os

from datetime import datetime, date, timedelta
from airflow.decorators import dag, task
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from calendar import monthrange

now = datetime.now()

hook = MySqlHook(mysql_conn_id='mysql_adgg_db_production')

grad_date = date.today()

current_file_path = os.path.abspath(__file__)  # Get the current file's path
dag_folder = os.path.dirname(os.path.dirname(current_file_path))
scripts_dir = dag_folder + '/dags/utilities/scripts/graduation'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 16),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id='Graduation',
    default_args=default_args,
    schedule_interval="0 2 * * *",  # Every day at 5:00 AM
    template_searchpath=[scripts_dir],
    catchup=False,
    max_active_runs=1  # Set the maximum number of active runs to 1
)
def graduation():
    @task(task_id="Start", provide_context=True)
    def start():
        return "start"

    graduate = MySqlOperator(
        task_id='Graduate',
        mysql_conn_id='mysql_adgg_db_production',
        sql='graduation.sql'
    )

    @task(task_id="Finish")
    def finish():
        return "finish"



    start() >> [graduate] >> finish()


graduation()
