import os

from datetime import datetime, date, timedelta
from airflow.decorators import dag, task
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from calendar import monthrange

now = datetime.now()

hook = MySqlHook(mysql_conn_id='mysql_adgg_db_production')

log_date = date.today()

current_file_path = os.path.abspath(__file__)  # Get the current file's path
dag_folder = os.path.dirname(os.path.dirname(current_file_path))
scripts_dir = dag_folder + '/dags/utilities/scripts/QA'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag_params = {
    'log_date': log_date,
}

@dag(
    dag_id='Data.QA.Logger.Monthly',
    default_args=default_args,
    schedule_interval="59 20 L * *",  # Every End Month at Midnight Nairobi Time
    template_searchpath=[scripts_dir],
    catchup=False,
    max_active_runs=1,  # Set the maximum number of active runs to 1
    params=dag_params
)
def qa_logger_monthly():
    @task(task_id="Start", provide_context=True)
    def start():
        return "start"

    log_qa = MySqlOperator(
        task_id='Log.QA.Data',
        mysql_conn_id='mysql_adgg_db_production',
        sql='monthly_data_qa_logger.sql'
    )

    @task(task_id="Finish")
    def finish():
        return "finish"



    start() >> [log_qa] >> finish()


qa_logger_monthly()
