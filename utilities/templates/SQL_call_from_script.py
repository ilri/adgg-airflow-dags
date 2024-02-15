
import os

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.mysql.operators.mysql import MySqlOperator
#from airflow.providers.mysql.hooks.mysql import MySqlHook

now = datetime.now()
#hook = MySqlHook(mysql_conn_id='mysql_adgg_db_production')
mysqlconnid='mysql_adgg_db_production'

current_file_path = os.path.abspath(__file__)# Get the current file's path
dag_folder = os.path.dirname(os.path.dirname(current_file_path))
scripts_dir = dag_folder + '/dags/utilities/scripts/myscriptdir' # create a dedicated script path in utilities
dagid='My_Script_Name_which_appears_in_the_Airflow_site'
scheduleinterval="0 2 * * 0"  # utc 
taskid = "Name_of_script"
taskid1 = "Start_Execution"
taskid2 = "Complete_Execution"
sqlscript1='SQLscript.sql'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 20),
    'retries': 1,
    # 'start_date': make_aware(now, timezone_nairobi),  # Use make_aware to set timezone
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
@dag(
    dag_id=dagid,
    default_args=default_args,
    schedule_interval=scheduleinterval,  
    template_searchpath=[scripts_dir],
    catchup=False,
    max_active_runs=1  # Set the maximum number of active runs to 1
)

def run_db_script():
    @task(task_id=taskid1, provide_context=True)
    def start():
        return "start"

    exec_SQL_script = MySqlOperator(
        task_id=taskid,
        mysql_conn_id=mysqlconnid,
        sql=sqlscript1
    )

    @task(task_id=taskid2)
    def finish():
        return "finish"

    start() >> [exec_SQL_script] >> finish()


run_db_script()
