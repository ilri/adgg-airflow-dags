
import os

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.mysql.operators.mysql import MySqlOperator


mysql_conn='mysql_adgg_db_production'
dagid='PROD_ADMIN_clean_scratchdb'
scheduleinterval="0 2 * * 0"  # utc 
sqlproc="call workdb.spm_ADMIN_cleanup_scratchdb"
taskid = sqlproc[0,30]
tag1 = 'Sample'
tag2 = 'Another tag'
now = datetime.now()

## Define any parameters you want to set at run time
dag_params = {
    'param1': 90,
    'param2': 365
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
@dag(
    dag_id=dagid,
    default_args=default_args,
    schedule_interval=scheduleinterval,  # utc 
    catchup=False,
    max_active_runs=1,  # Set the maximum number of active runs to 1
    params=dag_params,
    tags=[tag1, tag2])
)

def run_db_proc():
    @task(task_id="Start", provide_context=True)
    def start():
        return "start"
    
    sqlstmt="""{{sqlproc}}({{params.param1}}, {{params.param2}})"""

    execproc= MySqlOperator(     
        task_id=taskid,     
        mysql_conn_id=mysql_conn,     
        sql=sqlstmt,
        parameters=dag_params
        )
    

    @task(task_id="Finish")
    def finish():
        return "finish"

    start() >> [execproc] >> finish()


run_db_proc()
