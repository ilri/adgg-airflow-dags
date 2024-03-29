
import os

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

dagid='PROD_clean_scratchdb'
scheduleinterval="0 2 * * 0"  # utc 

tag3 = 'Housekeeping'
tag2 = 'scheduled update'
tag1 ='production'
now = datetime.now()

## Define any parameters you want to set at run time
dag_params = {
    'std_days': 90,
    'keep_days': 365
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 20),
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id=dagid,
    default_args=default_args,
    schedule_interval=scheduleinterval,  # utc 
    catchup=False,
    max_active_runs=1,  # Set the maximum number of active runs to 1
    params=dag_params,
    tags=[tag1, tag2, tag3]
)


def run_db_proc():
    @task(task_id="Start", provide_context=True)
    def start():
        return "start"

    
    sqlstmt="""call workdb.spm_ADMIN_cleanup_scratchdb({{params.std_days}}, {{params.keep_days}})"""

    execproc= SQLExecuteQueryOperator(     
        task_id='Execute_proc',     
        conn_id='mysql_adgg_db_production',     
        sql=sqlstmt,
        autocommit=True,
        parameters=dag_params
        )
    

    @task(task_id="Finish")
    def finish():
        return "finish"

    start() >> [execproc] >> finish()


run_db_proc()