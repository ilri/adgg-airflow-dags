
import os

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


dagid='DEV_archive_auditdb'
scheduleinterval="0 2 * * 0"  # utc 


tag4 = 'Housekeeping'
tag3 = 'auditdb'
tag2 = 'scheduled update'
tag1 ='development'
now = datetime.now()

## Define any parameters you want to set at run time
dag_params = {
    'days_interval': 20,
    'whatif': 0,
    'verbose': 0
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
    tags=[tag1, tag2,tag3,tag4]
)


def run_db_proc():
    @task(task_id="Start", provide_context=True)
    def start():
        return "start"

    connectionstring='mysql_adgg_db_dev'
    sqlstmt1="""call workdb.spm_ADMIN_archive_audit_core_farm({{params.days_interval}}, {{params.whatif}})"""
    sqlstmt2="""call workdb.spm_ADMIN_archive_audit_core_animal({{params.days_interval}}, {{params.whatif}}, {{params.verbose}})"""
    sqlstmt3="""call workdb.spm_ADMIN_archive_audit_core_event({{params.days_interval}}, {{params.whatif}}, {{params.verbose}})"""

    execproc1= SQLExecuteQueryOperator(     
        task_id='archive_core_farm',     
        conn_id=connectionstring,     
        sql=sqlstmt1,
        autocommit=True,
        parameters=dag_params
        )
    
    execproc2= SQLExecuteQueryOperator(     
        task_id='archive_core_animal',     
        conn_id=connectionstring,     
        sql=sqlstmt2,
        autocommit=True,
        parameters=dag_params
        )
    
    execproc3= SQLExecuteQueryOperator(     
        task_id='archive_core_event',     
        conn_id=connectionstring,     
        sql=sqlstmt3,
        autocommit=True,
        parameters=dag_params
        )

    @task(task_id="Finish")
    def finish():
        return "finish"

    start() >> [execproc1, execproc2, execproc3] >> finish()


run_db_proc()