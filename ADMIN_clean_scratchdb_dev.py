
import os

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

now = datetime.now()
mysql_conn='mysql_adgg_db_dev'

dag_params = {
    'std_days': 90,
    'keep_days': 365
}

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
    dag_id='DEV_ADMIN_clean_scratchdb',
    default_args=default_args,
    schedule_interval="0 2 * * 0",  # utc 
    catchup=False,
    max_active_runs=1,  # Set the maximum number of active runs to 1
    tags=['clean', 'database','dev'],
    params=dag_params
)

def clean_scratchdb():
    @task(task_id="Start", provide_context=True)
    def start():
        return "start"
    
    sqlstmt="""call workdb.spm_ADMIN_cleanup_scratchdb({{params.std_days}}, {{params.keep_days}})"""

    clean_scratch= MySqlOperator(     
        task_id='execute_stored_procedure',     
        mysql_conn_id=mysql_conn,     
        sql=sqlstmt,
        parameters=dag_params,     
      #  dag=dag 
        )
    

    @task(task_id="Finish")
    def finish():
        return "finish"

    start() >> [clean_scratch] >> finish()


clean_scratchdb()
