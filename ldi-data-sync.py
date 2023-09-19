
import os

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

now = datetime.now()
hook = MySqlHook(mysql_conn_id='mysql_adgg_db_production')

current_file_path = os.path.abspath(__file__)# Get the current file's path
dag_folder = os.path.dirname(os.path.dirname(current_file_path))
scripts_dir = dag_folder + '/dags/utilities/scripts/ldi'

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
    dag_id='LDI-Data-Sync',
    default_args=default_args,
    schedule_interval="0 4 * * 1-5",  # Monday to Friday at 7:00 AM
    template_searchpath=[scripts_dir],
    catchup=False,
    max_active_runs=1  # Set the maximum number of active runs to 1


)
def ldi_data_sync():
    @task(task_id="Start", provide_context=True)
    def start():
        return "start"


    sync_avg_milk_data = MySqlOperator(
        task_id='Sync-Milk-Yield-Data',
        mysql_conn_id='mysql_adgg_db_production',
        sql='ldi_average_milk_yield_per_cow_by_breed_and_region.sql'
    )

    sync_weekly_ai_service_data = MySqlOperator(
        task_id='Sync-Weekly-AI-Service-Data',
        mysql_conn_id='mysql_adgg_db_production',
        sql='ldi_weekly_ai_services_by_region_and_technician.sql'
    )

    sync_herd_registration_data = MySqlOperator(
        task_id='Sync-Herd-Reg-Data',
        mysql_conn_id='mysql_adgg_db_production',
        sql='ldi_herds_by_type.sql'
    )

    sync_animal_reg_data = MySqlOperator(
        task_id='Sync-Animal-Reg-Data',
        mysql_conn_id='mysql_adgg_db_production',
        sql='ldi_animal_reg_by_region.sql'
    )

    @task(task_id="Finish")
    def finish():
        return "finish"

    start() >> [sync_avg_milk_data,sync_weekly_ai_service_data,sync_herd_registration_data,sync_animal_reg_data] >> finish()


ldi_data_sync()
