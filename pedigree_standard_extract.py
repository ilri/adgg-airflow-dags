import gzip
import os
import uuid
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.email import EmailOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.models import Variable


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False
}
now = datetime.now()
hook = MySqlHook(mysql_conn_id='mysql_adgg_db_production')
scripts_dir = f"{Variable.get('scripts_folder')}pedigree"
output_dir = Variable.get("output_folder")
default_email = Variable.get("default_email")



dag_params = {
    "uuid": str(uuid.uuid4()),
    "country": 10,
    "email": default_email
}


def gen_file(df, filename, compressed_filename):
    # generate the filename using the current date and time
    df.to_csv(filename, index=False)
    # compress the CSV file using gzip
    with open(filename, 'rb') as f_in:
        with gzip.open(compressed_filename, 'wb') as f_out:
            f_out.writelines(f_in)

    # remove the original CSV file
    os.remove(filename)
    return compressed_filename


@dag(
    dag_id='Pedigree-Standard-Extract',
    start_date=datetime(2021, 1, 1),
    default_args=default_args,
    template_searchpath=[scripts_dir],
    max_active_runs=1,
    schedule=None,
    catchup=False,
    params=dag_params
)
def pedigree_standard_extract():
    @task(task_id="Start", provide_context=True)
    def start(**context):
        _uuid = context["params"]["uuid"]

        return _uuid

    start = start()

    # Stage Data > Initial Extractions > Data Stored In a Temporary Table
    stage = MySqlOperator(
        task_id='Stage-Data',
        mysql_conn_id='mysql_adgg_db_production',
        sql='pedigree_extract_data.sql',
        params={"country": "{{ dag_run.conf['country']}}", "uuid": start}
    )

    # Check Duplicate Records
    check_duplicates = MySqlOperator(
        task_id='Check-Duplicates',
        mysql_conn_id='mysql_adgg_db_production',
        sql='pedigree_check_duplicates.sql',
        params={"uuid": start}
    )

    # Check Value Dates
    check_value_date = MySqlOperator(
        task_id='Check-Value-Date',
        mysql_conn_id='mysql_adgg_db_production',
        sql='pedigree_check_value_date.sql',
        params={"uuid": start}
    )

    # Check Animal Sex
    check_sex_details = MySqlOperator(
        task_id='Check-Sex-Details',
        mysql_conn_id='mysql_adgg_db_production',
        sql='pedigree_check_sex_details.sql',
        params={"uuid": start}
    )

    # Check Bisexuals
    check_bisexuals = MySqlOperator(
        task_id='Check-Bisexuals',
        mysql_conn_id='mysql_adgg_db_production',
        sql='pedigree_check_bisexuals.sql',
        params={"uuid": start}
    )

    # Check Sires
    check_sires = MySqlOperator(
        task_id='Check-Sire',
        mysql_conn_id='mysql_adgg_db_production',
        sql='pedigree_check_sires.sql',
        params={"uuid": start}
    )

    #
    # # Check Calving Age
    # check_calving_age = MySqlOperator(
    #     task_id='Check-Calving-Age',
    #     mysql_conn_id='mysql_adgg_db_production',
    #     sql='transform_lactation_check_calving_age.sql',
    #     params={"uuid": start}
    # )
    #
    # # Check Animal Type
    # check_animal_type = MySqlOperator(
    #     task_id='Check-Animal-Type',
    #     mysql_conn_id='mysql_adgg_db_production',
    #     sql='transform_lactation_check_animal_type.sql',
    #     params={"uuid": start}
    # )
    #
    @task(task_id="Generate-Reports", provide_context=True)
    def generate_reports(**kwargs):
        # Fetch unique uuid
        unique_id = kwargs['ti'].xcom_pull()

        table_name = 'reports.staging_pedigree_data'

        # Valid Records Report
        valid_columns = ['country', 'region', 'district', 'ward', 'village', 'farmer_name', 'farm_id', 'animal_id',
                         'tag_id', 'original_tag_id', 'sire_tag_id', 'sire_id', 'dam_tag_id', 'dam_id', 'sex',
                         'reg_date', 'birthdate', 'main_breed', 'breed', 'longitude', 'latitude']

        valid_output_csv = f"{output_dir}pedigree-extract-{now.strftime('%Y-%m-%d')}-{unique_id}.csv"
        valid_output_gz = f"{valid_output_csv}.gz"
        valid_sql_query = f"SELECT {', '.join(valid_columns)} FROM {table_name} WHERE status = 1  AND uuid ='{unique_id}' ORDER BY animal_id, reg_date"
        valid_df = hook.get_pandas_df(valid_sql_query)
        valid_rpt = gen_file(valid_df, valid_output_csv, valid_output_gz)

        # Error Report
        error_output_csv = f"{output_dir}error-pedigree-extract-{now.strftime('%Y-%m-%d')}-{unique_id}.csv"
        error_output_gz = f"{error_output_csv}.gz"
        error_columns = ['country', 'region', 'district', 'ward', 'village', 'farmer_name', 'farm_id', 'animal_id',
                         'tag_id', 'original_tag_id', 'sire_tag_id', 'sire_id', 'dam_tag_id', 'dam_id', 'sex',
                         'reg_date', 'birthdate', 'main_breed', 'breed', 'longitude', 'latitude', 'comments']
        error_sql_query = f"SELECT {', '.join(error_columns)} FROM {table_name} WHERE status = 0 AND uuid ='{unique_id}' ORDER BY animal_id, reg_date"
        error_df = hook.get_pandas_df(error_sql_query)
        error_rpt = gen_file(error_df, error_output_csv, error_output_gz)

        rpt_dict = {'valid': valid_rpt, 'error': error_rpt}
        kwargs['ti'].xcom_push(key='my_values', value=rpt_dict)

    reports = generate_reports()

    @task(task_id="Email-Reports")
    def email_reports(**kwargs):
        xcom_values = kwargs['ti'].xcom_pull(key='my_values')
        valid_rpt = xcom_values['valid']
        error_rpt = xcom_values['error']
        recipients_email = kwargs['dag_run'].conf['email']

        send_email_task = EmailOperator(
            task_id='Email-Reports',
            to=recipients_email,
            subject='Pedigree Standard Extract',
            html_content="Hello, <br/> Please check the attachment to access the file extract. <br/><br/>Regards<br/> Apache Airflow",
            files=[valid_rpt, error_rpt]
        )

        return send_email_task.execute(context={})

    # Clean Transaction Tables
    flush_data = MySqlOperator(
        task_id='Flush-Data',
        mysql_conn_id='mysql_adgg_db_production',
        sql='pedigree_flush_data.sql',
        params={"uuid": start}
    )

    @task(task_id="Trash-Files")
    def trash_files(**kwargs):
        # Get Return values of Generate-Reports Task
        xcom_values = kwargs['ti'].xcom_pull(key='my_values')
        valid_rpt = xcom_values['valid']
        error_rpt = xcom_values['error']

        # remove the original CSV file
        os.remove(valid_rpt)
        os.remove(error_rpt)

    @task(task_id="Finish")
    def finish():
        return "finish"

    start >> stage >> [check_duplicates, check_value_date, check_sex_details,
                       check_bisexuals, check_sires] >> reports >> email_reports() >> [
        flush_data, trash_files()] >> finish()


pedigree_standard_extract()
