import gzip
import os
import uuid
from datetime import datetime, date, timedelta

from airflow.decorators import dag, task
from airflow.operators.email import EmailOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.mysql.operators.mysql import MySqlOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False
}

dag_params = {
    "uuid": str(uuid.uuid4()),
    "country": 10,
    "email": "g.kipkosgei@cgiar.org",
    'start_date': date.today() - timedelta(days=365),
    'end_date': date.today(),
}

output_location = "/home/kosgei/airflow/output/"
now = datetime.now()
hook = MySqlHook(mysql_conn_id='mysql_adgg_db_production')


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
    dag_id='Weights-Standard-Extract',
    start_date=datetime(2021, 1, 1),
    default_args=default_args,
    template_searchpath=['/home/kosgei/airflow/scripts'],
    max_active_runs=1,
    schedule="@daily",
    catchup=False,
    params=dag_params
)
def etl_weight():
    @task(task_id="Start", provide_context=True)
    def start(**context):
        _uuid = context["params"]["uuid"]
        return _uuid

    start = start()

    # Stage Data > Initial Extractions > Data Stored In a Temporary Table
    stage = MySqlOperator(
        task_id='Stage-Data',
        mysql_conn_id='mysql_adgg_db_production',
        sql='extract_weight_data.sql',
        params={"country": "{{ dag_run.conf['country']}}", "uuid": start,
                "start_date": "{{ dag_run.conf['start_date']}}",
                "end_date": "{{ dag_run.conf['end_date']}}"}

    )

    # Check Heart Girth
    check_heart_girth = MySqlOperator(
        task_id='Check-Heart-Girth',
        mysql_conn_id='mysql_adgg_db_production',
        sql='transform_weight_data_heart_girth_check.sql',
        params={"uuid": start}
    )

    # Check Weight
    check_weight = MySqlOperator(
        task_id='Check-Weight',
        mysql_conn_id='mysql_adgg_db_production',
        sql='transform_weight_data_weight_check.sql',
        params={"uuid": start}
    )

    # Check Duplicate Records
    check_duplicates = MySqlOperator(
        task_id='Check-Duplicates',
        mysql_conn_id='mysql_adgg_db_production',
        sql='transform_weight_data_check_duplicates.sql',
        params={"uuid": start}
    )

    # Check Dates
    check_dates = MySqlOperator(
        task_id='Check-Dates',
        mysql_conn_id='mysql_adgg_db_production',
        sql='transform_weight_data_date_check.sql',
        params={"uuid": start}
    )

    # Check Dates
    check_gps = MySqlOperator(
        task_id='Check-GPS',
        mysql_conn_id='mysql_adgg_db_production',
        sql='transform_weight_data_check_gps.sql',
        params={"uuid": start}
    )

    @task(task_id="Generate-Reports", provide_context=True)
    def generate_reports(**kwargs):
        # Fetch unique uuid
        unique_id = kwargs['ti'].xcom_pull()

        table_name = 'reports.staging_weight_data'

        # Valid Records Report
        valid_columns = ['country', 'region', 'district', 'ward', 'village', 'registration_date', 'animal_id', 'tag_id',
                         'birthdate', 'main_breed', 'sex', 'animal_type', 'longitude', 'latitude', 'weight_date',
                         'age_at_weighing', 'heart_girth', 'body_weight']

        valid_output_csv = f"{output_location}weight-extract-{now.strftime('%Y-%m-%d')}-{unique_id}.csv"
        valid_output_gz = f"{valid_output_csv}.gz"
        valid_sql_query = f"SELECT {', '.join(valid_columns)} FROM {table_name} WHERE status = 1 AND uuid ='{unique_id}'"
        valid_df = hook.get_pandas_df(valid_sql_query)
        valid_rpt = gen_file(valid_df, valid_output_csv, valid_output_gz)

        # Error Report
        error_output_csv = f"{output_location}error-weight-extract-{now.strftime('%Y-%m-%d')}-{unique_id}.csv"
        error_output_gz = f"{error_output_csv}.gz"
        error_columns = ['country', 'region', 'district', 'ward', 'village', 'registration_date', 'animal_id', 'tag_id',
                         'birthdate', 'main_breed', 'sex', 'animal_type', 'longitude', 'latitude', 'weight_date',
                         'age_at_weighing', 'heart_girth', 'body_weight', 'comments']
        error_sql_query = f"SELECT {', '.join(error_columns)} FROM {table_name} WHERE status = 0 AND uuid ='{unique_id}'"
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
            subject='Weights Standard Extract',
            html_content="Hello, <br/> Please check the attachment to access the file extract. <br/><br/>Regards<br/> Apache Airflow",
            files=[valid_rpt, error_rpt]
        )

        return send_email_task.execute(context={})

    # Clean Transaction Tables
    flush_data = MySqlOperator(
        task_id='Flush-Data',
        mysql_conn_id='mysql_adgg_db_production',
        sql='weight_flush_data.sql',
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

    start >> stage >> [check_heart_girth, check_weight, check_duplicates, check_dates,
                       check_gps] >> reports >> email_reports() >> [
        flush_data, trash_files()] >> finish()


etl_weight()
