import os
import uuid
import zipfile

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.utils.email import send_email
from airflow.models import TaskInstance
from airflow.models import Param

from utilities.scripts.testdaylactation.database_manager import DatabaseManager
from utilities.scripts.testdaylactation.milk_report_generator import MilkReportGenerator

# Get the directory of the current DAG file
dag_folder = os.path.dirname(os.path.abspath(__file__))
# Define the paths for scripts and output directories
scripts_dir = os.path.join(dag_folder, 'utilities', 'scripts', 'testdaylactation')
output_dir = os.path.join(dag_folder, 'utilities', 'output')


def report_generate_task(**kwargs):
    country_name = kwargs['params']['country_name']

    # Find the key that corresponds to the selected country_name value
    country_id = None
    for key, value in country_names.items():
        if value == country_name:
            country_id = key
            break

    if country_id is None:
        raise ValueError(f"Invalid country_name: {country_name}")

    db_manager = DatabaseManager(connection_id="mysql_adgg_db_production")
    db_manager.connect_to_database()

    report_generator = MilkReportGenerator(db_manager=db_manager, country_name=country_id, scripts_dir=scripts_dir,
                                           output_dir=output_dir)
    filename = os.path.basename(report_generator.main())
    current_time = datetime.now().strftime("%Y_%m_%d-%H_%M_%S")

    # Generate a unique identifier using uuid module
    unique_identifier = str(uuid.uuid4())[:3]  # Use first 8 characters of the UUID
    unique_filename = f"{country_name}_{filename}_{unique_identifier}_{current_time}.zst"

    original_file_path = os.path.join(output_dir, filename)
    zip_file_path = os.path.join(output_dir, unique_filename)

    with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(original_file_path, arcname=os.path.basename(original_file_path))

    # Delete the original file after compression
    if os.path.isfile(original_file_path):
        try:
            os.unlink(original_file_path)
            print(f'Successfully deleted original file {original_file_path}.')
        except Exception as e:
            print(f'Failed to delete {original_file_path}. Reason: {e}')

    # Push the compressed file path to XCom.
    kwargs['ti'].xcom_push(key='compressed_file_path', value=zip_file_path)
    return "Report Generation Successful...."


def start_task():
    print("Start Task Executed....")


def finish_task():
    print("Finish Task Executed....")


def send_email_with_attachment_task(**context):
    ti = TaskInstance(context['task'], context['execution_date'])
    file_to_send = ti.xcom_pull(task_ids='generate_report', key='compressed_file_path')
    email_to_send = context['params']['email']
    country_name = context['params']['country_name']
    current_time = datetime.now().strftime("%Y_%m_%d-%H_%M_%S")

    send_email(
        to=email_to_send,
        subject=f'Auto-Generated Test Day Report for {country_name}',
        html_content=f"""
            <p>Dear Recipient,</p>

            <p>Please find the attached a report that contains the linking of testday and lactation files for {country_name} </p>

            <p>Keep up the good work! 👍🏿👏🏿 Well done!</p>

            <p>This is an auto-generated email pertaining to the data processing tasks carried out today  at {current_time}.</p>

            <p>Best Regards,</p>
            <p>David</p>
            """,
        files=[file_to_send],
    )
    # Delete the ZIP file after sending the email
    try:
        os.unlink(file_to_send)
        print(f'Successfully deleted file {file_to_send}.')
    except Exception as e:
        print(f'Failed to delete {file_to_send}. Reason: {e}')


country_names = Variable.get("country_names", deserialize_json=True)

default_email = Variable.get("default_email")

dag_params = {
    'email': Param(
        default="example@example.com",
        type="string",
        format="idn-email",
        minLength=5,
        maxLength=255,
    ),
    'country_name': Param(
        default="Tanzania",
        enum=list(country_names.values()),
    ),
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'params': dag_params,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG('milk_report_generation_dag',
         default_args=default_args,
         description='DAG to generate the report',
         schedule_interval=None,
         catchup=False,
         params=dag_params) as dag:
    start = PythonOperator(
        task_id='start_task',
        python_callable=start_task,
        dag=dag,
    )

    testday_lactation_report_generator = PythonOperator(
        task_id='generate_report',
        python_callable=report_generate_task,
        provide_context=True,
        op_kwargs=dag_params,
        dag=dag,
    )

    send_email_task = PythonOperator(
        task_id='send_email',
        python_callable=send_email_with_attachment_task,
        provide_context=True,
        op_kwargs=dag_params,
        dag=dag,
    )

    finish = PythonOperator(
        task_id='finish_task',
        python_callable=finish_task,
        dag=dag,
    )

    start >> testday_lactation_report_generator >> send_email_task >> finish
