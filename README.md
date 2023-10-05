# adgg-airflow-dags

## MySQL Metadata DB  Config
Do not use SQLite as metadata DB in production – it should only be used for dev/testing. We recommend using Postgres or MySQL
sql_alchemy_conn = mysql://db_user:db_password@localhost:3306/dbname

# switch from default Sequential Executor to Local
Do not use SequentialExecutor in production. You also need to remove SQLite as a metastore as a prerequisite
check line 24 on config
change executor = SequentialExecutor to executor = LocalExecutor


## Remove default or Example DAGS & Connections
This can be done on the config file or Environment variables
load_examples = False
or 
export AIRFLOW__CORE__LOAD_EXAMPLES=False

## Reset Metastore/DB after removing examples
airflow db init
Alternatively you can go into the airflow_db and manually delete those entries from the dag table.

## Dependencies
pip install pandas
pip install 'apache-airflow-providers-common-sql[pandas]'
pip install 'apache-airflow[mysql]'
pip install reportlab 
pip install matplotlib
pip install pdfkit
pip install seaborn
pip install tqdm
pip install mysql-connector-python


sudo apt install wkhtmltopdf -> should be installed in the machine not env

## Airflow variables
you can create or upload for airflow ui

{
    "default_email": "xx@cgiar.org",
    "daily_distribution_list": "yy@cgiar.org"    
}

## directories
Inside the dag folder, create a directory called utilities. The utilities dir should have subdirectories below
img  
output  
scripts  
style

## log rotation Settings
in your Airflow configuration file (airflow.cfg), you can set up log rotation settings to control the behavior of log files. Here are some relevant configuration options:
log_retention_days: Specify the number of days to retain log files. Old log files beyond this retention period will be deleted.
log_rotation_interval: Set the interval at which log files should be rotated. This interval could be based on time (e.g., daily, hourly) or log size (e.g., every 100 MB).
log_file_max_size: Define the maximum size for individual log files. When a log file exceeds this size, a new one is created.

Example

[scheduler]
log_retention_days = 30
log_rotation_interval = 1 D
log_file_max_size = 100000000 # 100 MB






