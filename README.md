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

sudo apt install wkhtmltopdf -> should be installed in the machine not env

## variables

create or uploads

{
    "default_email": "xx@cgiar.org",
    "daily_distribution_list": "yy@cgiar.org"    
}



