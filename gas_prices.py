from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'vellie',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 20),
    'email': ['velyvineayieta@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='gas_prices_dag',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
    tags=['gas_prices', 'gas']
) as dag:
    
   
    fetch_data = BashOperator(
        task_id='fetch_gas_prices',
        bash_command='python /opt/airflow/dags/scripts/fetch_gas_prices.py',
        retries=3
    )
    
   
    process_data = BashOperator(
        task_id='process_data',
        bash_command='python /opt/airflow/dags/scripts/process_gas_data.py',
        retries=3
    )
    
    
    load_to_db = BashOperator(
        task_id='load_to_database',
        bash_command='python /opt/airflow/dags/scripts/load_to_db.py'
    )
    
   
    generate_reports = BashOperator(
        task_id='generate_reports',
        bash_command='python /opt/airflow/dags/scripts/generate_reports.py'
    )
    
   
    send_notification = BashOperator(
        task_id='send_notification',
        bash_command='python /opt/airflow/dags/scripts/send_email.py'
    )
    
    
    fetch_data >> process_data >> load_to_db
    load_to_db >> [generate_reports, send_notification]