from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import BranchPythonOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 26),
    'email': ['swastiksc1996prof@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'recommendation-pipeline',
    default_args=default_args,
    description='Movie Recommendation Pipeline',
    schedule_interval='0 0 * * *',
    catchup=False
)

def check_rf_model_run(**context):
    execution_date = context['execution_date']
    days_since_start = (execution_date - datetime(2024, 10, 26)).days
    if days_since_start % 3 == 0:
        return 'rf_model'
    return 'skip_rf_model'

def run_kafka_consumer():
    script_path = 'group-project-f24-skynetsaviors/main/data_fetch/kafka_consumer.py'
    process = subprocess.Popen(['python', script_path])
    try:
        process.wait(timeout=60)  # SET TIME In SECOND 3600 seconds = 1 hour
    except subprocess.TimeoutExpired:
        process.terminate()
        process.wait()

def run_movies_to_db():
    script_path = 'group-project-f24-skynetsaviors/main/data_fetch/movies_to_db.py'
    subprocess.run(['python', script_path], check=True)

def run_tmdb():
    script_path = 'group-project-f24-skynetsaviors/main/data_fetch/tmdb.py'
    subprocess.run(['python', script_path], check=True)

def run_preprocessing():
    script_path = 'group-project-f24-skynetsaviors/main/data_fetch/pre_processing.py'
    subprocess.run(['python', script_path], check=True)

def run_rf_model():
    script_path = 'group-project-f24-skynetsaviors/main/recommendation_model/rf_model_pipeline.py'
    subprocess.run(['python', script_path], check=True)

# Tasks
start_task = BashOperator(
    task_id='start_pipeline',
    bash_command='echo "Starting Movie Recommendation Pipeline - $(date)"',
    dag=dag,
)

kafka_task = PythonOperator(
    task_id='kafka_consumer',
    python_callable=run_kafka_consumer,
    dag=dag,
)

movies_db_task = PythonOperator(
    task_id='movies_to_db',
    python_callable=run_movies_to_db,
    dag=dag,
)

tmdb_task = PythonOperator(
    task_id='tmdb',
    python_callable=run_tmdb,
    dag=dag,
)

preprocessing_task = PythonOperator(
    task_id='preprocessing',
    python_callable=run_preprocessing,
    dag=dag,
)

# Branch operator to decide whether to run RF model
check_rf_model = BranchPythonOperator(
    task_id='check_rf_model',
    python_callable=check_rf_model_run,
    provide_context=True,
    dag=dag,
)

rf_model_task = PythonOperator(
    task_id='rf_model',
    python_callable=run_rf_model,
    dag=dag,
)

# Task for days when RF model should be skipped
skip_rf_task = BashOperator(
    task_id='skip_rf_model',
    bash_command='echo "Skipping RF model for today"',
    dag=dag,
)

end_task = BashOperator(
    task_id='end_pipeline',
    bash_command='echo "Movie Recommendation Pipeline Completed - $(date)"',
    trigger_rule='none_failed',
    dag=dag,
)

# Set task dependencies
start_task >> [kafka_task, movies_db_task, tmdb_task] >> preprocessing_task >> check_rf_model
check_rf_model >> [rf_model_task, skip_rf_task] >> end_task