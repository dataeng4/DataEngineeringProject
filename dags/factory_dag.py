from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# 1. Define the rules for the pipeline
default_args = {
    'owner': 'lead_data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2), # Automatically retry after 2 mins on failure
}

# 2. Initialize the Orchestrator
with DAG(
    'enterprise_data_factory',
    default_args=default_args,
    description='Automated execution of the synthetic PostgreSQL data factory',
    schedule_interval='@hourly', # Run automatically every hour
    catchup=False,
    tags=['ETL', 'PostgreSQL'],
) as dag:

    # Task 1: Compile Schema
    task_init_db = BashOperator(
        task_id='initialize_schema',
        bash_command='cd /opt/airflow/project && python database.py',
    )

    # Task 2: Inject Data
    task_generate_data = BashOperator(
        task_id='generate_synthetic_records',
        bash_command='cd /opt/airflow/project && python generator.py',
    )

    # Task 3: Performance Tuning
    task_optimize_db = BashOperator(
        task_id='optimize_database_indexes',
        bash_command='cd /opt/airflow/project && python optimize.py',
    )

    # 3. Define the Dependency Tree (Execution Order)
    task_init_db >> task_generate_data >> task_optimize_db