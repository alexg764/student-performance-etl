# test_etl_dag.py
# -----
# The purpose of this script is to 
# provide an optional Airflow DAG script for
# scheduling.
# -----
# Author: Alex Gonzalez
# October 2025

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from ETL_Pipeline import run_etl

# This is an optional Airflow DAG script for scheduling.

with DAG(
    dag_id="alexg-student-etl",
    start_date=datetime(2025, 10, 7),
    schedule_interval="@daily",
    catchup=False
) as dag:
    run_etl_task = PythonOperator(
        task_id="run_etl_pipeline",
        python_callable=run_etl
    )
