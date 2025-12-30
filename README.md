# student-performance-etl
This is a Python based ETL pipeline that cleans and transforms student performance data, recreating an academic Tableau Prep workflow that I developed
in my Data Mining class at Texas A&M using modern data engineering tools.

---

## Overview

This project demonstrates how to design a lightweight, production-style ETL workflow for real-world data engineering use cases.  
It extracts raw student performance data from two sources (Math and Portuguese classes), transforms and normalizes it, and outputs a clean dataset for analysis, with optional integration to **Snowflake** for cloud storage.

---

## Objectives

- Combine multiple datasets from different sources  
- Split combined grade columns into individual `G1`, `G2`, `G3` values  
- Standardize and clean categorical data (e.g., parental jobs, alcohol use)  
- Add calculated metrics (`Total Final Scores`, `Total Study Time`, etc.)  
- Export the final dataset for analytics or load it into Snowflake  

---

## Tech Stack

| Stage | Tools Used |
|--------|-------------|
| Extract | SQL Server, Pandas |
| Transform | Python (Pandas), Data Cleaning |
| Load | CSV Output, Snowflake (Optional) |
| Orchestration (Optional) | Apache Airflow DAG |
| Version Control | Git + GitHub |

---

## How to Run Locally

### i. Clone the repository
```bash
git clone https://github.com/alexg764/student-performance-etl.git
cd student-performance-et
```

### ii. Create and activate a virtual environment
```bash
python -m venv venv
venv\Scripts\activate        # Windows
```
or
```bash
source venv/bin/activate      # Mac/Linux
```

### iii. Install dependencies
```bash
pip install -r requirements.txt
```

### iv. Run ETL pipeline
```bash
python ETL_Pipeline.py
```

This script will:
- Merge the math and Portuguese datasets
- Split the combined grade column (`G1_G2_G3)` into `G1`, `G2`, `G3`
- Clean and standardize categorical fields (`Mjob`, `Fjob`, etc.)
- Create calculated fields for percentages, travel time, and study time
- Export the cleaned dataset as `Class_Performance.csv`

### Optional: Load to Snowflake
To load the final dataset to Snowflake for further analysis, set your Snowflake credentials using:
```powershell
$env:SNOWFLAKE_USER="your_username"
$env:SNOWFLAKE_PASSWORD="your_password"
$env:SNOWFLAKE_ACCOUNT="abcd-xy12345.us-east-1"
$env:SNOWFLAKE_DATABASE="STUDENT_DB"
$env:SNOWFLAKE_SCHEMA="PUBLIC"
$env:SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
```

Then run the uploader:
```bash
python snowflake_upload.py
```

This will create a Snowflake table named `CLASS_PERFORMANCE`, uploading the CSV data and loading it automatically.

### Optional: Airflow DAG Automation
For scheduling and orchestration, add this simple DAG - `test_etl_dag.py`:
```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from ETL_Pipeline import run_etl

with DAG(
    dag_id="student_performance_etl",
    start_date=datetime(2025, 10, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:
    run_etl_task = PythonOperator(
        task_id="run_etl_pipeline",
        python_callable=run_etl
    )
```
This allows for automation and monitoring of the ETL job in Apache Airflow.

---

## Author
Alex Gonzalez  
Texas A&M Class of 2026 (Whoop!)  

## License
This project is licensed under the **MIT License** - free for educational and professional use.


