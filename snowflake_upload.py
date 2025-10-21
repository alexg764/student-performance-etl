# snowflake_upload.py
# -----
# This is an optional script to load the ETL output (Class_Performance.csv)
# into a Snowflake table for analysis.
# -----
# Usage:
#   1. Make sure Class_Performance.csv exists (run ETL_Pipeline.py first)
#   2. Set your Snowflake credentials as environment variables (recommended)
#   3. Run: python snowflake_upload.py
# -----
# Author: Alex Gonzalez
# October 2025

import snowflake.connector
import os
import sys

# Configuration

# Read credentials from environment variables (recommended for security)
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD", "")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "STUDENT_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")

CSV_FILE = "Class_Performance.csv"
TABLE_NAME = "CLASS_PERFORMANCE"

# ----------------------------
# Upload Function
# ----------------------------

def upload_to_snowflake():
    
    # This will connect to Snowflake and loads the ETL output CSV into a table.
    
    # Check credentials
    required = [SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT]
    if not all(required):
        sys.exit("Missing credentials. Please set environment variables first.")

    # Check that CSV exists
    if not os.path.exists(CSV_FILE):
        sys.exit(f"{CSV_FILE} not found. Run ETL_Pipeline.py first.")

    print("Connecting to Snowflake...")

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )

    cs = conn.cursor()
    try:
        print("Creating target table if not exists...")

        # Create table dynamically if not already there
        create_stmt = f"""
        CREATE OR REPLACE TABLE {TABLE_NAME} (
            school STRING,
            sex STRING,
            age INT,
            address STRING,
            famsize STRING,
            pstatus STRING,
            medu INT,
            fedu INT,
            mjob STRING,
            fjob STRING,
            reason STRING,
            guardian STRING,
            traveltime INT,
            studytime INT,
            failures INT,
            schoolsup STRING,
            famsup STRING,
            paid STRING,
            activities STRING,
            nursery STRING,
            higher STRING,
            internet STRING,
            romantic STRING,
            famrel INT,
            freetime INT,
            goout INT,
            "Weekday Alcohol Consumption" INT,
            "Weekend Alcohol Consumption" INT,
            health INT,
            absences INT,
            "First Grade Raw Score" FLOAT,
            "Second Grade Raw Score" FLOAT,
            "Final Raw Score" FLOAT,
            "Total Final Scores" FLOAT,
            "Total Travel Time" STRING,
            "Total Study Time" STRING
        );
        """
        cs.execute(create_stmt)

        print("Uploading CSV data to Snowflake...")
        put_stmt = f"PUT file://{CSV_FILE} @%{TABLE_NAME} OVERWRITE = TRUE"
        copy_stmt = f"""
        COPY INTO {TABLE_NAME}
        FROM @%{TABLE_NAME}
        FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
        """
        cs.execute(put_stmt)
        cs.execute(copy_stmt)

        print(f"Successfully loaded {CSV_FILE} into {TABLE_NAME} in Snowflake!")

    except Exception as e:
        print(f"Error uploading to Snowflake: {e}")

    finally:
        cs.close()
        conn.close()
        print("Connection closed.")


if __name__ == "__main__":
    upload_to_snowflake()
