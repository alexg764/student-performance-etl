# ETL_Pipeline.py
# -----
# The purpose of this script is to 
# recreate Tableau Prep ETL workflow in Python using 
# Pandas and SQL.
# -----
# Author: Alex Gonzalez
# October 2025

import pandas as pd
from sqlalchemy import create_engine

# STEP 1: EXTRACT DATA
# -----
# Using a snippet of actual database files and extracting
# from a local copy for simplicity.

def extract_data():

    # Reading local CSV files
    math_df = pd.read_csv("math_grades.csv")
    port_df = pd.read_csv("portuguese_grades.csv")

    combined_df = pd.concat([math_df, port_df], ignore_index=True)
    print(f" Extracted {len(combined_df)} total records.")
    print("Step 1 - Data Extraction Complete.")
    return combined_df

# STEP 2 — TRANSFORM

def transform_data(df):
    # Cleaning the dataset based on the original assignment's specifications
    # Normalize column names
    df.columns = df.columns.str.strip().str.lower()

    # Split the combined G1_G2_G3 into separate columns on the basis of a comma
    if 'g1_g2_g3' in df.columns:
        grade_split = df['g1_g2_g3'].str.split(',', expand=True)
        grade_split.columns = ['g1', 'g2', 'g3']
        df = pd.concat([df, grade_split], axis=1)
        df.drop(columns=['g1_g2_g3'], inplace=True)

    # Rename columns
    df.rename(columns={
        'g1': 'First Grade Raw Score',
        'g2': 'Second Grade Raw Score',
        'g3': 'Final Raw Score',
        'dalc': 'Weekday Alcohol Consumption',
        'walc': 'Weekend Alcohol Consumption'
    }, inplace=True)

    # Clean job fields
    valid_jobs = ['teacher', 'health', 'services', 'at_home', 'other']
    df['mjob'] = df['mjob'].apply(lambda x: x if x in valid_jobs else 'other')
    df['fjob'] = df['fjob'].apply(lambda x: x if x in valid_jobs else 'other')

    # Create calculated fields
    df['Total Final Scores'] = (df['Final Raw Score'].astype(float) / 20) * 100

    # Travel time labels
    travel_map = {1: '<15 min.', 2: '15 to 30 min.', 3: '30 min to 1 hour', 4: '>1 hour'}
    df['Total Travel Time'] = df['traveltime'].map(travel_map)

    # Study time labels
    study_map = {1: '<2 hours', 2: '2 to 5 hours', 3: '5 to 10 hours', 4: '>10 hours'}
    df['Total Study Time'] = df['studytime'].map(study_map)

    print("Step 2 - Data Transformation complete.")
    return df


# STEP 3 — LOAD

def load_data(df):
    # Saving cleaned data to CSV file
    output_path = "Class_Performance.csv"
    df.to_csv(output_path, index=False)
    print(f"Data exported to {output_path}")
    print("Step 3 - Loading Data to CSV Complete.")

# STEP 4 — RUN ETL

def run_etl():
    df = extract_data()
    df = transform_data(df)
    load_data(df)
    print("Step 4 - ETL Pipeline complete.")

if __name__ == "__main__":
    run_etl()
