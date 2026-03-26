from weather_api import get_weather_data

import pandas as pd
import sqlite3
import logging
import os
import time

logging.basicConfig(
    level=logging.INFO,                        # INFO captures regular events; ERROR captures crashes.
    format='%(asctime)s - %(name)s - %(message)s', # Adds a timestamp and the module name.
    handlers=[
        logging.FileHandler("pipeline.log",encoding='utf-8'),   # Writes the "Journal" to a physical file.
        logging.StreamHandler()                # Streams the same "Journal" to your terminal.
    ]
)


def get_weather_with_delay(lat, lon):
    # The "Politeness" Delay
    time.sleep(0.1) 
    return get_weather_data(lat, lon)


def clean_name(full_name):
    # This function takes "John Albert Doe" and returns "John Doe"
    parts = str(full_name).split()
    if len(parts) >= 2:
        return f"{parts[0]} {parts[-1]}"
    return full_name

def get_standardized_data(filepath):
    # Step 1: Check if file exists to prevent a crash (Defensive Programming)
    if not os.path.exists(filepath):
        logging.error(f"File not found: {filepath}")
        raise FileNotFoundError(f"Missing: {filepath}")
    df = pd.read_csv(filepath, encoding_errors='ignore')

    # THE FIX: Force all columns to lowercase and remove ALL hidden spaces
    df.columns = df.columns.str.strip().str.replace('\n', '').str.replace('\r', '').str.lower()

    # DEBUG LINE: Let's see what Pandas actually sees in your CSV
    logging.info(f"📊 Columns found in {filepath}: {list(df.columns)}")

    # Handle Missing Salaries (Data Quality Gate)
    if 'salary' in df.columns:
        df['salary'] = pd.to_numeric(df['salary'], errors='coerce').fillna(0)
        
    return df

def run_production_pipeline():
    """
    Purpose: Orchestrates the ETL flow with full error handling.
    Term: Idempotency & Data Integrity.
    """
    conn = None
    try:
        logging.info("🚀 Pipeline Started: Extracting Data...")
        
        # 1. EXTRACT
        df_employees = get_standardized_data('data.csv')
        df_dept = get_standardized_data('departments.csv')


        # 2. TRANSFORM (The 1% Moves)
        # Lambda for string cleaning
        if 'name' in df_employees.columns:
            df_employees['name'] = df_employees['name'].apply(lambda x: str(x).strip().title())
        else:
            logging.error(f"🚨 CRITICAL: 'name' column missing! Available: {df_employees.columns}")
            raise KeyError("The column 'name' was not found in df_employees")
        
        # Type Casting for Join Keys (Preventing "Object vs Int" errors)
        df_employees['department'] = df_employees['department'].astype(str)
        df_dept['dept_id'] = df_dept['dept_id'].astype(str)
        
        # LEFT JOIN: Ensure no employee is lost if dept is missing
        df_final = pd.merge(df_employees, df_dept, left_on='department', right_on='dept_id', how='left')


        # 3. DATABASE SETUP (The Schema Contract)
        conn = sqlite3.connect('company.db')
        cursor = conn.cursor()
        
        # Manually define table to enforce PRIMARY KEY (Data Integrity)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS employee_master (
                id INTEGER PRIMARY KEY, 
                name TEXT, 
                department TEXT, 
                salary REAL,
                dept_name TEXT,
                manager TEXT,
                current_temp REAL
            )
        ''')
        conn.commit()
        # 4. LOAD (Idempotency Check)
        # Use 'append' + Primary Key to prevent duplicate data
        # Note: We filter columns to match our SQL table exactly
        cols_to_load = ['id', 'name', 'department', 'salary', 'dept_name', 'manager', 'current_temp']

        # --- NEW: Data Enrichment Step ---
        logging.info("☁️ Fetching live weather for office locations...")

        # Mapping: Dept 1 = London, Dept 2 = New York
        city_map = {
            '1': (51.5, -0.12),  # London
            '2': (40.7, -74.0)   # New York
        }

        # Create a new column by applying our API tool
        # x is the department ID from the row
        df_final['current_temp'] = df_final['department'].apply(
            lambda x: get_weather_with_delay(city_map[x][0], city_map[x][1]) if x in city_map else None
        )


        df_final[cols_to_load].to_sql('employee_master', conn, if_exists='append', index=False)
        logging.info("✅ Pipeline Success: Data pushed to employee_master.")
      


    except sqlite3.IntegrityError as e:
        logging.error(f"⚠️ Data Integrity Issue: {e} (Likely a duplicate ID)")
    except Exception as e:
        logging.error(f"❌ Critical Pipeline Failure: {e}")
        
    finally:

        # --- NEW PLACEMENT FOR VERIFICATION ---
        """ Method 1: Direct SQL Query (More Control)
        # We do this here so it runs even if the 'try' block hits a duplicate error
        try:
            temp_conn = sqlite3.connect('company.db')
            results = pd.read_sql("SELECT name, dept_name, current_temp FROM employee_master", temp_conn)
            print("\n--- Current Database State ---")
            print(results)
            temp_conn.close()
        except Exception as ver_err:
            logging.error(f"Could not verify data: {ver_err}") """
        #Method 2: Using Pandas (Less Control, More Convenience)
        if conn:
            try:
                # We use the connection we already have!
                results = pd.read_sql("SELECT name, dept_name, current_temp FROM employee_master", conn)
                print("\n--- Current Database State ---")
                print(results)
            except Exception as e:
                logging.error(f"Could not verify data: {e}")

        # Resource Cleanup (The Guarantee)
        if conn:
            conn.close()
            logging.info("🔒 Connection Closed: Resources released.")



if __name__ == "__main__":
    run_production_pipeline()