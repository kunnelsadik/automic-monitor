from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy_access import pyodbc
import urllib

 

def ordinal_days_nanos_to_datetime(days, nanoseconds):
    base = datetime(1, 1, 1)
    return base + timedelta(days=days, seconds=nanoseconds / 1_000_000_000)
 

def decode_access_extended_bytes(byte_data):
    """
    Decodes MS Access Date/Time Extended bytes specifically for:
    b'0000000000000739694:0000000258140000000:7\x00'
    Target: 2026-03-20 07:10:14
    """
    try:
        if byte_data:
            # 1. Clean and Split
            clean_str = byte_data.decode('utf-8').strip('\x00')
            parts = clean_str.split(':')
            
            # Part 0 is the "Tick" count
            days = int(parts[0]) 
            # Part 1 is the high-precision nanoseconds into the day
            nanoseconds_today = int(parts[1])  *100
            
            # 2. Define the MS Access Base Epoch (1899-12-30)
            base_epoch = datetime(1, 1, 1)

            # 3. Calculation:                        
           
            ts =  ordinal_days_nanos_to_datetime(days,nanoseconds_today)
            return ts
        else:
            return None
 
    except Exception as e:
        return f"Error: {e}"

# --- 1. Database Connection ---
def get_data():
    
    # DB_PATH = r"C:\Users\afe3356\OneDrive - Health Partners Plans, Inc\Documents\prod_support_modern.accdb"
    DB_PATH = r"C:\Users\afe3356\OneDrive - Health Partners Plans, Inc\Documents\prod_crl_modern.accdb"
    conn_str = f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={DB_PATH};"
    
    params = urllib.parse.quote_plus(
        r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        rf"DBQ={DB_PATH};"
    )

    engine = create_engine(f"access+pyodbc:///?odbc_connect={params}")

    # with pyodbc.connect(conn_str) as conn:
    #     
    #     query = "SELECT * FROM job_stats"
    #     df = pd.read_sql(query, conn)
    query = "SELECT * FROM job_stats"
    query = """
SELECT
    C.object_name AS workflow_name,
    C.business_function,
    A.*
FROM
   ( job_stats AS A
     INNER JOIN workflow_stats AS B
         ON A.workflow_run_id = B.run_id)
    INNER JOIN [workflows] AS C
         ON C.workflow_id = B.workflow_id ;
"""
    df = pd.read_sql(query, engine)
    
    # Clean data
    #df['start_time'] = pd.to_datetime(df['start_time'], errors='coerce')
    df['start_time'] = df['start_time'].apply(decode_access_extended_bytes)
    df['end_time'] = df['end_time'].apply(decode_access_extended_bytes)
    #df = df.dropna(subset=['start_time']) # Remove invalid dates
    # print(df[['run_id','start_time','end_time']].head())
    
    # ✅ CRITICAL FIX
    df['start_time'] = pd.to_datetime(df['start_time'], errors='coerce')
    df['end_time'] = pd.to_datetime(df['end_time'], errors='coerce')
    df = df.dropna(subset=['start_time'])
     

    
    return df