import os
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
from pathlib import Path

from utils import db_connection

load_dotenv(".env")

def run_sql_query_from_file(file_path: str, database: str) -> Optional[pd.DataFrame]:
    """
    Executes a SQL query from a .sql file and returns the result as a DataFrame.

    Args:
        file_path : str
            Path to the SQL file.
        database : str
            Name of the database to connect to.

    Returns: 
        Optional[pd.DataFrame] : 
            Query result as a DataFrame if successful, else None.
    """
    
    try:
        conn, mycursor = db_connection(host=os.getenv('HOST'),
                      user="root",
                      password=os.getenv('PASSWORD'),
                      database=database)
        
        with open(file_path, 'r') as f:
            query = f.read()
        
        df = pd.read_sql(query, conn)
        print(f"Query executed successfully. {len(df)} rows retrieved.")
        return df
    except FileNotFoundError: #work on this
        print(f"Query file not found: {file_path}")
    except Exception as e:
        print(f"Error executing query from file: {e}")

def process() -> None:
    """
    Runs a SQL query from file, loads data, and saves the result to S3.

    Returns:
        None
    """
    file_path = Path("src/query.sql")
    database = "processed_db"
    
    data = run_sql_query_from_file(file_path=file_path, database=database)
    return data
