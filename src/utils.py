import io
import os
from pathlib import Path
from typing import Optional, Tuple

import boto3
import gspread
import mysql.connector
import pandas as pd
from botocore.client import BaseClient
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
from mysql.connector import Error, MySQLConnection
from mysql.connector.cursor import MySQLCursor as Cursor

load_dotenv(Path('.env'))

# SQL Database Functions
def db_connection(host: str,
                  user: str,
                  password: str,
                  database: Optional[str] = None
                 ) -> Tuple[MySQLConnection, str]:
    """
    Connects to mysql server.re

    Args:
        host (str): MySQL Server host
        user (str): MySQL Server username
        password (str): MySQL Server password
        database (optional[str], Default=None): Database name (Default: None)

    Returns:
        tuple (MySQLConnection [Optional], cursor [Optional]): 
        - Connection object and Cursor obhect if successful, otherwise None

    Raises:
        Connection Error: Connection Unsuccessful
    """
    try:
        #connecting to mysql server
        con = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        mycursor = con.cursor()
        print("Connected to MySQL Successfully")

    except Error as e:
        print(f"Cannot connect to MySQL Server: {e}")
    return con, mycursor


def create_database(mycursor: Cursor,
             database: str
            ) -> Optional[Error]:
    """_summary_

    Args:
        mycursor (Cursor): mysql cursor to make changed into database
        database (str): the database that is being modified
    
    Returns:
        - None
    
    Raises: 
        -  Error if Database creation fails, otherwise none
    """
    #drop db if already exists and create a new one.
    mycursor.execute(f"DROP DATABASE IF EXISTS {database}")
    mycursor.execute(f"CREATE DATABASE {database}")
    mycursor.execute("SHOW DATABASES")
    dbs = mycursor.fetchall()
    dbs = [db[0] for db in dbs]
    if database in dbs:
        print(f"Database: '{database}' created successfully")
    else:
        print(f"Failed to create Database: '{database}'")
      

def create_table(mycursor: Cursor, database: str, table_name: str, schema: str) -> None:
    """Creates Table

    Args:
        mycursor (Cursor): MySQL cursor. 
        table_name (str): table name
        schema (Tuple): defines the columns of the table and its data types

    Returns:
        - None

    Raises:
        - Error if Table creation fails, otherwise None 

    """
    try:
        #drop table if exists
        mycursor.execute(f"USE {database}")
        sql = f"DROP TABLE IF EXISTS {table_name}"
        mycursor.execute(sql)
        print(f"Old Table '{table_name}' dropped before creation.")
        
        #create a new one
        sql = f"CREATE TABLE {table_name} ({schema})"
        mycursor.execute(sql)
        print(f"Table '{table_name}' created successfully.")
    except Error as e:
        print(f"Error creating table: {e}")


def get_data(csv_file: str) -> Optional[pd.DataFrame]:
    """
    Reads data from a CSV file using pandas and inserts it into the specified MySQL table.

    Args:
        csv_file (str): Path to the CSV file.

    Returns:
        pd.DataFrame: Dataframe containing the CSV data. [Optional]

    Raises:
        FileNotFoundError: If the file is not found.
    """
    try:
        #read data from csv, extarct columns and values separately
        df = pd.read_csv(csv_file).fillna(0)
        # Insert data into MySQL table using insert_data function
        # Drop 'Unnamed:0' column if it exists, ignoring errors if not present
        df.drop(['Unnamed: 0'], axis=1, inplace=True, errors='ignore')
    except FileNotFoundError:
        print(f"File not found: {csv_file}")
    return df


def formatting_columns_placeholders(df: pd.DataFrame) -> Tuple[str, str]:
    """
    Generates SQL schema and placeholders based on DataFrame columns.

    Args:
        df (pd.DataFrame): Pandas DataFrame containing the dataset.

    Returns:
        Tuple[str, str]: SQL schema and value placeholders.
    """
    sql_cols = []
    placeholders = []

    #changing python types to sql types
    for col in df.columns:
        if df[col].dtype == 'int64':
            data_type = 'INT'
        elif df[col].dtype == 'float64':
            data_type = 'FLOAT'
        elif df[col].dtype == 'bool':
            data_type = 'BOOLEAN'
        else:
            data_type = 'VARCHAR(255)'
        sql_cols.append(f"{col} {data_type}")
        placeholders.append("%s")

    #converting the list as strings
    schema = ", ".join(sql_cols)
    placeholder_str = f"({', '.join(placeholders)})"

    return schema, placeholder_str
            

def insert_data(con: MySQLConnection, 
                mycursor: Cursor,
                table_name: str,
                df: pd.DataFrame
                ) -> Optional[int]:
    """
    Inserts data into the specified table for specific columns.

    Args:
        con (MySQLConnection): Connection to the MySQL database.
        mycursor (Cursor): MySQL cursor.
        table_name (str): Name of the table.
        df (pd.DataFrame): Pandas DataFrame containing the data.

    Returns:
        int: Number of rows successfully inserted
       
    Raises:
        Error: If insertion fails, otherwise None
    """
    total=0
    schema, placeholders = formatting_columns_placeholders(df)
    cols = ", ".join(df.columns) #deriving column names in the specified df
    sql_query = f"INSERT INTO {table_name} ({cols}) Values {placeholders}"
    
    for _, row in df.iterrows():
        values = tuple(row) #one row data at a time 
        try:
            mycursor.execute(sql_query, values)
            if mycursor.rowcount == 1: #if the number of row inserted is 1 
                total+=1
            con.commit()
        except Error as e:
            print(e)
    return total
        
#Amazon Web Services (AWS)

def auth_aws() -> BaseClient:
    """
    Authenticates and returns an S3 client using the provided AWS credentials and region.

    Returns:
        BaseClient: An authenticated Boto3 S3 client.

    Raises:
        BotoCoreError, ClientError: If authentication or connection fails.
    """
    try:
        s3_client = boto3.client('s3', aws_access_key_id=os.getenv('ACCESS_KEY'),
                                 aws_secret_access_key=os.getenv('SECRET_KEY'),
                                 region_name='us-east-2')
        return s3_client
    except (BotoCoreError, ClientError, TypeError) as e:
        print(e)
    

def read_file_s3(bucket: str, 
                 object_name: str) -> pd.DataFrame:
    """
    Reads a file from S3 and loads it into a pandas DataFrame.

    Parameters:
        bucket (str): The S3 bucket name.
        file_name (str): The name of the file in the bucket.

    Returns:
        pd.DataFrame: Data loaded from the file in the specified bucket.
    
    Raises:
        ClientError: If there is an issue with the AWS request (e.g., file not found).
        Exception: For any other unforeseen errors during the file reading or DataFrame creation.
    """
    
    try:
        s3_client = auth_aws()
        s3_object = s3_client.get_object(Bucket=bucket, Key=object_name)
        df = pd.read_csv(s3_object['Body'])
        print(df)
    except ClientError as e:
        print(e)
    return df

def write_file_s3(df: pd.DataFrame, 
                  bucket: str, 
                  object_name: str) -> None:
    """
    Uploads a pandas DataFrame as a CSV file to a specified S3 bucket.

    Args:
        df (pd.DataFrame): The DataFrame to upload.
        bucket (str): The name of the S3 bucket.
        object_name (str): The key (file name) for the object in the S3 bucket.

    Returns:
        None

    Raises:
        ValueError: If `object_name` is not provided.
        ClientError: If there is an issue with the S3 request (e.g., invalid bucket, permissions issue).
    """

    if object_name is None:
        raise ValueError("You must provide an object_name when uploading a DataFrame.")
    
    try:
        s3_client = auth_aws()
        csv = df.to_csv(index = False)
        s3_client.put_object(Bucket=bucket, Key=object_name, Body=csv)
        print("File uploaded Successfully")
    except ClientError as e:
        print(e)
        
def gcp_authentication() -> Credentials:
    """
    Authenticates with Google Cloud Platform using a service account and environment variables.

    Environment Variables Required:
        - PRIVATE_KEY_ID
        - PRIVATE_KEY
        - CLIENT_EMAIL
        - CLIENT_ID
        - CLIENT_X509_CERT_URL

    Returns:
        Credentials: A Google OAuth2 credentials object used for accessing Google Sheets and Drive APIs.
    """
    
    SCOPES = [
        "https://spreadsheets.google.com/feeds",
        'https://www.googleapis.com/auth/spreadsheets',
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive"
    ]
     
    type = "service_account"
    project_id = "potent-symbol-456616-g9"
    private_key_id = os.getenv("PRIVATE_KEY_ID")
    private_key = os.getenv("PRIVATE_KEY").replace('\\n', '\n')
    client_email = os.getenv("CLIENT_EMAIL")
    client_id = os.getenv("CLIENT_ID")
    auth_uri = "https://accounts.google.com/o/oauth2/auth"
    token_uri = "https://oauth2.googleapis.com/token"
    auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
    client_x509_cert_url = os.getenv("CLIENT_X509_CERT_URL")
    
    credentials = Credentials.from_service_account_info({
        "type": type,
        "project_id": project_id,
        "private_key_id": private_key_id,
        "private_key": private_key,
        "client_email": client_email,
        "client_id": client_id,
        "client_x509_cert_url": client_x509_cert_url,
        "token_uri": token_uri,
        "auth_uri": auth_uri,
        "auth_provider_x509_cert_url": auth_provider_x509_cert_url,
        },
       scopes=SCOPES
    )
    
    return credentials
    
def gcp_feed_data(spreadsheet_id: str, 
                  worksheet_name: str, 
                  df: pd.DataFrame) -> None:
    
    """
    Uploads a pandas DataFrame to a specified Google Sheets worksheet.

    If the worksheet does not exist, it creates a new one.
    Existing data in the worksheet is cleared before uploading the new DataFrame.

    Args:
        spreadsheet_id (str): The ID of the target Google Spreadsheet.
        worksheet_name (str): The name of the worksheet to write data into.
        df (pd.DataFrame): The DataFrame containing data to upload.

    Returns:
        None

    Raises:
        gspread.exceptions.WorksheetNotFound: If the worksheet doesn't exist (handled by creating a new one).
        gspread.exceptions.SpreadsheetNotFound: If the spreadsheet ID is invalid or inaccessible.
    """
    
    creds = gcp_authentication()
    client = gspread.authorize(creds)
    
    # Open the spreadsheet
    try:
        sheet = client.open_by_key(spreadsheet_id).worksheet(worksheet_name)
    except gspread.exceptions.WorksheetNotFound:
        sheet = client.open_by_key(spreadsheet_id).add_worksheet(worksheet_name,1,1)
    except gspread.SpreadsheetNotFound:
        print(f"Spreadsheet '{spreadsheet_id}' not found.")
    
    # Clear existing data
    sheet.clear()
    
    df=df.astype(str)
    
    cell_list = sheet.update([df.columns.values.tolist()] + df.values.tolist())
    
    if cell_list:
        return True
    else:
        return False