import io
import os
from typing import Optional, Tuple

import boto3
import mysql.connector
import pandas as pd
from botocore.client import BaseClient
from botocore.exceptions import BotoCoreError, ClientError
from mysql.connector import Error, MySQLConnection
from mysql.connector.cursor import MySQLCursor as Cursor


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
        print(f"Connected to MySQL Successfully")

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
        mycursor.execute(f"USE {database}")
        sql = f"DROP TABLE IF EXISTS {table_name}"
        mycursor.execute(sql)
        print(f"Old Table '{table_name}' dropped before creation.")

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

def auth_aws(aws_access_key: str, 
             aws_secret_key: str, 
             region: Optional[str]='us-east-2') -> BaseClient:
    """
    Authenticates and returns an S3 client using the provided AWS credentials and region.

    Parameters:
        aws_access_key (str): AWS access key.
        aws_secret_key (str): AWS secret key.
        region (Optional[str]): AWS region (optional).

    Returns:
        BaseClient: An authenticated Boto3 S3 client.

    Raises:
        BotoCoreError, ClientError: If authentication or connection fails.
    """
    try:
        s3_client = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name=region)
        return s3_client
    except (BotoCoreError, ClientError, TypeError) as e:
        print(e)
    

def read_file_s3(s3_client: BaseClient, 
                 bucket: str, 
                 object_name: str) -> pd.DataFrame:
    """
    Reads a file from S3 and loads it into a pandas DataFrame.

    Parameters:
        s3_client (BaseClient): The S3 client.
        bucket (str): The S3 bucket name.
        file_name (str): The name of the file in the bucket.

    Returns:
        pd.DataFrame: Data loaded from the file in the specified bucket.
    
    Raises:
        ClientError: If there is an issue with the AWS request (e.g., file not found).
        Exception: For any other unforeseen errors during the file reading or DataFrame creation.
    """
    
    try:
        s3_object = s3_client.get_object(Bucket=bucket, Key=object_name)
        body = s3_object['Body'].read()
        data = io.BytesIO(body)
        df = pd.read_csv(data)
    except ClientError as e:
        print(e)
    return df

def write_file_s3(s3_client: BaseClient, 
                  file: str, 
                  bucket: str, 
                  object_name: Optional[str]=None) -> None:
    """
    Uploads a file to an S3 bucket.

    Parameters:
        s3_client (BaseClient): The S3 client used to interact with AWS S3.
        file (str): The local file path to be uploaded.
        bucket (str): The S3 bucket name where the file will be uploaded.
        object_name (str): The name of the object in S3 (the file name).

    Returns:
        None: This function does not return anything. It only uploads the file to S3.

    Raises:
        ClientError: If there is an issue with the S3 request (e.g., invalid bucket or permission error).
    """
    if object_name is None:
        object_name = os.path.basename(file)
        
    try:
        with open(file, "rb") as file_content:
            s3_client.put_object(Bucket=bucket, Key=object_name, Body=file_content)
        print("File uploaded Successfully")
    except ClientError as e:
        print(e)