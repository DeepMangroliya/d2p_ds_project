import argparse
import os

from dotenv import load_dotenv

from utils import (create_database, create_table, db_connection,
                   formatting_columns_placeholders, get_data, insert_data, run_sql_query_from_file)

#utilizing argparse module to push the arguments dynamically
parser = argparse.ArgumentParser(description="Accessing Database test1 for D2P Project") #creating parser
parser.add_argument('-dbn','--database_new', default=False, type=bool, help='Use existing database or create a new database') #optional
parser.add_argument('-db','--database_name', required=True, type=str, help='Name of the database') #required
parser.add_argument('-c', '--csv_file', type=str, help='Path to csv file') #optional
parser.add_argument('-t', '--table_name', type=str, help='table name') #optional
parser.add_argument('-fp', '--file_path', type=str, help='sql file path for query')
args = parser.parse_args()

# load credentials
load_dotenv(".env")

# connect to mysql server
con, mycursor = db_connection(host=os.getenv("HOST"), 
                              user="root", 
                              password=os.getenv("PASSWORD"))

if args.database_new:
    create_database(mycursor=mycursor, database=args.database_name)
else:
    df = get_data(csv_file=args.csv_file)
    schema, placeholder_str = formatting_columns_placeholders(df=df)
    create_table(mycursor=mycursor, database=args.database_name, table_name=args.table_name, schema=schema)
    insert_data(con=con, mycursor=mycursor, table_name=args.table_name, df=df)

df = run_sql_query_from_file(con=con, mycursor=mycursor, file_path=args.file_path)
print(df)