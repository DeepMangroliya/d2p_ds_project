#!/bin/bash

# load data from csv files into a database
echo "Creating raw database......."
python3 src/tools/database_final.py -cd True -nd "raw_sales"

# normalize and clean data, and upload to database
echo "Creating table & uploading data......."
python3 src/tools/database_final.py -nd "raw_sales" -id upload-to-database

echo "Running ETL......."
python3 src/tools/etl_script_final.py

echo "Creating processed database......."
python3 src/tools/database_final.py -cd True -nd "processed_sales"

echo "Creating table & uploading data......."
python3 src/tools/database_final.py -nd "processed_sales" -id cleaned-upload-to-database

# train and evaluate machine learning models
echo "Extracting data & uploading to S3......."
python3 main.py -t data_analysis_ext

echo "Running modelling & uploading to gsheet......."
python3 main.py -t modeling
