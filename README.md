# My D2P Project

## Description

I will add the description here

## Prerequisites:
- Python 3.8 or higher.
- MySQL Workbench for managing SQL queries.

## How to run this Project:

### 1. Clone the GitHub repository:

```
git clone -b "the github repository link"
cd project_directory_path
```

### 2. Create a Python virtual environment (using Conda):

**Note: You can also create it with *"venv"*.**

```
conda create "your env name"
conda activate env
```

### 3. Install dependencies:

```
pip install -r requirements.txt
```

### 4. Set up environment variables:

Configure the following environment variables for AWS, MySQL. Replace placeholder values with your actual credentials.

**Note: Store them in a ***.env*** file: and make sure your code loads them using *"python-dotenv"* library**

 - MySQL Database Configuration:

```
#for SQL Connection
export user="your_mysql_user"
export passworkd="your_mysql_password"
export host="your_db_host"
```

- AWS Configuration:

```
#for AWS Connection
export ACCESS_KEY="your_aws_access_key"
export SECRET_KEY="your_aws_secret_key"
```

### 5. Start MySQL server:

- Open MySQL Workbench or ensure your MySQL service is running.

### 6. Run the main scsript:

Use the following command depending on your use case:

- Create a new database and insert data from CSV:

```
python test.py -dbe False -db your_db -c path/to/file.csv -t your_table
```

- Use an existing database (no CSV needed):

```
python test.py -dbe True -db your_db
```

**Notes:**
- -db and -dbe are required.

- -c and -t are only needed when creating a new DB and inserting data.

Ensure .env has correct DB credentials.

### 7. Check for output:

- If you're creating tables or inserting data, check your MySQL Workbench.

- If you're processing data, check any log/output folders or the terminal for success messages.