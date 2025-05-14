import os

from dotenv import load_dotenv

from src.utils import auth_aws, read_file_s3, write_file_s3

# load credentials
load_dotenv('.env')

#Connecting to AWS Servers
client = auth_aws(aws_access_key=os.getenv("access_key"), 
                  aws_secret_key=os.getenv("secret_key"), 
                  region=None)

#Upload a file to specified s3 bucket and giving it a object name
write_file_s3(s3_client=client, file="data/inventory_data.csv", 
              bucket="d2p.testing.bucket", 
              object_name="inventory.csv")

#Read the data of the specified file in a particular bucket
df = read_file_s3(s3_client=client, 
                  bucket="d2p.testing.bucket", 
                  object_name="inventory.csv")