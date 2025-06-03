import argparse
import yaml
from src.utils import write_file_s3, process_task, gcp_feed_data

args = argparse.ArgumentParser(
    description="Provies some inforamtion on the job to process"
)
args.add_argument(
    "-t", "--task", type=str, required=True,
    help="This will point to a task location into the config.yaml file.\
        Then it will follow the step of this specific task.")
args = args.parse_args()

with open("./config/config.yaml", 'r') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

config_export = config[args.task]["export"]

if config_export[0]["export"]["host"] == 's3':
    write_file_s3(process_task(args.task), 
                  config_export[0]["export"]["bucket_name"], 
                  config_export[0]["export"]["object_name"])
elif config_export[0]["export"]["host"] == 'gsheet':
    gcp_feed_data(config_export[0]["export"]["spread_sheet_id"],
                  config_export[0]["export"]["worksheet_name"],
                  process_task(args.task))
