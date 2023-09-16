import yaml
import os
from google.cloud import bigquery
from google.oauth2 import service_account
import json


BIG_QUERY_PROJECT_ID = 'tranquil-tiger-399222'

def read_yaml_file(filename):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, filename)
    with open(file_path, 'r') as file:
        data = yaml.safe_load(file)
    return data

def read_json_file(filename):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, filename)
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data

if __name__ == "__main__":
    configFile = read_yaml_file('./config.yml')
    bigQueryCredentialsFile = read_json_file('./keys.json')

    credentials = service_account.Credentials.from_service_account_info(
        bigQueryCredentialsFile
    )
    bigQueryClient = bigquery.Client(
        project=BIG_QUERY_PROJECT_ID,
        credentials=credentials
    )
    result = bigQueryClient.query(
        query='SELECT * FROM `bigquery-public-data.austin_311.311_service_requests` LIMIT 10'
    ).to_dataframe()
    print(result)
