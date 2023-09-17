import yaml
import os
from google.cloud import bigquery
from google.oauth2 import service_account
import json



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

def embeddingsConfigToQuery(embeddingsConfig):
    embeddingsQueries = []
    for embeddingIndex, embeddingConfig in enumerate(embeddingsConfig):
        for columnIndex, column in enumerate(embeddingConfig['columns']):
            if len(embeddingsQueries) < embeddingIndex + 1:
                embeddingsQueries.append('SELECT ')
            embeddingsQueries[embeddingIndex] += column['name']
            if columnIndex < len(embeddingConfig['columns']) - 1:
                embeddingsQueries[embeddingIndex] += ' , '

        embeddingsQueries[embeddingIndex] += ' FROM ' + embeddingConfig['table']
        embeddingsQueries[embeddingIndex] += ' LIMIT 10'
    return embeddingsQueries

def getScehmaFromTable(table):
    return bigqueryClient.get_table(
        table
    ).schema


if __name__ == "__main__":
    configFile = read_yaml_file('./config.yml')
    bigQueryCredentialsFile = read_json_file('./keys.json')

    embeddingsQuery = embeddingsConfigToQuery(configFile['embeddings'])
    print(embeddingsQuery)

    bigqueryCredentials = service_account.Credentials.from_service_account_info(
        bigQueryCredentialsFile
    )
    bigqueryClient = bigquery.Client(
        project=bigQueryCredentialsFile['project_id'],
        credentials=bigqueryCredentials
    )


    result = bigqueryClient.query(
        query=embeddingsQuery[0]
    ).to_dataframe()
    print(result)
