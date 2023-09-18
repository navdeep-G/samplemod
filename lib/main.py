import yaml
import os
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import openai
from dotenv import load_dotenv
import utils
import lancedb

# Load the stored environment variables
# Must be called before reading environment variables
load_dotenv()


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

        embeddingsQueries[embeddingIndex] += ' FROM ' + \
            embeddingConfig['table']
        embeddingsQueries[embeddingIndex] += ' LIMIT 5'
    return embeddingsQueries


def getScehmaFromTable(table):
    return bigqueryClient.get_table(
        table
    ).schema


def embed(text):
    # Skipping for development
    # openai.api_key = configFile['embedder']['api_key']
    api_key = os.getenv("OPENAI_API_KEY")
    utils.checkDefined('OPENAI_API_KEY', api_key)
    openai.api_key = api_key
    embedding = openai.Embedding.create(
        model="text-embedding-ada-002",
        input=text
    )

    return {
        'vector': embedding['data'][0].embedding,
        'item': prompt
    }


'''
    unique_key                              complaint_description
0  19-00491235                              Parking Machine Issue
1  20-00517188  Street Light Issue- Multiple poles/multiple st...
2  20-00289596                Community Connections - Coronavirus
3  21-00355512                              Parking Machine Issue
4  23-00046057                                  AE - Key Accounts
'''


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

    stringRows = []
    for index, row in result.iterrows():
        prompt = ''
        for columnIndex, column in enumerate(result.columns):
            prompt += column + ': ' + str(row[column]) + '\n'
        stringRows.append(prompt)

    embeds = []
    for prompt in stringRows:
        embedding = embed(prompt)
        embeds.append(embedding)

    uri = configFile['database']['uri']
    db = lancedb.connect(uri)
    table = db.open_table("user")
    # table = db.create_table("user",
    #                         data=embeds)

    searchTerm = embed('Problem with parking')
    result = table.search(searchTerm['vector']).limit(2).to_df()
