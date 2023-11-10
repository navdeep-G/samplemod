from importlib_metadata import metadata
import pandas as pd
import json
import openai
import lancedb

from pexpect import TIMEOUT
from langchain.embeddings import HuggingFaceBgeEmbeddings

import asyncio
import time

import json

import asyncio
import aiohttp
import nest_asyncio

# to be adjusted
with open('../api.key', 'r') as f:
    key = f.read().strip('\n')

openai.api_key = key

# constant - to be adjusted
VECTORDB_INDEX = 105

API_URL_LLAMA_7B = "https://api-inference.huggingface.co/models/meta-llama/Llama-2-7b-chat-hf"
HEADERS_LLAMA_7B = {"Authorization": f"Bearer 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",
                    "Content-Type": "application/json", }

API_URL_LLAMA_13B = "https://api-inference.huggingface.co/models/meta-llama/Llama-2-13b-chat-hf"
HEADERS_LLAMA_13B = {
    "Authorization": "Bearer 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",
    "Content-Type": "application/json", }

API_URL_LLAMA_30B = "https://api-inference.huggingface.co/models/meta-llama/Llama-2-70b-chat-hf"
HEADERS_LLAMA_30B = {
    "Authorization": "Bearer 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",
    "Content-Type": "application/json", }

API_URL_LLAMA_7B_2 = "https://api-inference.huggingface.co/models/meta-llama/Llama-2-7b-chat-hf"
HEADERS_LLAMA_7B_2 = {
    "Authorization": "Bearer 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",
    "Content-Type": "application/json", }

API_URL_LLAMA_13B_2 = "https://api-inference.huggingface.co/models/meta-llama/Llama-2-13b-chat-hf"
HEADERS_LLAMA_13B_2 = {
    "Authorization": "Bearer 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",
    "Content-Type": "application/json", }

API_URL_LLAMA_30B_2 = "https://api-inference.huggingface.co/models/meta-llama/Llama-2-70b-chat-hf"
HEADERS_LLAMA_30B_2 = {
    "Authorization": "Bearer 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",
    "Content-Type": "application/json", }

API_URLS = [API_URL_LLAMA_7B, API_URL_LLAMA_13B, API_URL_LLAMA_30B,
            API_URL_LLAMA_7B_2, API_URL_LLAMA_13B_2, API_URL_LLAMA_30B_2]
HEADERS = [HEADERS_LLAMA_7B, HEADERS_LLAMA_13B, HEADERS_LLAMA_30B,
           HEADERS_LLAMA_7B_2, HEADERS_LLAMA_13B_2, HEADERS_LLAMA_30B_2]

SAMPLE_SIZE = 20
BATCH_SIZE = 100
TIMEOUT_SECONDS = 2.5 * BATCH_SIZE
MODEL = "gpt-3.5-turbo"

## to be adjusted
lance_db_uri = "data/sample-lancedb"
api_selector = 0


def get_openai_template_prompt(prompt):
    response = openai.ChatCompletion.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": prompt}
        ],
        temperature=0,
    )
    response = response['choices'][0]['message']['content']

    return response


def get_openai_embedding(text, model="text-embedding-ada-002"):
    text = text.replace("\n", " ")
    return openai.Embedding.create(input=[text],
                                   model=model,
                                   )['data'][0]['embedding']


def get_baai_embedder(model_name="BAAI/bge-small-en"):
    model_kwargs = {'device': 'cpu'}
    encode_kwargs = {'normalize_embeddings': False}
    hf = HuggingFaceBgeEmbeddings(
        model_name=model_name,
        model_kwargs=model_kwargs,
        encode_kwargs=encode_kwargs
    )

    return hf


async def get_baai_embedding(text, embedder):
    try:
        embedded_query = embedder.embed_query(text)
    except Exception as e:
        print("failed to embed query with error: " + e)
        return None

    return embedded_query


async def send_requests_async(session, url, headers, requests):
    async with session.post(url, headers=headers, data=requests, timeout=TIMEOUT_SECONDS) as response:
        return await response.text()


async def main(llm_requests):
    responses = []

    try:
        async with aiohttp.ClientSession() as session:
            global api_selector
            api_selector %= len(API_URLS)
            print("API selector: " + str(api_selector))
            tasks = [send_requests_async(
                session, API_URLS[api_selector], HEADERS[api_selector], request) for request in llm_requests]
            responses = await asyncio.gather(*tasks)
            api_selector += 1
    except asyncio.TimeoutError:
        print("Timeout error. Completing processing...")

    return responses


def read_and_process_input_tables():

    # raw interactions - path to be adjusted
    raw_interactions_df = pd.read_csv('../jupyter_notebooks/data/food/raw_interactions.csv',
                                      encoding='latin-1')

    raw_interactions_columns = ['user_id',
                                'recipe_id',
                                'date',
                                'rating',
                                'review']
    raw_interactions_descriptions = ['uniquer user id',
                                     'unique recipe id',
                                     'date',
                                     'rating of the user towards the recipe',
                                     'review of the user towards the recipe'
                                     ]

    # raw recipe information - path to be adjusted
    raw_recipes_df = pd.read_csv('../jupyter_notebooks/data/food/raw_recipes.csv',
                                 encoding='latin-1')
    raw_recipes_columns = ['name',
                           'id',
                           'contributor_id',
                           'submitted',
                           'tags',
                           'nutrition',
                           'n_steps',
                           'steps',
                           'description',
                           'ingredients',
                           'n_ingredients'
                           ]
    raw_recipes_descriptions = ['Recipe name',
                                'Recipe id',
                                'Minutes to prepare recipe',
                                'User ID who submitted this recipe',
                                'Date recipe was submitted',
                                'Food.com tags for recipe',
                                'Nutrition information (calories (#), total fat (PDV), sugar (PDV) , sodium (PDV) , protein (PDV) , saturated fat',
                                'Number of steps in recipe',
                                'Text for recipe steps, in order',
                                'User-provided description'
                                ]

    interactions_df = raw_interactions_df[raw_interactions_df.date <=
                                          '2012-01-01'][raw_interactions_columns]
    recipes_df = raw_recipes_df[raw_recipes_df.submitted <=
                                '2012-01-01'][raw_recipes_columns]

    interactions_columns_string = ", ".join(raw_interactions_columns)
    recipes_columns_string = ", ".join(raw_recipes_columns)

    interactions_descriptions_string = ", ".join(raw_interactions_descriptions)
    recipes_descriptions_string = ", ".join(raw_recipes_descriptions)

    count_df = interactions_df.groupby(
        by=['user_id'])['recipe_id'].count().reset_index(name='count_reviews')

    users_10_reviews = count_df[(count_df.count_reviews >= 5) & (
        count_df.count_reviews <= 15)]['user_id'].sample(2500).unique()
    interactions_10_df = interactions_df[interactions_df.user_id.isin(
        users_10_reviews)]
    avg_score = interactions_10_df.groupby(
        by=['user_id'])['rating'].mean().reset_index(name='avg_rating')
    filtered_user_ids = avg_score[avg_score.avg_rating <=
                                  4.8]['user_id'].unique()

    interactions_df = interactions_10_df[interactions_10_df.user_id.isin(
        filtered_user_ids)]
    recipes = interactions_df[interactions_df.user_id.isin(
        filtered_user_ids)]['recipe_id'].unique()
    recipes_df = recipes_df[raw_recipes_df.id.isin(recipes)]

    return recipes_df, interactions_df, recipes_descriptions_string, interactions_descriptions_string, recipes_columns_string, interactions_columns_string


def get_prompt(columns_string, descriptions_string):
    prompt = """ You are an eloquent database expert and analyst. You can very well describe SQL table rows in natural language """
    prompt += """. I'll now provide you with some data for a row your task is to generate a poetic and accurate description in a paragraph form.
                        Do not acknowledge my request with 'sure' or in any other way besides going straight to the answer and I want you to include information from all columns in your description. Don't use bullet points!
                        """

    role_message = prompt + \
        "The columns of the table are the following: " + columns_string
    role_message += ". And the description for each column is the following: " + \
        descriptions_string

    return role_message


def get_template_promt(columns_string):
    prompt = """Your task is to generate a template description string that describes a row with the following column names: """ \
        + columns_string \
        + ". The template should include placeholders for each column's value, but only for columns (not descriptions) " \
        + " and I will replace these placeholders with real data from each row later. " \
        "Please ensure that the placeholders are appropriately formatted for data insertion. For example, you could start " \
        "with something like: This row describes a recipe with id {id} that was contributed by the user with id {contributor_id}... I want you to be detailed in your description and imagine you're generating a description for a row. "

    return prompt


def stringify(df, role_message):
    print("stringifying...")
    df['stringify'] = df.apply(
        lambda row: role_message + row.astype(str).str.cat(sep=' '), axis=1)

    return


def create_llm_requests(df, ids_string):
    llm_requests = []
    for _, row_data in df.iterrows():
        json_body = {
            "inputs": f"[INST] {row_data['stringify']} [/INST] {ids_string}:: This row describes",
            "parameters": {"max_new_tokens": 256,
                           "top_p": 0.9,
                           "temperature": 0.7}
        }
        data = json.dumps(json_body)
        llm_requests.append(data)


def text2embed_recipes(df, all_responses, hf):
    responses_data = [response[0].split('[/INST] ')[1]
                      for response in all_responses
                      if response and len(response[0].split('[/INST] ')) > 1]

    full_responses = []
    for response in responses_data:
        metadata_dict = {}
        row_id = response.split("::")[0]
        try:
            row_id = float(int(row_id))
        except:
            print("Failed to convert ids to ints")
            continue
        row_data = df[df.id == row_id]
        metadata_dict['recipe_id'] = row_id
        metadata_dict['contributor_id'] = row_data.iloc[0]['contributor_id']
        metadata_dict['text'] = response.split("::")[1].strip()
        metadata_dict['recipe_name'] = row_data.iloc[0]['name']
        metadata_dict['submitted'] = row_data.iloc[0]['submitted']
        metadata_dict['embedder'] = "baai"
        metadata_dict['llm'] = "llama2"

        metadata_dict['vector'] = get_baai_embedding(
            response.split("::")[1], hf)

        full_responses.append(metadata_dict)

    return full_responses


def text2embed_interactions(df, all_responses, hf):
    responses_data = [response[0].split('[/INST] ')[1]
                      for response in all_responses
                      if response and len(response[0].split('[/INST] ')) > 1]

    full_responses = []
    for response in responses_data:
        metadata_dict = {}
        user_id = response.split("::")[0]
        recipe_id = response.split("::")[1]
        try:
            user_id = float(int(user_id))
            recipe_id = float(int(recipe_id))
        except:
            print("Failed to convert ids to ints: ", user_id, recipe_id)
            continue

        row_data = df[(df.user_id == user_id) & (df.recipe_id == recipe_id)]
        metadata_dict['recipe_id'] = recipe_id
        metadata_dict['user_id'] = user_id
        metadata_dict['text'] = response.split("::")[2].strip()
        metadata_dict['rating'] = row_data.iloc[0]['rating']
        metadata_dict['embedder'] = "baai"
        metadata_dict['llm'] = "llama2"

        metadata_dict['vector'] = get_baai_embedding(
            response.split("::")[1], hf)

        full_responses.append(metadata_dict)

    return full_responses


def text2embed_template(df, hf, cols, textify_col):
    full_responses = []
    for _, row in df.iterrows():
        metadata_dict = {}
        metadata_dict['text'] = row[textify_col]
        metadata_dict['vector'] = get_baai_embedding(row[textify_col], hf)
        metadata_dict['embedder'] = "baai"
        metadata_dict['llm'] = "llama2"
        for col in cols:
            metadata_dict[col] = row[col]

        full_responses.append(metadata_dict)

    return full_responses


async def text2embed_async(df, hf, cols, textify_col):
    batch_responses = []

    async def process_row(row):
        metadata_dict = {}
        metadata_dict['text'] = row[textify_col]
        metadata_dict['vector'] = await get_baai_embedding(row[textify_col], hf)
        metadata_dict['embedder'] = "baai"
        metadata_dict['llm'] = "llama2"
        for col in cols:
            metadata_dict[col] = row[col]
        return metadata_dict

    tasks = [process_row(row) for _, row in df.iterrows()]
    batch_responses = await asyncio.gather(*tasks)

    return batch_responses


def save_to_vector_db(lance_db_uri, table_name, full_responses):
    db = lancedb.connect(lance_db_uri)
    _ = db.create_table(
        name=table_name + str(VECTORDB_INDEX),
        data=full_responses
    )

    return table_name + str(VECTORDB_INDEX)


def predict():
    # initialize embedder
    hf = get_baai_embedder()

    print('Reading input..')
    recipes_df, interactions_df, _, _, recipes_columns_string, interactions_columns_string = read_and_process_input_tables()

    dfs = [recipes_df, interactions_df]
    dfs_columns = [recipes_columns_string, interactions_columns_string]
    dfs_names = ['recipes', 'interactions']

    print('Generating template prompts...')
    template_descriptions = {}
    for index, df in enumerate(dfs):
        template_prompt = get_template_promt(
            dfs_columns[index])
        template_descriptions[dfs_names[index]] = get_openai_template_prompt(
            template_prompt)

    print('Textifying...')
    for df_index, df in enumerate(dfs):
        df['textify'] = ' '
        df.reset_index(inplace=True, drop=True)
        for index, row in df.iterrows():
            df.at[index, 'textify'] = template_descriptions[dfs_names[df_index]].format(
                **row)

    print('Text2Embed...')
    responses = []
    nest_asyncio.apply()
    loop = asyncio.get_event_loop()

    # text2embed recipes table
    all_embeddings_recipes, all_embeddings_interactions = [], []

    print("text2embed recipes_df..")
    start_time = time.time()
    num_batches = len(recipes_df)//BATCH_SIZE
    for i in range(len(recipes_df)//BATCH_SIZE):
        responses = loop.run_until_complete(
            text2embed_async(recipes_df[i*BATCH_SIZE: i*BATCH_SIZE+BATCH_SIZE], hf, ['name', 'id', 'contributor_id', 'name', 'submitted'], 'textify'))
        print("--- Batch %s of %s: %s seconds ---" %
              (i, num_batches, time.time() - start_time))
        all_embeddings_recipes.append(responses)

    all_embeddings_recipes = [
        item for sublist in all_embeddings_recipes for item in sublist]
    # text2embed interactions table
    print("text2embed interactions_df..")
    start_time = time.time()
    num_batches = len(interactions_df)//BATCH_SIZE
    for i in range(len(interactions_df)//BATCH_SIZE):
        responses = loop.run_until_complete(
            text2embed_async(interactions_df[i*BATCH_SIZE: i*BATCH_SIZE+BATCH_SIZE], hf, ['recipe_id', 'user_id', 'date', 'rating'], 'textify'))
        print("--- Batch %s of %s: %s seconds ---" %
              (i, num_batches, time.time() - start_time))
        all_embeddings_interactions.append(responses)

    all_embeddings_interactions = [
        item for sublist in all_embeddings_interactions for item in sublist]

    print('Saving to vectorDB...')
    vdb_name = save_to_vector_db(
        lance_db_uri, 'recipes-data-points-', all_embeddings_recipes)
    print(vdb_name)
    vdb_name = save_to_vector_db(
        lance_db_uri, 'interactions-data-points-', all_embeddings_interactions)
    print(vdb_name)
