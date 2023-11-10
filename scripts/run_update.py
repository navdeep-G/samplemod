from importlib_metadata import metadata
import pandas as pd
import numpy as np
import lancedb
import random

# to be adjusted
VECTORDB_INDEX = 105


def save_to_vector_db(lance_db_uri, table_name, full_responses):
    db = lancedb.connect(lance_db_uri)
    _ = db.create_table(
        name=table_name + str(VECTORDB_INDEX),
        data=full_responses
    )

    return table_name + str(VECTORDB_INDEX)


def update():
    lance_db_uri = "./data/sample-lancedb"
    db = lancedb.connect(lance_db_uri)

    recipes_vectordb_table = db.open_table(
        'recipes-data-points-' + str(VECTORDB_INDEX))
    interactions_vectordb_table = db.open_table(
        'interactions-data-points-' + str(VECTORDB_INDEX))

    recipes_vector_df = recipes_vectordb_table.to_pandas()
    interactions_vector_df = interactions_vectordb_table.to_pandas()

    recipes_groupped = recipes_vector_df.groupby(by=['embedder', 'llm', 'id'])[
        'vector'].mean().reset_index(name='vector').rename(columns={"embedder": "embedder", "llm": "llm", "id": "recipe_id"})

    # interactions table has two entities: user_id and recipe_id - group by both and save to vector DBs
    interaction_users_groupped = interactions_vector_df.groupby(by=['embedder', 'llm', 'user_id'])[
        'vector'].mean().reset_index(name='vector')

    interactions_recipes_groupped = interactions_vector_df.groupby(by=['embedder', 'llm', 'recipe_id'])[
        'vector'].mean().reset_index(name='vector')

    recipes_concat_groupped = pd.concat(
        [recipes_groupped, interactions_recipes_groupped], axis=0).groupby(by=['embedder', 'llm', 'recipe_id'])[
        'vector'].mean().reset_index(name='vector')

    # store in the two vectorDBs
    print('Saving to vectorDB...')
    vdb_name = save_to_vector_db(
        lance_db_uri, 'recipes-unique-', recipes_concat_groupped)
    print(vdb_name)
    vdb_name = save_to_vector_db(
        lance_db_uri, 'interactions-unique-', interaction_users_groupped)
    print(vdb_name)
