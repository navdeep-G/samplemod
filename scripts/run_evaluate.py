from importlib_metadata import metadata
import pandas as pd
import numpy as np
import json
import lancedb


VECTORDB_INDEX = 105


def weird_division(n, d):
    return n / d if d else 0


def evaluate():
    lance_db_uri = "./data/sample-lancedb"
    db = lancedb.connect(lance_db_uri)

    recipes_vectordb_table = db.open_table(
        'recipes-unique-' + str(VECTORDB_INDEX))
    interactions_vectordb_table = db.open_table(
        'interactions-unique-' + str(VECTORDB_INDEX))
    interactions_vectordb_df = interactions_vectordb_table.to_pandas()

    raw_interactions_df = pd.read_csv('../jupyter_notebooks/data/food/raw_interactions.csv',
                                      encoding='latin-1')
    raw_recipes_df = pd.read_csv('../jupyter_notebooks/data/food/raw_recipes.csv',
                                 encoding='latin-1')

    raw_interactions_columns = ['user_id',
                                'recipe_id',
                                'date',
                                'rating',
                                'review']
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

    interactions_df = raw_interactions_df[raw_interactions_df.date >
                                          '2012-01-01'][raw_interactions_columns]

    recipes_df = raw_recipes_df[raw_recipes_df.submitted >
                                '2012-01-01'][raw_recipes_columns]

    count_df = raw_interactions_df.groupby(
        by=['user_id'])['recipe_id'].count().reset_index(name='count_reviews')

    users_10_reviews = count_df[(count_df.count_reviews >= 5) & (
        count_df.count_reviews <= 15)]['user_id'].unique()
    interactions_10_df = raw_interactions_df[raw_interactions_df.user_id.isin(
        users_10_reviews)]
    avg_score = interactions_10_df.groupby(
        by=['user_id'])['rating'].mean().reset_index(name='avg_rating')
    filtered_user_ids = avg_score[avg_score.avg_rating <=
                                  4.8]['user_id'].unique()

    interactions_df = interactions_10_df[interactions_10_df.user_id.isin(
        filtered_user_ids)]

    interactions_vectordb_df = interactions_vectordb_table.to_pandas()
    recipes_vectordb_df = recipes_vectordb_table.to_pandas()
    recipes_df['recipe_id'] = recipes_df['id']

    interactions_df = interactions_df.merge(interactions_vectordb_df, on='user_id').merge(
        recipes_vectordb_df[['recipe_id']], on='recipe_id')
    interactions_df['vector'] = interactions_df['vector'].apply(tuple)

    user_clicked_recipes = interactions_df.groupby(
        by=['user_id', 'vector'])['recipe_id'].apply(list).reset_index(name='clicked_recipes')
    user_clicked_recipes['recipes_count'] = user_clicked_recipes.apply(
        lambda row: len(row['clicked_recipes']), axis=1)

    num_correct_predictions = 0
    map_metrics = [10, 20, 30, 40, 50, 100, 1000]

    for map_metric in map_metrics:
        for index, row in user_clicked_recipes.iterrows():
            recipes_preds = recipes_vectordb_table.search(
                list(row['vector'])).limit(map_metric).to_df()['recipe_id'].tolist()

            correct_preds = len(set(recipes_preds).intersection(
                set(row['clicked_recipes'])))

            num_correct_predictions += correct_preds

        print("MAP@" + str(map_metric) + " Score: ", str(weird_division(num_correct_predictions,
                                                                        user_clicked_recipes['recipes_count'].sum())))
