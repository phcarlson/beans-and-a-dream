
import random
import numpy as np 
import pandas as pd
import json
import asyncio 
from database import DBClient
from datetime import datetime

def load_test_queries(query_set_name):
    if query_set_name == "example_set":
        test_queries = [
        {"$and": [{"IngredientName": 'onion' }, {"Quantity": { "$lt": 1.5 } }]}
        
    ]
        
def run_test_queries(queries, collection):
    for i, query in enumerate(queries):
        results = list(collection.find(query))
        print(f"Query {i+1}: {query} -> {len(results)} results found")


async def get_distinct_ingredients(collection):
    pipeline = [
        # Treat each array elem as a doc
        { "$unwind": "$Ingredients" },  
        # Get all of the same ingredient
        { "$group": { "_id": "$Ingredients.IngredientName" } }, 
        # Make it such that the distinct vals become the ID and that's all that is left in the doc
        { "$project": { "_id": 0, "IngredientName": "$_id" } }
    ]

    # Run the pipeline and asyncronously collect all of the results in memory... BE WARY lol
    ingredient_names = []
    async for document in await collection.aggregate(pipeline):
        ingredient_names.append(document['IngredientName'])
    # ingredient_names = await collection.distinct("Ingredients.IngredientName")
    return ingredient_names

async def get_min_and_max_quantities(collection):
    pipeline = [
        # Flatten all ingredients entries from each doc into one big list
        {"$unwind": "$Ingredients"},
        # Take these entries, reduce into one doc per id value. Since id is not listed, 
        # all docs are treated as part of the same group
        # Apply 
        {"$group": {
            "_id": None,
            "minQuantity": {"$min": "$Ingredients.Quantity"},
            "maxQuantity": {"$max": "$Ingredients.Quantity"}
            }
        }
    ]


    min_and_max_cursor = await collection.aggregate(pipeline)
    min_and_max = await min_and_max_cursor.to_list()

    return  min_and_max[0]["minQuantity"],  min_and_max[0]["maxQuantity"]

async def get_n_random_ingredients(collection, n):
    static_names = await get_distinct_ingredients(collection)
    sampled_ingredients = random.sample(static_names, n)
    return sampled_ingredients



async def generate_n_random_simple_criteria(collection, n=10000):
    """ Our definition of 'simple' criteria are:
      a single ingredient and exact quantity, 
      a single ingredient and quantity range
      1-3 ingredients with quantity ranges?

      This generates the list of criteria to write to a csv file rather than the queries themselves. 
      That way, if we change the way we construct a query, the generated criteria can simply be reused.
    """
    distinct_ingredients = await get_distinct_ingredients(collection)
    min_quantities, max_quantities = await get_min_and_max_quantities(collection)
    
    criteria_rows = []

    for i in range(n):
        # Pick up to 3 ingredients to search/query at random
        num_ingredients = np.random.randint(1, 4)
        ingredients = np.random.choice(distinct_ingredients, size=num_ingredients, replace=False)
        quantities = np.random.uniform(low=0.1, high=10.0, size=num_ingredients)
        # quantities = np.random.uniform(low=min_quantities, high=max_quantities + 0.1, size=num_ingredients)
        is_range = random.choice([True, False])

        criteria_rows.append({
            'Type': 'simple',
            'Exact': is_range,
            'Ingredients': ingredients,
            'Quantities': np.round(quantities, 2)
        })
    
    criteria_df = pd.DataFrame(criteria_rows)
    criteria_df.to_csv(f'simple_test_criteria_{datetime.now()}.csv', index=False)

async def generate_n_random_complex_criteria(collection, n):
    """ Our definition of 'complex' criteria are:
    over 3 ingredients with quantity ranges

    
    This generates the list of criteria to write to a csv file rather than the queries themselves. 
    That way, if we change the way we construct a query, the generated criteria can simply be reused.
    """
    distinct_ingredients = await get_distinct_ingredients(collection)

    criteria_rows = []

    for i in range(n):
        # Pick between 4 and say, 10 ingredients
        num_ingredients = np.random.randint(4, 11)
        ingredients = np.random.choice(distinct_ingredients, size=num_ingredients, replace=False)
        quantities = np.random.uniform(low=0.1, high=10.0, size=num_ingredients)
        is_range = random.choice([True, False])

        criteria_rows.append({
            'Type': 'simple',
            'Exact': is_range,
            'Ingredients': ingredients,
            'Quantities': np.round(quantities, 2)
        })
    
    criteria_df = pd.DataFrame(criteria_rows)
    criteria_df.to_csv(f'complex_test_criteria_{datetime.now()}.csv', index=False)


async def main():
    client = DBClient()
    database_name = "test_db_ranges"
    collection_name = "test_recipes_range_support"
    db = await client.get_database(database_name)
    collection = db[collection_name]
    await generate_n_random_simple_criteria(collection, n=10000)

if __name__ == "__main__":
    asyncio.run(main())

    
    #     # TODO  create reg query and create search query from generated criteria
    # reg_query = None
    # search_query = None
