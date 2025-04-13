
import random

# TODO come up with simple to complex queries for our benchmarking consistently
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
    return ingredient_names

async def get_n_random_ingredients(collection, n):
    static_names = await get_distinct_ingredients()
    sampled_ingredients = random.sample(static_names, n)
    return sampled_ingredients

