
import random
import numpy as np 
import pandas as pd
import asyncio 
from ast import literal_eval 

from database.mongo_client import DBClient
from datetime import datetime
 
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


async def generate_n_random_simple_criteria(collection, n=10000, save_criteria = True):
    """ Our definition of 'simple' criteria are:
      a single ingredient and exact quantity, 
      a single ingredient and quantity range
      1-3 ingredients with quantity ranges?

      This generates the list of criteria to write to a csv file rather than the queries themselves. 
      That way, if we change the way we construct a query, the generated criteria can simply be reused.
    """

    # Get the list of most to least freq ingredients
    freqs = await get_ingredient_frequencies(collection)

    # Based on a threshold, determine what is considered common or not
    common_ingredients, rare_ingredients = split_common_rare(freqs, common_threshold=0.1)

    criteria_rows = []

    for i in range(n):
        # Pick up to 3 ingredients to search/query at random
        num_ingredients = np.random.randint(1, 4)
        ingredients = sample_ingredients(common_ingredients, rare_ingredients, num_ingredients, common_fraction=0.3)
        quantities = np.random.uniform(low=0.10, high=10.00, size=num_ingredients)
        is_exact = random.choice([True, False])

        criteria_rows.append({
            'Type': 'simple',
            'Exact': is_exact,
            'Ingredients': ingredients,
            'Quantities': np.round(quantities, 2).tolist()
        })
    
    criteria_df = pd.DataFrame(criteria_rows)

    if save_criteria:
        now = datetime.now()
        datetime_file = now.strftime("%Y%m%d_%H%M%S")
        criteria_df.to_csv(f'simple_test_criteria_{datetime_file}.csv', index=False)
    
    #can use as a source for query construction
    return criteria_df


async def generate_n_random_complex_criteria(collection, n, save_criteria = True):
    """ Our definition of 'complex' criteria are:
    over 3 ingredients with quantity ranges

    This generates the list of criteria to write to a csv file rather than the queries themselves. 
    That way, if we change the way we construct a query, the generated criteria can simply be reused.
    """

    # Get the list of most to least freq ingredients
    freqs = await get_ingredient_frequencies(collection)

    # Based on a threshold, determine what is considered common or not
    common_ingredients, rare_ingredients = split_common_rare(freqs, common_threshold=0.1)
    
    criteria_rows = []
    for i in range(n):
        # Pick between 4 and say, 10 ingredients
        num_ingredients = np.random.randint(4, 11)
        
        # Determine what percentage of the ingredients sampled we want to be common or rare, to prevent artificially short running queries
        ingredients = sample_ingredients(common_ingredients, rare_ingredients, num_ingredients, common_fraction=0.3)
               
        is_exact = random.choice([True, False])

        if is_exact:
            # Weights toward smaller quantities (assuming querying with mostly American metrics)
            # quantities = 0.10 + (10.00 - 0.10) * np.random.beta(2, 5, size=num_ingredients)
            np.random.uniform(low=0.10, high=10.00, size=num_ingredients)
        
        # In terms of complex queries, what this means is that it has an upper AND lower bound specified (that isn't just 0)
        else:
            quantities = np.random.uniform(low=0.10, high=10.00, size=(num_ingredients * 2))

        criteria_rows.append({
            'Type': 'complex',
            'Exact': is_exact,
            'Ingredients': ingredients,
            'Quantities': np.round(quantities, 2).tolist()
        })
    
    criteria_df = pd.DataFrame(criteria_rows)

    if (save_criteria):
        #grab current datetime and transform for legal file name
        now = datetime.now()
        datetime_file = now.strftime("%Y%m%d-%H%M%S")
        criteria_df.to_csv(f'complex_test_criteria_{datetime_file}.csv', index=False) #{datetime.now()}
    
    #can use as a source for query construction
    return criteria_df


def try_literal_eval(csv_string):
    '''library function that parses basic python literals'''
    try:
        return literal_eval(csv_string)
    except ValueError:
        print("Keep an eye on the output, quantities and ingredients did not pass literal_eval.")
        return csv_string


#not sure if necessary or just use literal_eval in iterrows
def typefy(r_sample) -> pd.DataFrame:
    '''func that helps make csv generated criteria usable
        pesky charlatan strings'''
    if isinstance(r_sample, pd.DataFrame):
        df = r_sample 
    elif isinstance(r_sample, str):
        try:
            with open(r_sample, "r") as file:
                df = pd.read_csv(file, header=0)
                
                #clean values, transform from str to list
                df["Ingredients"] = df["Ingredients"].apply(try_literal_eval)
                
                #clean values, transform from str to list, convert to float. 
                df["Quantities"]  = df["Quantities"].apply(try_literal_eval)

        except:
            print("ERROR: Check csv file. File must be result of query_creator().")
            raise ImportError
    else:
        print("Must be existing generated criteria CSV string or pandas DF")
        raise TypeError
    return df


async def query_creator(collection, r_sample) -> pd.DataFrame: #creates "or" filter per row of criteria

    '''function that takes either simple or complex generated criteria and translate them into mongoDB filter syntax
        r_sample should be either pd.dataframe that is created from generators or filepath to generator created CSV
        currently queries each criteria and outputs a pandas.DF of queries. also saves dataframe as CSV'''
    df = typefy(r_sample)

    recipeQueries = []

    #read in each row one at a time
    for index, row in df.iterrows():
        ingredient_list = row["Ingredients"]
        quantity_list = row["Quantities"]
        exact = row["Exact"]
        qType = row["Type"]
        qntSliceStart = 0

        #collect queries for ing/qnt combos for this row
        recipe = []

        if exact:
            for ing, qnt in zip(ingredient_list, quantity_list):
                #NOTE: qnt should be numeric since Quantities field is a number in mongo
                #all values cast back to native python so csv queries dont have "np.str_" and "np.float64"
                query = {
                    "Ingredients.IngredientName": str(ing),
                    "Ingredients.Quantity" : float(qnt)
                }
                recipe.append(query)

        else:
            if qType == "complex":
                for ing in ingredient_list:
                    #for each ingredient, pull 2 values from the Quantity list
                    qntMinMax = quantity_list[qntSliceStart:qntSliceStart + 2]

                    #get min and max of the two values
                    qntMin = min(qntMinMax)
                    qntMax = max(qntMinMax)

                    query = {
                        "Ingredients.IngredientName": str(ing),
                        "Ingredients.Quantity" : {
                        "$gte": float(qntMin),
                        "$lte": float(qntMax)
                        }
                    }
                    recipe.append(query)
                    
                    #increase index. next ingredient will get next 2 quantity values
                    qntSliceStart +=2
            else:
                for ing, qnt in zip(ingredient_list, quantity_list):
                    query = {
                        "Ingredients.IngredientName": str(ing),
                        "Ingredients.Quantity" : {"$lte" : float(qnt)}
                    }
                    recipe.append(query)
                
        #gather list of queries and slap an or on it
        db_query = {"$or" : recipe}

        #query the database whynot. see the fruits of your labor
        # print(f'Query {index+1} : {db_query}', f'{await collection.count_documents(db_query)} results found.\n', sep= '\n')

        #add all row queries together with associated type and exact reference 
        recipeQueries.append({
            "Type" : qType,
            "Exact" : exact,
            "Query" : db_query
            })
    
    #convert and save
    recipeQueries_df = pd.DataFrame(recipeQueries)
    now = datetime.now()
    datetime_file = now.strftime("%Y%m%d_%H%M%S")
    recipeQueries_df.to_csv(f'{qType}_test_queries_{datetime_file}.csv', index=False) #{datetime.now()}
    
    #returns dataframe if you want to query right away or randomly. Need to use index
    return recipeQueries_df


async def csv_query_reader(collection, csv_string):
    '''func to read in previosly created queries and query database'''
    try:
        with open(csv_string, "r") as file:
            df = pd.read_csv(file, header=0)

            #read each row one at a time
            for index, row in df.iterrows():
                
                #translate out of a string back to a dic
                db_query = literal_eval(row["Query"])                

                #print(db_query, "**"*5 ,sep= '\n')
                print(f'Query {index+1} : {db_query}', f'{await collection.count_documents(db_query)} results found.\n', sep= '\n')       
    except:
        print("ERROR: Check csv file. File must be result of query_creator().")


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# AI attribution: 
# Source = 
    # ChatGPT
# Prompt = 
    # the rest of existing code for producing criteria in this file + "How do I balance sampling the common ingredients with rare ones so complex queries are not artificially fast because 10 ingredients could all be rare" 
# Significant response =
    # Wrapped code below, with own minor adjustments, and verifying step by step with own comments
# Verifications = 
    # https://www.mongodb.com/docs/manual/reference/operator/aggregation/unwind/, https://stackoverflow.com/questions/66863545/get-frequency-for-multiple-elements-in-all-documents-inside-a-collection-mongodb 

async def get_ingredient_frequencies(collection):
    pipeline = [
        # Deconstructs the array field to output a doc per element (the sub doc that includes name and quantity)
        {"$unwind": "$Ingredients"},
        # Groups by the ingredient name and then sums count of occurrences by incrementing by  1, thus disregards quantities 
        {"$group": {"_id": "$Ingredients.IngredientName", "count": {"$sum": 1}}},
        # Goes from most to least frequent ingredients
        {"$sort": {"count": -1}},
    ]

    ingredient_counts = []
    async for document in await collection.aggregate(pipeline):
        ingredient_counts.append((document["_id"], document["count"]))

    return ingredient_counts  
     
def split_common_rare(ingredient_counts, common_threshold=0.1):
    '''Based on the given threshold, split full ingredeint list into common/rare'''

    # Should already be sorted by get_ingredient_frequencies but just in case an unsorted list is passed
    ingredient_counts = sorted(ingredient_counts, key=lambda x: -x[1]) 

    # Takes the percent of common based on threshold to make the split
    n_common = int(len(ingredient_counts) * common_threshold)
    common_ingredients = [name for name, _ in  ingredient_counts[:n_common]]
    rare_ingredients = [name for name,  _ in ingredient_counts[n_common:]]
    return common_ingredients, rare_ingredients


def sample_ingredients(common_ingredients, rare_ingredients, n, common_fraction=0.6):
    if n != 0:
        # Based on the frac of common to sample, it's either a number > 1 or, if the percent leads to 0, just sample 1 common at least. 
        n_common = max(1, int(n * common_fraction))
        # The rest is the num rare
        n_rare = n - n_common
        # Select from both groups and combine to be the query ingredients
        sampled_common = random.sample(common_ingredients, n_common)
        sampled_rare = random.sample(rare_ingredients, n_rare)
        return sampled_common + sampled_rare
    else:
        return [], []
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


async def main():
    client = DBClient()
    database_name = "test_db_ranges"
    collection_name = "test_recipes_range_support"
    db = await client.get_database(database_name)
    collection = db[collection_name]
    
    #smpl_crt = generate_n_random_simple_criteria(collection, n=2)
    # cmpx_crt = await generate_n_random_complex_criteria(collection, n=10000)
        #queryCreator returns a list of queries (1 per n). will need to query datafram index or come up with another way
    #query inside query functions? could pass collection in and just run through queries as they are generated
    filterQueries = await query_creator(collection, 'complex_test_criteria_20250427-204518.csv')
    #print(collection.count_documents(filterQueries.iloc[0, 2]))

if __name__ == "__main__":
    asyncio.run(main())
