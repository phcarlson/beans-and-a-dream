import sys
sys.path.append('../')

from database import generate_n_random_complex_criteria, typefy
from retrieval import IngredientAtlasSearch
from util import IngredientList
from datetime import datetime

import pandas as pd
import asyncio 
from database.mongo_client import DBClient

async def atlas_search_creator(client, db, r_sample, collection_name: str = "test_recipes_range_support", search_index: str = "ING-EMB-DYN", index_type: str = "embeddedDocument", save_searches_to_csv: bool = True) -> list[IngredientAtlasSearch]:
    '''function that takes a complex generated criteria and translates it into a list atlas searches using the class'''
    df = typefy(r_sample)
    atlas_searches = []

    #read in each row one at a time
    for _, row in df.iterrows():

        current_search = IngredientAtlasSearch(client, db, collection_name = collection_name, search_index = search_index, index_type = index_type)

        ingredient_list = row["Ingredients"]
        quantity_list = row["Quantities"]
        exact = row["Exact"]
        qType = row["Type"]
        qntSliceStart = 0

        #collect queries for ing/qnt combos for this row
        ing_list = IngredientList()

        if exact:
            for ing, qnt in zip(ingredient_list, quantity_list):
                ing_list.addIngredientExact(str(ing), float(qnt))
        else:
            if qType == "complex":
                for ing in ingredient_list:
                    #for each ingredient, pull 2 values from the Quantity list
                    qntMinMax = quantity_list[qntSliceStart:qntSliceStart + 2]

                    #get min and max of the two values
                    qntMin = min(qntMinMax)
                    qntMax = max(qntMinMax)

                    ing_list.addIngredientRanged(str(ing), float(qntMin), float(qntMax))
                    
                    #increase index. next ingredient will get next 2 quantity values
                    qntSliceStart +=2
            else:
                for ing, qnt in zip(ingredient_list, quantity_list):
                    ing_list.addIngredientRanged(str(ing), 0, float(qnt))
                
        current_search.setIngredientList(ing_list)
        atlas_searches.append(current_search)

        '''Uncomment below if you also want to run the query and have the instance get the recipe results'''
        #await current_search.queryRecipes()

    if save_searches_to_csv:
        searches_generated = []
        for search_obj in atlas_searches:
            search = search_obj.generateQuery()
            searches_generated.append({"Search": search})
        
        atlas_searches_df = pd.DataFrame(searches_generated)
        now = datetime.now()
        datetime_file = now.strftime("%Y%m%d_%H%M%S")
        atlas_searches_df.to_csv(f'{qType}_test_searches_{datetime_file}.csv', index=False)

    #returns list of atlas_searches. Simply choose one and run atlas_search.run_recipeSearch, and print it with print(self.recipeList)
    return atlas_searches   
    

async def main():
    client = DBClient()
    database_name = "test_db_ranges"
    collection_name = "test_recipes_range_support"
    db = await client.get_database(database_name)
    collection = db[collection_name]

    # Adjust n to create more atlas searches
    # cmpx_crt = await generate_n_random_complex_criteria(collection, n=10000, save_criteria = True)
    atlas_searches = await atlas_search_creator(client, db, 'complex_test_criteria_20250427-204518.csv')
    
    # looking at just the first search, you can do it for as man as you want
    first_search = atlas_searches[0]

    # generates the query string using the search class' ingredient list
    first_search.generateQuery()

    # gets the generated query string
    '''first_search.getQuery()'''

    # the searcher will run the query and store the results in its recipeList
    await first_search.queryRecipes()

    # gets the recipeList resulting from the search -> use print to get it to print pretty
    '''
    first_search_query_results = first_search.getQueryResults()
    print(first_search_query_results)
    '''


if __name__ == "__main__":
    asyncio.run(main())
