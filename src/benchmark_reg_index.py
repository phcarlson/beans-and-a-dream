import os
from database import DBClient, load_test_queries, create_reg_index
import asyncio


async def run_reg_index_benchmark():
        
    # Create the client ONCE for the experimental session
    client = DBClient()

    # Path to all of our test reg indexes
    path_to_search_indexes = "src/database/reg_indexes_to_try"

    # Plug in what collection we will work over (the one with ALL the items in it presumably)
    database_name = "test_db_ranges"
    collection_name = "test_recipes_range_support"
    db = await client.get_database(database_name)
    collection = db[collection_name]

    # Iterate over each possible one in the folder
    for index_file_name in [file for file in os.listdir(path_to_search_indexes) if file.endswith('.json')]:

        # Get just the file name without the extension, to use as the index name
        index_name = os.path.splitext(index_file_name)[0]
     
        succeeded = await create_reg_index(client=client, 
                                collection=collection,
                                index_name=index_name, 
                                setup_instance = None, 
                                setup_file_name = index_file_name)
        if succeeded:
      
            # Worked, so we proceed with the query testing
            #TODO Use Locust?
            # Finally reset to have a clean slate for the next experiment
            # await collection.drop_index(index_name)
            pass 
if __name__ == "__main__":

    asyncio.run(run_reg_index_benchmark())
