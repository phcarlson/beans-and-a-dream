# Use the Python Driver (PyMongo?) to create an Atlas Search index, 
# define the search index from your application and call the create_search_index() method.

# See: https://www.mongodb.com/docs/atlas/atlas-search/create-index/ after selecting Python as the language
# Also see how it differs from the regular MongoDB querying: https://www.mongodb.com/resources/basics/databases/database-search

# We can index on specific fields, which for us would be the ingredient field
# https://www.mongodb.com/docs/atlas/atlas-search/define-field-mappings/

# We might also want to see how to optimize searching exact ranges like < 2 tomatoes??? (https://www.mongodb.com/docs/atlas/atlas-search/field-types/number-type/) 

from pymongo.operations import SearchIndexModel
from mongo_client import DBClient
import json
    
def create_index(database_name, collection_name, index_name, 
                 search_index_model_instance = None, 
                 search_index_model_file_name = None):
    
    """Creates an Atlas SEARCH index on a database collection, 
    by either passing in search index model instance directly 
    or specifying file name to the JSON definition
    in directory 'search_indexes_to_try' """

    try:    
        client = DBClient()
        db = client.get_database(database_name)
        collection = db[collection_name]

        specified_model = None
        if search_index_model_instance != None:
            specified_model = search_index_model_instance
        elif search_index_model_file_name != None:
            # Load model json into object to pass
            with open(f'search_indexes_to_try/{search_index_model_file_name}', 'r') as f:
                specified_definition = json.load(f)
                specified_model = (
                    SearchIndexModel(
                        definition=specified_definition,
                        name=index_name,
                    )
                )
        else:
            print("Index not created, because no model was specified.")
        
        if specified_model != None:
            # Now to actually create the index
            result = collection.create_search_index(model=specified_model)
            print(result)
    finally:
        client.close()

def main():
    create_index(database_name, collection_name, index_name, 
                 search_index_model_instance, search_index_model_file_name)

if __name__ == "__main__":
    database_name = "db_name_here"
    collection_name = "collection_name_here"
    index_name = "index_name_here"

    search_index_model_example = (
        SearchIndexModel(
            definition={
                "mappings": {
                    "dynamic": True
                },
            },
            name=index_name,
        )
    )

    search_index_model_instance = search_index_model_example
    search_index_model_file_name = None
    
    main()