from pymongo.operations import SearchIndexModel
import json
import asyncio

# https://www.mongodb.com/docs/languages/python/pymongo-driver/current/indexes/
async def create_search_index(collection, index_name, 
                 search_index_model_instance = None, 
                 search_index_model_file_name = None):
    
    """Creates an Atlas SEARCH index on a database collection, 
    by either passing in search index model instance directly 
    or specifying file name to the JSON definition
    in directory 'search_indexes_to_try' """

    try:   
        specified_model = None
        if search_index_model_instance != None:
            specified_model = search_index_model_instance
        elif search_index_model_file_name != None:
            # Load model json into object to pass
            with open(f'src/database/search_indexes_to_try/{search_index_model_file_name}', 'r') as f:
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
            await collection.create_search_index(model=specified_model)
            return True
        else:
            return False
    except Exception as e:
        print("There was a problem creating the search index {}!".format(index_name))
        print("The exception: {}".format(e))
        return False

def main():
    #TODO: Read about search partitions, see if it applies, and run experiments with it https://www.mongodb.com/docs/atlas/atlas-search/create-index/
    create_search_index(database_name, collection_name, index_name, 
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