# Use the Python Driver (PyMongo?) to create an Atlas Search index, 
# define the search index from your application and call the create_search_index() method.

# See: https://www.mongodb.com/docs/atlas/atlas-search/create-index/ after selecting Python as the language
# Also see how it differs from the regular MongoDB querying: https://www.mongodb.com/resources/basics/databases/database-search

# We can index on specific fields, which for us would be the ingredient field
# https://www.mongodb.com/docs/atlas/atlas-search/define-field-mappings/

# We might also want to see how to optimize searching exact ranges like < 2 tomatoes??? (https://www.mongodb.com/docs/atlas/atlas-search/field-types/number-type/) 

from pymongo.operations import SearchIndexModel
from database import get_database

def create_index(collection_name, index_name, is_dynamic=True, fields_to_index=[]):
    try:
        recipe_suggestion_system = get_database('recipe_suggestion_system')
        collection = recipe_suggestion_system[collection_name]

        if is_dynamic:
            # Create your index model, then create the search index
            search_index_model = SearchIndexModel(
                definition={
                    "mappings": {
                        "dynamic": True
                    },
                },
                name=index_name,
            )
            result = collection.create_search_index(model=search_index_model)
            print(result)

        elif fields_to_index != None:
            # When static, we are *specifying the exact fields* in an array. Since not all the recipe fields are actually relevant, 
            # we likely want to only index on the ingredients and the preprocessed keywords
            search_index_model = SearchIndexModel(
                definition={
                    {
                        "mappings": {
                            "dynamic": False,
                            "fields": {
                            "recipeName": { "type": "string" },
                            "ingredients": {
                                "type": "array",
                                "fields": [
                                { "type": "string" }
                                ]
                            },
                            "instructions": { "type": "string" },
                            "description": { "type": "string" },
                            "prepTime": { "type": "int" },
                            "cookTime": { "type": "int" },
                            "author": { "type": "string" }
                            }
                        }
                    },
                },
                name=index_name,
            )

            result = collection.create_search_index(model=search_index_model)
            print(result)

    except Exception as e:
        print(f"Error creating index: {e}")

if __name__ == "__main__":   
  
   pass