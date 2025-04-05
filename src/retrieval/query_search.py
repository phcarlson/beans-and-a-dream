# Backend interface for the actual PyMongo querying logic for Atlas Search
# https://www.mongodb.com/docs/atlas/atlas-search/tutorial/run-query/

from ..database import DBClient

client = DBClient()
db = client.get_database("test_db_ranges")

# FIXME: The problem with a query like this, if you run it, 
# is that it does NOT specifically associate the range with the respective ingredient
# Ex we get a result with olive oil being > the desired amount. How do we make the right search? 
result = db['test_recipes_range_support'].aggregate([
    {
        '$search': {
            "index": "ingredient_name_and_quantity",  
            "compound": {
                "must": [
                    {
                        "compound": {
                            "must": [
                                {
                                    "text": {
                                        "path": "Ingredients.IngredientName",
                                        "query": "fresh lemon juice"
                                    }
                                },
                                {
                                    "range": {
                                        "path": "Ingredients.Quantity",
                                        "lte": 4,
                                        "gt": 0
                                    }
                                }
                            ]
                        }
                    },
                    {
                        "compound": {
                            "must": [
                                {
                                    "text": {
                                        "path": "Ingredients.IngredientName",
                                        "query": "olive oil"
                                    }
                                },
                                {
                                    "range": {
                                        "path": "Ingredients.Quantity",
                                        "lte": 0.1,
                                        "gt": 0
                                    }
                                }
                            ]
                        }
                    }
                ]
            }
        }
    },
    {
        '$limit': 1  # Limit the number of results returned
    }
])

for recipe in result:
    print(recipe)