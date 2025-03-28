# To use the Python Driver (PyMongo?) to create an Atlas Search index, 
# define the search index from your application and call the create_search_index() method.

# See: https://www.mongodb.com/docs/atlas/atlas-search/create-index/ after selecting Python as the language
# Also see how it differs from the regular MongoDB search: https://www.mongodb.com/resources/basics/databases/database-search

# We can index on specific fields, which for us would be the ingredient field
# We will also want to see how to optimize searching exact ranges like < 2 tomatos (https://www.mongodb.com/docs/atlas/atlas-search/field-types/number-type/) 