from database import DBClient
import asyncio
import json
from pymongo.collation import Collation

# https://www.mongodb.com/docs/languages/python/pymongo-driver/current/indexes/
# https://stackoverflow.com/questions/24316117/mongodb-difference-between-index-on-text-field-and-text-index
# https://dev.to/erikhatcher/when-not-to-use-atlas-search-kh9
#https://www.mongodb.com/docs/manual/core/indexes/index-types/index-multikey/multikey-index-bounds/


# https://www.mongodb.com/docs/manual/tutorial/equality-sort-range-rule/#std-label-esr-indexing-rule

#Limit the Number of Query Results to Reduce Network Demand
# MongoDB cursors return results in groups of multiple documents. If you know the number of results you want, you can reduce the demand on network resources by issuing the limit() method.

# This is typically used in conjunction with sort operations. For example, if you need only 10 results from your query to the posts collection, you would issue the following command:

# db.posts.find().sort( { timestamp : -1 } ).limit(10)

# MongoDB cannot perform an index sort on the results of a range filter. Place the range filter after the sort predicate so MongoDB can use a non-blocking index sort. For more information on blocking sorts, see

# Alternatively can compare the diff of converting everythign to lowercase ahead of time ?

# https://www.mongodb.com/docs/manual/core/indexes/index-types/index-compound/sort-order/

async def create_reg_index(client, collection, index_name, setup_instance = None, setup_file_name = None):
    
    """ Creates a regular MongoDB index on a database collection, 
    by either passing in array of fields (and sort order) and possibly collate directly 
    or specifying file name to the JSON definition
    in directory 'reg_indexes_to_try' """
 
    try:  
        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        print("CREATING INDEX NAMED '{}'....".format(index_name))
        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

        specified_setup = None
        if setup_instance != None:
            specified_setup = setup_instance
        elif setup_file_name != None: 
            # Load model json into object to pass
            with open(f'src/database/reg_indexes_to_try/{setup_file_name}', 'r') as f:
                specified_setup = json.load(f)
                # If this does not exist we are SOL anyway
                indexes_to_set_up = specified_setup["indexes"] 
                # It's either just the fields to index on in order, 
                # or it also has collation for a string category to do case insensitive direct comparison
                for i, index in enumerate(indexes_to_set_up):
                    # Get the key-vals as list of tuples
                    fields_to_index_ordered = list(index.items())
                    if "collation" in specified_setup:
                        collation =  specified_setup["collation"]
                        await collection.create_index(fields_to_index_ordered, collation=Collation(locale = collation["collation"]["locale"], strength = collation["collation"]["strength"]), name= f"{index_name}{i}" )

                    else:
                        await collection.create_index(fields_to_index_ordered, name= f"{index_name}{i}")

                # Confirm the index was in fact created
                indexes_made = await collection.list_indexes()
                index_names = [ index["name"] async for index in indexes_made]

                for i in range(len(indexes_to_set_up)):
                    was_made = f"{index_name}{i}" in index_names
                    if was_made != True:
                        print("Problem with this index creation... index {} was not found when checked.".format(index_name))
                        return False
                else:
                    print("All new indexes were found in the list after creation!".format(index_name))
                    return True

        # Neither option was passed in           
        else:
                print("Index not created, because no setup was specified.")
                return False
                
        
    except Exception as e:
        print("There was a problem creating the reg index {}!".format(index_name))
        print("The exception: {}".format(e))
        return False

async def main():
    async def test():
        client = DBClient()
        database_name = "test_db_ranges"
        collection_name = "test_recipes_range_support"
        db = client.get_database(database_name)
        collection = db[collection_name] 
        await collection.create_index([("Ingredients.IngredientName", 1), ("Ingredients.Quantity", 1)], collation=Collation(locale="simple", strength=2), name="index_name_test")
    await test()
    
if __name__ == "__main__":
    database_name = "db_name_here"
    collection_name = "collection_name_here"

    asyncio.run(main())
