from mongo_client import DBClient

# TODO: If there is anything necessary to automate the process of creating a regular MongoDB index
# for our experiments, put that all here

# https://www.mongodb.com/docs/languages/python/pymongo-driver/current/indexes/
def create_reg_index(database_name, collection_name):
    
    """Creates a regular MongoDB index on a database collection"""

    try:    
        client = DBClient()
        db = client.get_database(database_name)
        collection = db[collection_name]

    finally:
        client.close()

def main():
    pass 

if __name__ == "__main__":
    database_name = "db_name_here"
    collection_name = "collection_name_here"

    main()