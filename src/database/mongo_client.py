from pymongo import MongoClient
import os 

#TODO: Follow this tutorial for Python to create a simple DB, put the connection string in a safe place
# https://www.mongodb.com/resources/languages/python

def get_database(database_name):
 
    CONNECTION_STRING = os.environ.get("MONGO_CONNECTION_STRING", default=None)

    # Create a connection using MongoClient
    client = MongoClient(CONNECTION_STRING)

    # Creates the database if not already exists
    return client[database_name]
  
if __name__ == "__main__":   
  
   dbname = get_database()