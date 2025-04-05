from pymongo import MongoClient
from pymongo.server_api import ServerApi
import os 

class DBClient:
    def __init__(self):
        # Get creds stored in safe place
        self.user = os.environ.get("MONGO_RECIPE_USER", default=None)
        self.password = os.environ.get("MONGO_RECIPE_PW", default=None)
        
        # Create connection string based on creds
        self.uri = f'mongodb+srv://{self.user}:{self.password}@reverse-index.xkyk7ik.mongodb.net/?retryWrites=true&w=majority&appName=Reverse-Index'
        
       # Per this doc https://www.mongodb.com/docs/manual/administration/connection-pool-overview/, 
       # we will only create one client instance throughout the application PER CLUSTER
       # TODO experiment with mult clusters ???
        self.client = MongoClient(self.uri, server_api=ServerApi('1'))
        
        # Test connection
        self._test_connection()
    
    def _test_connection(self):
        try:
            self.client.admin.command('ping')
            print("Pinged your deployment. You successfully connected to MongoDB!")
        except Exception as e:
            print("MongoDB connection failed!")
            print(e)
    
    def get_database(self, database_name):
        """Returns a database instance, creating it if it does not exist only once data starts to be added."""
        return self.client[database_name]
