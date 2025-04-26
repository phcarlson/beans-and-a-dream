from pymongo import AsyncMongoClient
from pymongo.server_api import ServerApi
import os 

class DBClient:
    def __init__(self):
        # Get creds stored in safe place
        #self.user = os.environ.get("MONGO_RECIPE_USER", default=None)
        #self.password = os.environ.get("MONGO_RECIPE_PW", default=None)

        self.user = "jmendiratta"
        self.password = "M8R5ajgxMXRF3Ull"
        
        # Create connection string based on creds
        self.uri = f'mongodb+srv://{self.user}:{self.password}@reverse-index.xkyk7ik.mongodb.net/?retryWrites=true&w=majority&appName=Reverse-Index'
        
       # Per this doc https://www.mongodb.com/docs/manual/administration/connection-pool-overview/, 
       # we will only create one client instance throughout the application PER CLUSTER
        self.client = AsyncMongoClient(self.uri, server_api=ServerApi('1'))
    
    async def _test_connection(self):
        try:
            await self.client.admin.command('ping')
            print("Pinged your deployment. You successfully connected to MongoDB!")
        except Exception as e:
            print("MongoDB connection failed!")
            print(e)
    
    async def get_database(self, database_name):
        """Returns a database instance, creating it if it does not exist only once data starts to be added."""
        return self.client[database_name]

    async def close_connection(self):
        self.client.close()