# Backend interface for the traditional MongoDB queries
from ..database import DBClient

client = DBClient()
db = client.get_database("test_db_ranges")