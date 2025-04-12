"""
This package is to provide an interface for the actual construction/maintenance of the MongoDB Atlas Search index and/or standard DB,
in addition to anything required for actually connecting to the cloud DB to use it at all.
"""

from .mongo_client import DBClient
from .search_index import create_search_index
from .regular_index import create_reg_index

from .database_test_utils import load_test_queries