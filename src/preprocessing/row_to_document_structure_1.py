# For going from the database row to constructing the (predictable) table of recipe docs
from pyspark.sql import SparkSession
from database import get_database
import csv
import time

# Initialize Spark session
spark = SparkSession.builder \
    .appName('CSV to MongoDB Atlas') \
    .getOrCreate()

# Read in small sample for testing
df = spark.read.csv('data/raw/first_24500_recipes.csv', header=True, inferSchema=True)

recipe_suggestion_system  = get_database('recipe_suggestion_system')
recipes = recipe_suggestion_system['recipes']

# #TODO: Func to process a single row into the desired structure
def row_to_document(row):
    document = ''
    return document

# Use appropriate PySpark dataframe method to map each row to the doc

# Insert all of the prepared docs into the collection CONCURRENTLY
# https://www.mongodb.com/docs/spark-connector/current/configuration/

# Close Spark session
spark.stop()