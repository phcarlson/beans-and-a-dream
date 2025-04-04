# For going from the database row to constructing the nonconventional table of ingredient docs
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, struct, arrays_zip, transform
from preprocessing_utils import convert_fractions_udf_wrapper
import json
import os
import time

def main():
    # Initialize Spark session with MongoDB connection string based on creds
    user = os.environ.get("MONGO_RECIPE_USER", default=None)
    password = os.environ.get("MONGO_RECIPE_PW", default=None) 
    uri = f'mongodb+srv://{user}:{password}@reverse-index.xkyk7ik.mongodb.net/?retryWrites=true&w=majority&appName=Reverse-Index'
    spark = (SparkSession.builder 
        .appName('CSV to MongoDB Atlas Non-Conventional') 
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.1") 
        .config("spark.mongodb.read.connection.uri", uri) 
        .config("spark.mongodb.write.connection.uri", uri) 
        .getOrCreate())
    
    
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    print("LOADING DATASET AND BEGINNING CONVENTIONAL PREPROCESSING....")
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

    start_preprocessing_time = time.time()

    # Read in the big big recipes dataset
    df = spark.read.parquet('data/raw/recipes.parquet')

    # TODO: Probably do any filtering and stuff early on rather than later to reduce the load

    # Register the UDF for converting the (often mixed) frac quantities to numbers to work with
    # https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.udf.html
    convert_fractions_udf = convert_fractions_udf_wrapper(convert_to_float=True)
    df_to_numeric_quantities = df.withColumn("RecipeIngredientQuantities", convert_fractions_udf(col("RecipeIngredientQuantities")))
   
    # TODO: Since we want the documents to be organized differently than the original recipe format,
    # before loading to MongoDB, do that here. Whatever you think would be another interesting way to store it,
    # to compare efficiency

    end_preprocessing_time = time.time()
    elapsed_preprocessing_time = end_preprocessing_time - start_preprocessing_time
    print(f"Time taken for preprocessing: {elapsed_preprocessing_time} seconds")
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")


    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    print("SHARING TOP NON-CONVENTIONAL RESULTS IN JSON FORMAT....")
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    # Convert JSON to read and write, except this is single-line 
    # df_to_numeric_quantities.toJSON().coalesce(1).saveAsTextFile("data/processed/test.json")
    json_list = df_to_numeric_quantities.limit(10).toJSON().collect()
    # Turn them into prettier JSON by loading them back to a Python dict and reconstructing them as nicely indented and stuff
    for i, json_str in enumerate(json_list):
        parsed_json = json.loads(json_str)
        print(f"\nConverted row {i + 1}:")
        print(json.dumps(parsed_json, indent=4))
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

    #https://www.mongodb.com/docs/spark-connector/current/batch-mode/batch-write/#std-label-batch-write-to-mongodb
    # Tell PySpark to use the mongodb connector with .format
    # Mode append adds new docs without overwriting
    # If database and collection do not exist, it creates it
    if write_to_db:
        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        print("WRITING NON-CONVENTIONAL TO DB....")
        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

        print("Database name:{}".format(database_name))
        print("Collection name:{}".format(database_collection))
        print("Max batch size:{}".format(max_batch_size))
        # print("Id field list:{}".format("_id" if id_field_list == None else id_field_list))
        print("Write type:{}".format(write_type))

        start_write_time = time.time()

        df_to_numeric_quantities_limited = (df_to_numeric_quantities if total_doc_size == None 
                                            else df_to_numeric_quantities.limit(total_doc_size))
        
        (df_to_numeric_quantities_limited.write.format("mongodb")
        .mode(write_type)
        .option("database", database_name)
        .option("collection", database_collection)
        .option("maxBatchSize", max_batch_size) 
        # .option("idFieldList", "_id" if id_field_list == None else id_field_list) 
        .save())

        end_write_time = time.time()
        elapsed_write_time = end_write_time - start_write_time
        print(f"Time taken for writing to DB: {elapsed_write_time} seconds")
        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

    spark.stop()
  
if __name__ == "__main__":

    # Per https://www.mongodb.com/docs/spark-connector/current/batch-mode/batch-write-config/ , setting up the correct configuration 
    database_name = "test_db_ranges"
    database_collection = "test_recipes_range_support"
    max_batch_size = 1000

    # If none, I will load all of them to the remote collection. 
    # Otherwise, say for experimenting, you can limit to the first __
    total_doc_size = 10000

    # Whether we are just testing diff doc structures and don't want to spend the time writing
    write_to_db = True

    # The MongoDB Spark Connector supports the following save modes:
    # append
    # overwrite
    # If you specify the overwrite write mode, the connector drops the target collection and creates a new collection that uses the default collection options.
    write_type = "overwrite"

    # Start the preprocessing script up
    main()