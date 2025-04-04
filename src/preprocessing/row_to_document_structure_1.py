# For going from the database row to constructing the (predictable) table of recipe docs
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, struct, arrays_zip, transform
from preprocessing_utils import convert_fractions_udf_wrapper
import json
import os
import time

# See this for limits on diff things
# We have 512 MB storage free
#https://www.mongodb.com/docs/manual/reference/limits/
#https://www.mongodb.com/docs/atlas/sizing-tier-selection/

def main():
    # Initialize Spark session with MongoDB connection string based on creds
    user = os.environ.get("MONGO_RECIPE_USER", default=None)
    password = os.environ.get("MONGO_RECIPE_PW", default=None) 
    uri = f'mongodb+srv://{user}:{password}@reverse-index.xkyk7ik.mongodb.net/?retryWrites=true&w=majority&appName=Reverse-Index'
    spark = (SparkSession.builder 
        .appName('CSV to MongoDB Atlas Conventional') 
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

    # df_length = df.count()
    # print(f"Number of rows in the DataFrame at beginning: {df_length}")

    # Because I will make an ingredients array where the elements are ingredient/quantity pairs, need to be same length
    # I noticed that whoever scraped the data was pretty wreckless about scraping recipe lists where not all ingredients had a measurement.
    # This led to mismatched list lengths... 
    df_where_quantity_length_matches_part_length = df.filter( F.size(col('RecipeIngredientQuantities')) ==  F.size(col('RecipeIngredientParts'))).drop("RecipeId")
    # df_length = df_where_quantity_length_matches_part_length.count()
    # print(f"Number of rows in the DataFrame where quantity/parts match length: {df_length}")

    # TODO: Show how the order matters in terms of speed for filtering first, then to numeric vs the other way around :)
    # Register the UDF
    # https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.udf.html
    convert_fractions_udf = convert_fractions_udf_wrapper(convert_to_float=True)
    df_to_numeric_quantities = df_where_quantity_length_matches_part_length.withColumn("RecipeIngredientQuantities", convert_fractions_udf(col("RecipeIngredientQuantities")))
    # df_to_numeric_quantities.printSchema()

    # Goodness gracious this took a long time to figure out-- okay, now we want to pair ingredient names and quantities together because
    # when we do the search index we are going to want to do a compound search for rice that is lte 0.25 quantity, and onion that is lte 1 quantity,
    # and the subdocs help associate those two
    df_to_numeric_quantities_with_ingredient_maps = df_to_numeric_quantities.withColumn(
        "Ingredients", 
        transform(
            arrays_zip(col("RecipeIngredientParts"), col("RecipeIngredientQuantities")), 
            lambda x: struct(x.RecipeIngredientParts.alias("IngredientName"), x.RecipeIngredientQuantities.alias("Quantity"))
            )
        ).drop("RecipeIngredientParts", "RecipeIngredientQuantities")


    # Debugging to see if the structs worked out okay 
    df_to_numeric_quantities_with_ingredient_maps.printSchema()
    # df_to_numeric_quantities_with_ingredient_maps.select("Ingredients").show(n=5, truncate=False)

    end_preprocessing_time = time.time()
    elapsed_preprocessing_time = end_preprocessing_time - start_preprocessing_time
    print(f"Time taken for preprocessing: {elapsed_preprocessing_time} seconds")
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")


    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    print("SHARING TOP CONVENTIONAL RESULTS IN JSON FORMAT....")
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    # Convert JSON to read and write, except this is single-line 
    # df_to_numeric_quantities.toJSON().coalesce(1).saveAsTextFile("data/processed/test.json")
    json_list = df_to_numeric_quantities_with_ingredient_maps.limit(10).toJSON().collect()
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
        print("WRITING CONVENTIONAL TO DB....")
        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

        print("Database name:{}".format(database_name))
        print("Collection name:{}".format(database_collection))
        print("Max batch size:{}".format(max_batch_size))
        print("Write type:{}".format(write_type))
        # print("Id field list:{}".format("_id" if id_field_list == None else id_field_list))

        start_write_time = time.time()

        df_to_numeric_quantities_limited = (df_to_numeric_quantities_with_ingredient_maps if total_doc_size == None 
                                            else df_to_numeric_quantities_with_ingredient_maps.limit(total_doc_size))
        
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

    # If none, defaults to the MongoDB object id it creates. Otherwise, uses the field(s) specified
    # id_field_list = "RecipeId"
    # I commented this out because I guess it's beneficial/easier to use MongoDB's generated IDs instead

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