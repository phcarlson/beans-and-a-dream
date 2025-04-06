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

    #register UDF
    pad_Ingred_Quant_udf = F.udf(pad_Ingred_Quant, ArrayType(ArrayType(StringType())))
    
    #replace columns with updated padded columns
    df_padded = df.withColumns({"RecipeIngredientParts": pad_Ingred_Quant_udf(df.RecipeIngredientParts, df.RecipeIngredientQuantities)[0], 
                                "RecipeIngredientQuantities": pad_Ingred_Quant_udf(df.RecipeIngredientParts, df.RecipeIngredientQuantities)[1]})
    #print(df_padded.filter(F.size(df_padded.RecipeIngredientQuantities) ==  F.size(df_padded.RecipeIngredientParts)).count())

#I kept running out of memory for the explode/join/group so had to do in batches
    #convert quantity field to numbers. Break vqalues into 20 batches
    convert_fractions_udf = convert_fractions_udf_wrapper(convert_to_float=True)
    df_to_numeric_quantities = df_padded.withColumns(
        {"RecipeIngredientQuantities" : convert_fractions_udf(df_padded.RecipeIngredientQuantities),
         "batchID" : col("RecipeID") % 20})
    #print(df_to_numeric_quantities.select(col("batchID")).distinct().count())

    #blank element to collect batches
    df_batches = []

    #creates array of the 20 batchIDs
    #iterates through "batchID" to process the data in batches. 
    for batchNum in df_to_numeric_quantities.select(col("batchID")).distinct().collect():
        #sets current dataframe to current batch in iterator
        batch_group = df_to_numeric_quantities.filter(col("batchID") == batchNum[0]) 

        #breaks apart ingredient arrays so each ingredient is on its own line
        #gives each line a rowID
        df_exploded_I = batch_group.select(
            col("RecipeID"),
            col("Name"), 
            F.explode(col("RecipeIngredientParts")).alias("Ingredient")).withColumn('RowID', F.monotonically_increasing_id())

        #same as above but for quantity
        df_exploded_Q = batch_group.select(
            col("RecipeID"),
            col("Name"), 
            F.explode(col("RecipeIngredientQuantities")).alias("Quantity")).withColumn('RowID', F.monotonically_increasing_id())

        #joins ingredient and quantity df based on rowID
        #groups data by ingredient. puts RecipeID, name, and quantity as a list of lists [{R, N, Q}, {R, N, Q}, {R, N, Q}, ...]
        df_QI = df_exploded_I.join(
            df_exploded_Q, [df_exploded_I.RowID == df_exploded_Q.RowID]).groupBy(df_exploded_I.Ingredient).agg(
            F.collect_list(
                struct(
                    df_exploded_I.RecipeID,
                    df_exploded_I.Name,
                    df_exploded_Q.Quantity).alias("Recipes")
                    )
            )

        #appends each batch to df_batches
        df_batches.append(df_QI)       
    
    
    #unions each dataframe batch in list. 
    df_JSON_Ready = df_batches[0] #starts structure
    for batch in df_batches[1:]: 
        df_JSON_Ready = df_JSON_Ready.union(batch)
    
    print(df_JSON_Ready.show(5))
    
    #transforms to JSON format
    #json_list = df_JSON_Ready.limit(10).toJSON().collect()

    #put dataframe into readable format to check
    # for i, json_str in enumerate(json_list):
    #     parsed_json = json.loads(json_str)
    #     print(f"\nConverted row {i + 1}:")
    #     print(json.dumps(parsed_json, indent=4))





    
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
