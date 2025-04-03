# For going from the database row to constructing the (predictable) table of recipe docs
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.types import ArrayType, DoubleType, StringType
from fractions import Fraction
import json
import os
import re
import time
# #TODO: Func to process a single row into the desired structure
#RecipeId,Name,AuthorId,AuthorName,CookTime,PrepTime,TotalTime,DatePublished,Description,Images,
# 
# RecipeCategory,Keywords,RecipeIngredientQuantities,RecipeIngredientParts,

# AggregatedRating,ReviewCount,

# Calories,FatContent,SaturatedFatContent,CholesterolContent,SodiumContent,CarbohydrateContent,FiberContent,SugarContent,ProteinContent,

# RecipeServings,RecipeYield,RecipeInstructions
def row_to_document(row):
    ingredients = row["RecipeIngredientParts"][2:-1] 
    quantities = row["RecipeIngredientQuantities"][2:-1] 
    ingredient_quantity_pairs = dict(zip(ingredients, quantities))

    document = ''
    return document

def convert_fractions_udf_wrapper(convert_to_float=True):
   return udf(lambda x: convert_fractions(x, convert_to_float), ArrayType(DoubleType()))

def convert_fractions(quantity_strs, convert_to_float=True):
    return [convert_fraction(quantity_str, convert_to_float) for quantity_str in quantity_strs]

def convert_fraction(quantity_str, convert_to_float=True):
    try:
        if quantity_str != None:
            # First get the frac slashes a consistent char
            quantity_str_consistent_slash = quantity_str.replace('⁄', '/').strip()
        
            if convert_to_float:

                 # If there was a range, take the lower bound if exists because we are ummmm desperate for ingredients?
                if ' - ' in quantity_str:
                    quantity_str_consistent_slash = [p.strip() for p in quantity_str_consistent_slash.split(' - ')] 
                    quantity_str_consistent_slash = [p for p in quantity_str_consistent_slash if p]  # Remove empty values
                    if len(quantity_str_consistent_slash) == 0:
                        return None  # If all parts are empty, return None
                    else:
                        quantity_str_consistent_slash = quantity_str_consistent_slash[0]
                parts = re.split(r'\s+', quantity_str_consistent_slash)

                # Next, if it a mixed number split it up
                if len(parts) == 2:
                    whole_part, fraction_part = parts

                    # Convert it to actual numerical value to put back
                    fraction = Fraction(fraction_part)
                    whole_number = abs(float(whole_part))
                    numeric_value = whole_number + abs(float(fraction))

                    return numeric_value
                # Not mixed
                else:
                    fraction = Fraction(quantity_str_consistent_slash)
                    numeric_value = abs(float(fraction))

                    # print("it worked:{}".format(numeric_value))
                    return numeric_value
            else:
                return quantity_str_consistent_slash
        else:
            return None
    # SOL...
    except Exception as e:
        print(e)
        return None  

def main():
    # Initialize Spark session
    user = os.environ.get("MONGO_RECIPE_USER", default=None)
    password = os.environ.get("MONGO_RECIPE_PW", default=None)
        
        # Create connection string based on creds
    uri = f'mongodb+srv://{user}:{password}@reverse-index.xkyk7ik.mongodb.net/?retryWrites=true&w=majority&appName=Reverse-Index'
    spark = (SparkSession.builder 
        .appName('CSV to MongoDB Atlas') 
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.1") 
        .config("spark.mongodb.read.connection.uri", uri) 
        .config("spark.mongodb.write.connection.uri", uri) 
        .getOrCreate())
    # spark.conf.set("spark.sql.debug.maxToStringFields", 10000)

    start_preprocessing_time = time.time()

    # Read in the big big recipes dataset
    df = spark.read.parquet('data/raw/recipes.parquet')

    df_length = df.count()
    print(f"Number of rows in the DataFrame at beginning: {df_length}")

    df_where_quantity_length_matches_part_length = df.filter( F.size(col('RecipeIngredientQuantities')) ==  F.size(col('RecipeIngredientParts')))
    df_length = df_where_quantity_length_matches_part_length.count()
    print(f"Number of rows in the DataFrame where quantity/parts match length: {df_length}")

    # print("BEFORE FRAC CONVERSION:")
    # print(df.show(n=3, truncate=False))


    # #TODO Show how the order matters in terms of speed for filtering first, then to numeric vs the other way around :)

    # Register the UDF
    # https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.udf.html
    convert_fractions_udf = convert_fractions_udf_wrapper(convert_to_float=True)
    df_to_numeric_quantities = df_where_quantity_length_matches_part_length.withColumn("RecipeIngredientQuantities", convert_fractions_udf(col("RecipeIngredientQuantities")))

    # df_filtered = df_to_numeric_quantities.filter(
    #     F.forall(col("RecipeIngredientQuantities"), lambda x: x > 0.0)
    # )

    print("AFTER FRAC CONVERSION:")
    df_length = df_to_numeric_quantities.count()
    print(f"Number of rows in the DataFrame where quantity/parts match length, after numeric conversion stuff: {df_length}")

    # Convert JSON to read and write, except this is single-line 
    # df_to_numeric_quantities.toJSON().coalesce(1).saveAsTextFile("data/processed/test.json")
    json_list = df_to_numeric_quantities.limit(10).toJSON().collect()

    print("our df length is ")
    # Turn them into prettier JSON by loading them back to a Python dict and reconstructing them as nicely indented and stuff
    for i, json_str in enumerate(json_list):
        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        parsed_json = json.loads(json_str)
        print(f"Converted row {i + 1}:")
        print(json.dumps(parsed_json, indent=4))


    # End preprocessing time
    end_preprocessing_time = time.time()
    elapsed_time = end_preprocessing_time - start_preprocessing_time
    print(f"Time taken for preprocessing: {elapsed_time} seconds")
    
    #https://www.mongodb.com/docs/spark-connector/current/batch-mode/batch-write/#std-label-batch-write-to-mongodb
    (df_to_numeric_quantities.write.format("mongodb")
     .mode("append")
     .option("database", "test_db")
     .option("collection", "test_recipes")
     .save())

    spark.stop()
  
if __name__ == "__main__":
    # arg parsing here n whatnot

    # start the program
    main()
