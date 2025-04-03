# For going from the database row to constructing the (predictable) table of recipe docs
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, transform, lit, expr, when, concat

from database import get_database
import time
from fractions import Fraction
from pyspark.sql.functions import udf, col, explode
from pyspark.sql.types import ArrayType, DoubleType, StringType

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

def convert_fraction_udf_wrapper(convert_to_float=True):
   return udf(lambda x: convert_fractions(x, convert_to_float), StringType())

def convert_fractions(quantity_strs, convert_to_float=True):
    return [convert_fraction(quantity_str, convert_to_float) for quantity_str in quantity_strs]

def convert_fraction(quantity_str, convert_to_float=True):
    try:
        # First get the frac slashes a consistent char
        quantity_str_consistent_slash = quantity_str.replace('⁄', '/')

        if convert_to_float:
            # Next, if it a mixed number split it up
            if ' ' in quantity_str_consistent_slash:
                whole_part, fraction_part = quantity_str_consistent_slash.split(' ')

                # Convert it to actual numerical value to put back
                fraction = Fraction(fraction_part)
                whole_number = float(whole_part)
                numeric_value = whole_number + float(fraction)

                return numeric_value
            # Not mixed
            else:
                fraction = Fraction(quantity_str_consistent_slash)
                numeric_value = float(fraction)

                # print("it worked:{}".format(numeric_value))
                return numeric_value
        else:
            return quantity_str_consistent_slash
    # SOL...
    except Exception as e:
        print(e)
        return None  
    
def main():
    # Initialize Spark session
    spark = (SparkSession.builder 
        .appName('CSV to MongoDB Atlas') 
        .getOrCreate())
    
    start_preprocessing_time = time.time()

    # https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.udf.html
    # convert_fractions_udf = convert_fraction_udf_wrapper(convert_to_float=True)
    # Read in small sample
    df = spark.read.parquet('data/raw/recipes.parquet')
    print("BEFORE FRAC CONVERSION:")
    print(df.first())
    # df_to_numeric_quantities = df.withColumn(
    #     "RecipeIngredientQuantities", convert_fractions_udf("RecipeIngredientQuantities")
    #     )

#     # # print("JSON array:",json.dumps(json_array, indent=4))


    # Register the UDF
    append_ing_udf = udf(convert_fractions, ArrayType(StringType()))

    # Apply the UDF to transform the 'string_lists' column
    df_to_numeric_quantities = df.withColumn("RecipeIngredientQuantities", append_ing_udf(col("RecipeIngredientQuantities")))

    df_to_numeric_quantities.show(truncate=True) 

    print("AFTER FRAC CONVERSION:")

    json_dict = df_to_numeric_quantities.limit(1).toJSON().collect()[0]
    parsed_json = json.loads(json_dict) 

    print(json.dumps(parsed_json, indent=4)) 

    # End preprocessing time
    end_preprocessing_time = time.time()
    elapsed_time = end_preprocessing_time - start_preprocessing_time
    print(f"Time taken for preprocessing: {elapsed_time} seconds")
    spark.stop()
  
if __name__ == "__main__":
    # arg parsing here n whatnot

    # start the program
    main()
