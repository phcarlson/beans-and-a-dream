
# For going from the database row to constructing the (predictable) table of recipe docs
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, DoubleType
from fractions import Fraction
import re

def convert_fractions_udf_wrapper(convert_to_float=True):
   return udf(lambda x: convert_fractions(x, convert_to_float), ArrayType(DoubleType()))

def convert_fractions(quantity_strs, convert_to_float):
    return [convert_fraction(quantity_str, convert_to_float) for quantity_str in quantity_strs]

def convert_fraction(quantity_str, convert_to_float):
    try:
        if quantity_str != None:
            # First get the frac slashes a consistent char
            quantity_str_consistent_slash = quantity_str.replace('⁄', '/').strip()
        
            if convert_to_float:
                # If there is an ingredient range specified
                if ' - ' in quantity_str:
                    # First get the vals from the range
                    quantity_str_consistent_slash = [p.strip() for p in quantity_str_consistent_slash.split(' - ')] 
                    # Keep only the parts that exist
                    quantity_str_consistent_slash = [p for p in quantity_str_consistent_slash if p]
                    # If none left, then we don't have a measurement
                    if len(quantity_str_consistent_slash) == 0:
                        return None 
                    # Otherwise, take the lower bound if exists because we are ummmm desperate with the amounts we have?
                    else:
                        quantity_str_consistent_slash = quantity_str_consistent_slash[0]

                # With whatever is left, which COULD be a mixed number, split that up on whitespace
                parts = re.split(r'\s+', quantity_str_consistent_slash)

                # If it a mixed number split it up into each part
                if len(parts) == 2:
                    whole_part, fraction_part = parts

                    # Convert it to actual numerical value to put back
                    fraction = Fraction(fraction_part)
                    whole_number = abs(float(whole_part))
                    numeric_value = whole_number + abs(float(fraction))

                    return numeric_value
                # Not mixed, simpler
                else:
                    fraction = Fraction(quantity_str_consistent_slash)
                    numeric_value = abs(float(fraction))

                    return numeric_value
            else:
                return quantity_str_consistent_slash
        else:
            return None
    # SOL...
    except Exception as e:
        print(e)
        return None  

# This is just AI generated code to extract a certain number of recipes from the big file to smaller file for lighter weight troubleshooting
# Produced by Google's Gemini integrated into Google search with the prompt "how to efficiently copy just up to a certain line from one file to another python"
def copy_up_to_line(source_file, destination_file, line_number):
    """
    Copies content from source_file to destination_file up to line_number.

    Args:
        source_file (str): Path to the source file.
        destination_file (str): Path to the destination file.
        line_number (int): The line number up to which content should be copied.
    """
    try:
        with open(source_file, 'r', encoding='utf-8') as infile, open(destination_file, 'w', encoding='utf-8') as outfile:
            for i, line in enumerate(infile, 1):
                if i <= line_number:
                    outfile.write(line)
                else:
                    break  # Stop copying after reaching the specified line
    except FileNotFoundError:
        print(f"Error: File not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

# # Example usage:
# source_file_path = 'C:/Users/phcar/Downloads/recipes.csv/recipes.csv'
# destination_file_path = 'data/raw/first_24500_recipes.csv'
# # Started at id=38 for some reason, so to get to 24500 recipes (25000 too big for GH)
# target_line_number = 34575

# copy_up_to_line(source_file_path, destination_file_path, target_line_number)