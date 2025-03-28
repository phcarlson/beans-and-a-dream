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