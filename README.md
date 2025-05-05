# Beans and a Dream
- Staring into your pantry, all you've got is one can of beans, a half cup of rice, and a dream. What can you make? 
- Beans and a Dream aims to be a scalable, ingredient quantity-based recipe search system built with MongoDB Atlas Search and PySpark. Indexes 500k+ recipe documents to search against the exact ingredient amounts you have. A WIP, pending system analyses/benchmarking.

# Data Source
- The dataset we used is found here on Kaggle: https://www.kaggle.com/datasets/irkaal/foodcom-recipes-and-reviews/data
- It is under License CC0: Public Domain, so we are free to use it in any way.

# Setting Up Your Environment

1. Clone the repo.
2. Download the recipes.parquet from the Kaggle link above and add it to the data/raw directory. DO NOT DOWNLOAD THE CSV! The parquet preserves metadata about the different types and makes it easier to cleanly load into a PySpark dataframe. 
3. Create a virtual environment for Python ____. 
    - I use Miniconda3 as a way to install conda, which is used for package/environ management (https://www.anaconda.com/docs/getting-started/miniconda/install). 
    - Once this is installed, at least for Ubuntu with a Bash/Zsh shell, I edit the startup configuration file for the shell with the line "conda deactivate" after the conda initialize code block.
    - Store your Mongo creds in a safe enough place given the project size/scope.
4. Activate the virtual environment and do "pip install -r requirements.txt" to set up the dependencies.
5. (Optional, for development) Add extension Todo Tree in VSCode to easily navigate what needs to be done in the code itself.
    - In VSCode user settings JSON file, edit it like so: 
        ``` 
        {
            "workbench.colorTheme": "Default Dark Modern",
            "explorer.confirmDragAndDrop": false,
            "notebook.lineNumbers": "on",
            "todo-tree.highlights.customHighlight": {
                "TODO": {
                    "type": "line",
                    "iconColour": "#f1d257",
                    "foreground": "black",
                    "gutterIcon": true,
                    "background": "#f1d257",
                }
            }
        } 
         ```
    - This way, when you place comments with with TODO in them, it will highlight the entire comment line with a soft pale yellow to make it easy for others to see without being too annoying. Or customize it yourself: https://marketplace.visualstudio.com/items?itemName=Gruntfuggly.todo-tree
    - Then you can click the tree icon on the side bar to see exactly where TODOs were maded in different folders and whatnot.

# How To Run Database and Index Creation
- Set up account with MongoDB Atlas and store necessary connection string credentials in safe place to retrieve for project's scope, like env variables
- Add your/path/to/beans-and-a-dream/src to the Python sys path for importing the folders as modules, as they are under development 
- Run src/preprocessing/row_to_document_structure_1.py to load the 500k+ recipes using with your own database_name, collection_name in main func and increase batch size as much as it can without slowing (say, 10k per batch)
- Once recipes are loaded, use regular_index.py or search_index.py to set up an index using with your own database_name, collection_name based on what querying type you prefer

# How To Run Experiments
- For experiments, utilize database/database_test_utils or database/test_utils_atlas to create randomly specified reg queries or search queries for experiments
- Run database/benchmark_reg_index.py, database/benchmark_search_index.py, database/cache_test.py after passing in the test query csvs you have created
- Pass the resulting csvs into database/plot_benchmarks.py and comment out the log scale if you prefer reg scale