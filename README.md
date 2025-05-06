# Beans and a Dream
 *A scalable, quantity-aware recipe search system leveraging PySpark and MongoDB Atlas Search.*

# Project Overview
- Staring into your pantry, all you've got is one can of beans, a half cup of rice, and a dream. What can you make? 
- Beans and a Dream aims to be a scalable, ingredient quantity-based recipe search system built with MongoDB Atlas Search and PySpark. Indexes 500k+ recipe documents to search against the exact ingredient amounts you have. A WIP, pending system analyses/benchmarking.

# Data Source
- Dataset: [Food.com Recipes and Reviews on Kaggle](https://www.kaggle.com/datasets/irkaal/foodcom-recipes-and-reviews/data)  
- Format: Use the `.parquet` file (not CSV) to preserve metadata and ensure clean PySpark loading.
- License: CC0 (Public Domain)

# Project Structure

    beans-and-a-dream/
    │
    ├── data/
    │ └── raw/ # Place recipes.parquet here
    │
    ├── src/
    │ ├── preprocessing/ # PySpark-based document construction
    │ │ ├── row_to_document_structure_1.py
    │ │ └── recipe_cleaning_utils.py
    │ │
    │ ├── database/ # MongoDB index creation, benchmarking, querying
    │ │ ├── regular_index.py
    │ │ ├── search_index.py
    │ │ ├── benchmark_reg_index.py
    │ │ ├── benchmark_search_index.py
    │ │ ├── cache_test.py
    │ │ ├── plot_benchmarks.py
    │ │ ├── database_test_utils.py
    │ │ └── test_utils_atlas.py
    │ │
    │ ├── app/ # Lightweight frontend (FastAPI)
    │ │ ├── templates/
    │ │ ├── static/
    │ │ ├── main.py
    │ │ └── ...
    │ │
    │ └── utils/ # Shared helpers (optional / in-dev)
    │
    ├── requirements.txt
    └── README.md

    
# Setting Up Your Environment

1. Clone the repository.
2. Download `recipes.parquet` from Kaggle and place it in `data/raw/`.  
   ⚠️ **Do not use the CSV version** — the Parquet format preserves column types and metadata.
3. Create a Python virtual environment (e.g., using conda or venv):
    ```bash
    conda create -n beans python=3.9
    conda activate beans
    ```
4. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
5. (Optional) If using VSCode, install the **Todo Tree** extension to highlight development tasks. You can add this to your `settings.json`:
    ```json
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
          "background": "#f1d257"
        }
      }
    }
    ```
    - This way, when you place comments with TODO in them, it will highlight the entire comment line with a soft pale yellow to make it easy for others to see without being too annoying. Or customize it yourself: https://marketplace.visualstudio.com/items?itemName=Gruntfuggly.todo-tree
    - Then you can click the tree icon on the side bar to see exactly where TODOs were maded in different folders and whatnot.

# How To Run Database and Index Creation

1. Set up a MongoDB Atlas account and cluster.
2. Use the provided test user credentials** or your own:
    ```bash
    export MONGO_RECIPE_USER=guest_user
    export MONGO_RECIPE_PW=secretpassw0rd
    ```
3. Add the project to your Python path:
    ```bash
    export PYTHONPATH=$PYTHONPATH:/path/to/beans-and-a-dream/src
    ```
4. Load recipes into MongoDB:
    ```bash
    python src/preprocessing/row_to_document_structure_1.py
    ```
    - Modify `database_name`, `collection_name`, and `batch_size` (recommended: 10,000) inside the script.

5. Create an index:
    - For basic querying:
        ```bash
        python src/database/regular_index.py
        ```
    - For Atlas Search (recommended):
        ```bash
        python src/database/search_index.py
        ```

# How to Run the Frontend
- To run the web interface:
    ```bash
    cd src/app
    python -m main
    ```
- Then open your browser and go to: http://localhost:8000/adjustIngredients
- This lets you input ingredients and quantities, and returns matching recipes from your MongoDB collection.

# How To Run Experiments
1. **Generate query CSVs**  
Use helper functions to generate random test queries:
- For regular index:
     ```python
     from database.database_test_utils import generate_random_queries
     ```
- For Atlas Search:
     ```python
     from database.test_utils_atlas import generate_random_search_queries
     ```

2. **Run benchmark scripts**  
   These scripts benchmark query response times across different configurations:
   ```bash
   python src/database/benchmark_reg_index.py
   python src/database/benchmark_search_index.py
   ```

# Caching Tests
- To test repeated query performance:
    ```bash
    python src/database/cache_test.py
    ```
