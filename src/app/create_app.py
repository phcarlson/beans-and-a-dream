"""
This package is for handling the SIMPLE, NOT THE MAIN POINT OF THE PROJ, web app stuff... or something. Flask?
https://flask.palletsprojects.com/en/stable/tutorial/
"""

import os

# if not working run: 
'''py -m pip install 'flask[async]'''

from flask import Flask, render_template, request, jsonify

from database import DBClient
from retrieval import IngredientAtlasSearch

async def create_app(test_config=None):
    #create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY='dev',
    )

    if test_config is None:
        #load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        #load the test config if passed in
        app.config.from_mapping(test_config)

    #ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    # create a search object
    client = DBClient()
    db = await client.get_database("test_db_ranges")
    search = IngredientAtlasSearch(client, db)

    #a simple page that says hello
    @app.route('/', methods = ["GET", "POST"])
    def hello():
        nav_str = "Welcome!"
        nav_str += "\nUse /adjustIngredients: to edit Ingredients"
        nav_str += "\n"
        return nav_str
    
    #display page
    @app.route('/adjustIngredients', methods = ["GET", "POST"])
    def home():
        exact = True if request.form.get("exact") == "Exact" else False
        if not request.form.get("ingredient") is None and request.form.get("ingredient") != '':
            search.updateIngredient(request.form.get("ingredient"), exact = exact, quantity = float(request.form.get("quantity")), lower_bound = float(request.form.get("lower")), upper_bound = float(request.form.get("upper")))
        return render_template("adjustIngredients.html")
    
    @app.route('/getIngredients')
    def get_ingredients():
        json_list = jsonify(search.getIngredientList().to_JSON())
        return json_list.json
    
    @app.route('/displaySearch')
    def display():
        return render_template("displayRecipes.html")
    
    @app.route('/getRecipes')
    async def get_recipes():
        search.generateQuery()
        await search.queryRecipes()
        recipe_list = jsonify(search.getRecipeList().to_JSON())
        return recipe_list.json

    return app