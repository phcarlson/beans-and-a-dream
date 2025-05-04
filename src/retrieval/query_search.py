# Backend interface for the actual PyMongo querying logic for Atlas Search
# https://www.mongodb.com/docs/atlas/atlas-search/tutorial/run-query/

import sys
sys.path.append('../')

from database import DBClient
import asyncio 
from util import IngredientList
from util import RecipeList
from util import Ingredient
    
class IngredientAtlasSearch:
    '''A class that takes in a range of ingredients and quantities from the user, and computes a query on the database using an embedded_document search index'''
    def __init__(self, client: DBClient, db: any, collection_name: str = "test_recipes_range_support", search_index: str = "ING-EMB-DYN", index_type: str = "embeddedDocument"):
        '''Initializes the class with the provided client, specified/default database, collection and index, and an empty Ingredient List'''
        self.client: DBClient = client
        self.db: str = db
        self.collection: str = collection_name
        self.search_index: str = search_index
        self.index_type: str = index_type
        self.ingredientList: IngredientList = IngredientList()
        self.recipeList: RecipeList = RecipeList()
        self.query: str = ""

    async def run_searchTest(self) -> None:
        '''Backend function to allow user to interact on the console to create ingredient searches'''
        user_quit = False
        print("Welcome to the query search tester!") 
        while (not user_quit):
            print("Please enter an option (enter q to quit):")
            self.printOptions()
            choice = input("")
            if choice == '1':
                print(self.ingredientList)
            elif choice == '2':
                self.adjustIngredients()
            elif choice == '3':
                await self.run_recipeSearch()
            elif choice == '4':
                print(self.recipeList)
            elif choice == 'q':
                user_quit = True
            else:
                print("This option does not exist!")

    def printOptions(self) -> None:
        '''prints the command list'''
        print("\t1: See ingredients")
        print("\t2: Adjust Ingredients")
        print("\t3: Run a recipe search")
        print("\t4: See retrieved recipes")
    
    def getQueryResults(self) -> str:
        return self.recipeList
    
    def getIngredientList(self) -> IngredientList:
        '''Returns the ingredientList of the search'''
        return self.ingredientList
    
    def setIngredientList(self, new_ingredient_list: IngredientList) -> None:
        '''Sets the ingredient list manually'''
        self.ingredientList = new_ingredient_list

    def getRecipeList(self) -> RecipeList:
        '''Returns the current IngredientList of the user'''
        return self.recipeList
    
    def getQuery(self) -> str:
        '''Returns the current query for the search'''
        return self.query
    
    def updateIngredient(self, ingredient_name, exact = True, quantity = 1, lower_bound = 0, upper_bound = 1):
        '''Updates the quantity/ranges for an ingredient (creates one if it doesn't exist)'''
        if exact:
            self.ingredientList.updateIngredientExact(ingredient_name, quantity)
        else:
            self.ingredientList.updateIngredientRange(ingredient_name, lower_bound, upper_bound)

    def adjustIngredients(self) -> None:
        '''Backend function to allow user to adjust ingredients'''
        user_quit = False
        while (not user_quit):
            ingredient = input("Which ingredient do you want to adjust? (enter i to get your ingredient list, or q to go back): ")
            if ingredient == 'q':
                user_quit = True
            elif ingredient == 'i':
                print(self.ingredientList)
            else:
                index = self.ingredientList.getIngredientIndexbyName(ingredient)
                if index == -1:
                    add_choice = input("This ingredient (" + ingredient + ") is not in your ingredient list! Do you want to add this ingredient (y/n)?: ")
                    if add_choice == 'y':
                        type_choice = input("Do you want to add an (e) exact amount, or (r) ranged amount?: ")
                        if type_choice == 'e':
                            quantity = input("Enter a quantity: ")
                            try:
                                self.ingredientList.addIngredientExact(ingredient, float(quantity))
                                print("Added " + str(quantity) + " " + ingredient + " sucessfully!")
                            except ValueError:
                                print("Invalid quantity!")
                        elif type_choice == 'r':
                            lower_bound = input("Enter a lower bound: ")
                            upper_bound = input("Enter an upper bound: ")
                            try:
                                self.ingredientList.addIngredientRanged(ingredient, float(lower_bound), float(upper_bound))
                                print("Added " + ingredient + " sucessfully!")
                            except ValueError:
                                print("Invalid lower and/or upper bound!")
                        else:
                            print("Invalid input!")
                else:
                    if self.ingredientList.getIngredientbyName(ingredient).isRanged():
                        adjustment_choice = input("Ingredient (" + ingredient + ") found! Do you want to change the range (c) or remove (r) this ingredient?: ")
                        if adjustment_choice == 'c':
                            lower_bound = input("Enter a new lower bound: ")
                            upper_bound = input("Enter a new upper bound: ")
                            try:
                                self.ingredientList.updateIngredientRange(ingredient, float(lower_bound), float(upper_bound))
                                print("Updated range for" + ingredient + " sucessfully!")
                            except ValueError:
                                print("Invalid lower and/or upper bound!")
                        elif adjustment_choice == 'r':
                                self.ingredientList.removeIngredient(ingredient)
                                print("Removed " + ingredient + " sucessfully!")
                        else:
                            print("Invalid input!")
                    else:
                        adjustment_choice = input("Ingredient (" + ingredient + ") found! Do you want to add to (a) or remove from (r) this ingredient?: ")
                        if adjustment_choice == 'a':
                            quantity = input("Enter a quantity to add: ")
                            try:
                                self.ingredientList.addIngredientExact(ingredient, float(quantity))
                                print("Added " + str(quantity) + " " + ingredient + " sucessfully!")
                            except ValueError:
                                print("Invalid quantity!")
                        elif adjustment_choice == 'r':
                            quantity = input("Enter a quantity to remove (enter r again to remove the ingredient entirely): ")
                            try:
                                self.ingredientList.removeIngredientExact(ingredient, float(quantity))
                                print("Removed " + str(quantity) + " " + ingredient + " sucessfully!")
                            except ValueError:
                                if quantity == 'r':
                                    self.ingredientList.removeIngredientExact(ingredient)
                                    print("Removed " + ingredient + " sucessfully!")
                        else:
                            print("Invalid input!")
   
    async def run_recipeSearch(self):
        '''runs the updateRecipes function using the main ingredient chosen by the user'''
        ingredient = input("Which ingredient do you want to use as the main ingredient? (enter d to use the default ingredient, i to get your ingredient list, or q to go back): ")
        if ingredient == 'q':
                return
        elif ingredient == 'i':
            print(self.ingredientList)
        else:
            index = self.ingredientList.getIngredientIndexbyName(ingredient)
            if index == -1 and ingredient != 'd':
                print("This ingredient (" + ingredient + ") is not in your ingredient list!")
                return
            enteredLimit = False
            while not enteredLimit:
                limit = input("How many recipes do you want to get? (press enter to default to 10): ")
                if not limit:
                    limit = 10
                    enteredLimit = True
                else:
                    try:
                        limit = float(limit)
                        enteredLimit = True
                    except ValueError:
                        print("Please enter a number!")
            if ingredient == 'd':
                self.generateQuery(limit = limit)
            else:
                self.generateQuery(mainIngredient = self.ingredientList.getIngredientbyIndex(index), limit = limit)

            await self.queryRecipes()
            print("Updated Recipes!")
            

    def generateQuery(self, mainIngredient = None, limit = 10) -> str:
        '''generates the query used by mongodb for this atlas_Search'''
        index = self.ingredientList.getIngredientIndexbyName(mainIngredient)
        if index == -1:
            mainIngredient = self.ingredientList.getIngredientbyIndex(0)
            index = 0
        else:
            mainIngredient = self.ingredientList.getIngredientbyIndex(index)

        otherIngredients = self.ingredientList.getIngredients().copy()
        otherIngredients.pop(index)

        mainIngredient_should, otherIngredients_shoulds = self.create_shoulds(mainIngredient, otherIngredients)

        self.query = [
            {
                "$search": {
                    "index": self.search_index,
                    self.index_type: {
                        "path": "Ingredients",
                        "operator": {
                            "compound": {
                                "must": [
                                    {
                                        "compound": {
                                            "must": [{
                                                "text": {
                                                    "path": "Ingredients.IngredientName",
                                                    "query": mainIngredient.getIngredientName()
                                                }
                                            }],
                                            "should": mainIngredient_should
                                        }
                                    }
                                ],
                                "should": otherIngredients_shoulds
                            }
                        },
                        "score": {
                            "embedded": {
                                "aggregate": "mean"
                            }
                        }
                    }
                }
            },
            {
                "$limit": limit
            }
        ]

        return self.query

    async def queryRecipes(self) -> RecipeList:
        '''returns a list of documents using the stored query'''    
        new_recipes = await self.db[self.collection].aggregate(self.query)
        await self.recipeList.updateRecipes(new_recipes)
        return self.recipeList

    def create_shoulds (self, mainIngredient: Ingredient, otherIngredients: list[Ingredient]) -> tuple[list[dict[str, dict[str, int|float]]], list]:
        '''constructs the should portions of the query dynamically depending on the number of ingredients'''
        otherIngredients_should = []

        if not mainIngredient.isRanged():
            mainIngredient_should = [{
                "equals": {
                    "path": "Ingredients.Quantity",
                    "value": mainIngredient.getRelevantQuantities()[0]
                }
            }]
            
        else:
            mainIngredient_should = [{
                "range": {
                    "path": "Ingredients.Quantity",
                    "gte": mainIngredient.getRelevantQuantities()[0],
                    "lte": mainIngredient.getRelevantQuantities()[1]
                }
            }]

        for otherIngredient in otherIngredients:
            if not otherIngredient.isRanged():
                otherIngredients_should.append({
                    "compound": {
                        "must": [{
                            "text": {
                                "path": "Ingredients.IngredientName",
                                "query": otherIngredient.getIngredientName()
                            }
                        }],
                        "should": [{
                            "equals": {
                                "path": "Ingredients.Quantity",
                                "value": otherIngredient.getRelevantQuantities()[0]
                            }
                        }]
                    }
                })
            else:
                otherIngredients_should.append({
                    "compound": {
                        "must": [{
                            "text": {
                                "path": "Ingredients.IngredientName",
                                "query": otherIngredient.getIngredientName()
                            }
                        }],
                        "should": [{
                            "range": {
                                "path": "Ingredients.Quantity",
                                "gte": otherIngredient.getRelevantQuantities()[0],
                                "lte": otherIngredient.getRelevantQuantities()[1]
                            }   
                        }]
                    }
                })
        
        return mainIngredient_should, otherIngredients_should
        

async def main():
    '''main function --> creates an ingredient atlas search and runs a testing func'''
    client = DBClient()
    database_name = "test_db_ranges"
    db = await client.get_database(database_name)
    ingredient_search = IngredientAtlasSearch(client, db)
    await ingredient_search.run_searchTest()

if __name__ == "__main__":
    asyncio.run(main())


'''
KEEP! Default search index used for reference (and in case of deletion/override in Mongo)
{
  "mappings": {
    "dynamic": true,
    "fields": {
      "Ingredients": {
        "dynamic": true,
        "type": "embeddedDocuments"
      }
    }
  },
  "storedSource": {
    "include": [
      "Ingredients.IngredientName",
      "Ingredients.Quantity"
    ]
  }
}
'''