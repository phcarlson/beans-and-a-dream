# Potential Additions: add recipe search functionality

class RecipeList():
    '''A class that takes keep track of the user's recipe search result as an array of Dictionaries'''
    def __init__(self):
        '''initialize the class'''
        self.recipes = []

    def getRecipes(self) -> list[any]:
        '''Returns the current IngredientList of the user'''
        return self.recipes
    
    def updateRecipes(self, new_recipes: list[any]) -> None:
        '''updates the recipe list with a new recipe list'''
        self.recipes = new_recipes
         
    def __str__(self):
        '''creates print string for the Recipe List'''
        out = "\nRecipes retrieved from most recent search:"
        for recipe in self.recipes:
            out += "\tName: " + recipe["Name"] + ", Calories: " + str(recipe["Calories"]) + "\n"
            out += "\tDescription: " + recipe["Description"] + "\n"
            out += "\tIngredients:" + "\n"
            for ingredient in recipe["Ingredients"]:
                out += "\t\tName: " + ingredient["IngredientName"] + ", Quantity: " + str(ingredient["Quantity"]) + "\n"
        return out + "\n"