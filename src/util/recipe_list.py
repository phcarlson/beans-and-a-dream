# Potential Additions: add recipe search functionality

class RecipeList():
    '''A class that takes keep track of the user's recipe search result as an array of Dictionaries'''
    def __init__(self):
        '''initialize the class'''
        self.recipes = []

    def getRecipes(self) -> list[any]:
        '''Returns the current IngredientList of the user'''
        return self.recipes
    
    async def updateRecipes(self, new_recipes: list[any]) -> None:
        '''updates the recipe list with a new recipe list (updated for ascyncio)'''
        self.recipes = await new_recipes.to_list()

    def to_JSON(self) -> object:
        recipe_obj_list = []
        for recipe in self.recipes:

            recipe_ing_list = []
            for ingredient in recipe["Ingredients"]:
                recipe_ing_list.append({
                    "ingredient": ingredient["IngredientName"],
                    "quantity": str(ingredient["Quantity"])
                })

            recipe_inst_list = []
            for instruction in recipe["RecipeInstructions"]:
                recipe_inst_list.append(instruction)

            recipe_obj_list.append({
                "name": recipe["Name"],
                "calories": recipe["Calories"],
                "desc": recipe["Description"],
                "ingredients": recipe_ing_list,
                "instructions": recipe_inst_list
            })

        return {
            "recipes": recipe_obj_list
        }
         
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