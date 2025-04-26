# Potential Additions: add a function to load an ingredientList from a JSON
from util import Ingredient

class IngredientList():
    '''A class that takes keep track of the user's ingredient list as an array of Dictionaries'''
    def __init__(self):
        '''initialize the class with an empty ingredient list'''
        self.ingredients: list[Ingredient] = []

    def __init__(self, ):
        '''initialize the class with an empty ingredient list'''
        self.ingredients: list[Ingredient] = []
        
    def getIngredients(self) -> list[Ingredient]:
        '''Returns the current IngredientList of the user'''
        return self.ingredients
    
    def getIngredientbyIndex(self, index: int) -> Ingredient:
        '''Returns the ingredient at a certain index'''
        if index == -1: return None
        elif index < 0 or index >= len(self.ingredients): raise Exception("Invalid index provided when trying to get ingredient")
        else: return self.ingredients[index]
    
    def getIngredientbyName(self, name: str) -> Ingredient:
        '''Returns the ingredient by ingredient name'''
        return self.getIngredientbyIndex(self.getIngredientIndexbyName(name))
    
    def getIngredientIndexbyName(self, name: str) -> int:
        '''Returns the index of the ingredient in ingredientList by ingredient name (-1 if not in list)'''
        return next((i for i, item in enumerate(self.ingredients) if item.getIngredientName() == name), -1)
    
    def changeIngredientType(self, name: str, isRanged: bool = None) -> None:
        '''Changes the isRanged status of an ingredient to the isRanged parameter. If isRanged is None then just switches from t <-> f'''
        self.getIngredientbyName(name).changeisRanged(isRanged)
    
    def addIngredientExact(self, name: str, quantity: int|float) -> None:
        '''adds a specific quantity of an Ingredient to the users IngredientList- adds a new ingredient if it doesn't exist in the list'''
        index = self.getIngredientIndexbyName(name)
        if index == -1:
            self.ingredients.append(Ingredient(name, False, [quantity, None, None]))
        else:
            self.getIngredientbyIndex(index).increaseQuantityExact(quantity)
    
    def removeIngredientExact(self, name: str, quantity: int|float = -1) -> None:
        '''removes some amount of an Ingredient from the users IngredientList- defaults to removing entire ingredient'''
        index = self.getIngredientIndexbyName(name)
        if index == -1:
            raise Exception("Ingredient trying to being removed does not exist!")
        else:
            if (quantity >= self.getIngredientbyIndex(index).getRelevantQuantities()[0] or quantity == -1):
                self.removeIngredient(name)
            else:
                self.ingredients[index].decreaseQuantityExact(quantity)

    def addIngredientRanged(self, name: str, lower_bound: int|float, upper_bound: int|float) -> None:
        '''adds a ranged variant of an Ingredient to the users IngredientList'''
        if lower_bound > upper_bound or lower_bound < 0 or upper_bound < 0:
            raise Exception("Invalid bounds!")
        self.ingredients.append(Ingredient(name, True, [None, lower_bound, upper_bound]))

    def updateIngredientRange(self, name: str, newLower: int|float, newUpper: int|float) -> None:
        '''updates the range of an ingredient'''
        if newLower > newUpper or newLower < 0 or newUpper < 0:
            raise Exception("Invalid bounds!")
        index = self.getIngredientIndexbyName(name)
        if index == -1:
            raise Exception("Ingredient trying to be updated does not exist!")
        else:
            self.getIngredientbyIndex(index).updateQuantityRange(newLower, newUpper)
    
    def removeIngredient(self, name: str) -> None:
        '''removes an ingredient with the given name'''
        index = self.getIngredientIndexbyName(name)
        if index == -1:
            raise Exception("Ingredient trying to be updated does not exist!")
        else:
            self.ingredients.pop(index)
        
    def __str__(self):
        '''creates print string for the Ingredient List''' 
        out = "Current Ingredient List:\n"
        for ingredient in self.ingredients:
            if ingredient.isRanged():
                out += "\tName: " + ingredient.getIngredientName() + ", Lower Bound: " + str(ingredient.getRelevantQuantities()[0]) + ", Upper Bound: " + str(ingredient.getRelevantQuantities()[1]) + "\n"
            else:
                out += "\tName: " + ingredient.getIngredientName() + ", Quantity: " + str(ingredient.getRelevantQuantities()[0]) + "\n"
        return out