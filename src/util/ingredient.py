class Ingredient():
    '''A class that defines an Ingredient: can be ranged or exact'''
    def __init__(self):
        '''initialize the class'''
        self.name: str = None
        self.is_ranged: bool = None
        self.exact: int|float = None
        self.upperBound: int|float = None
        self.lowerBound: int|float = None
    
    def __init__(self, name: str, is_ranged: bool, quantities: list[int|float]):
        '''initialize the class with a name, isRanged, and a list of 3 quantities (exact, lower bound, upper bound)'''
        if len(quantities) != 3:
            raise Exception("quantities must of length 3!")
        self.name: str = name
        self.is_ranged: bool = is_ranged
        self.exact: int|float = quantities[0]
        self.lowerBound: int|float = quantities[1]
        self.upperBound: int|float = quantities[2]
    
    def getIngredientName(self) -> str:
        '''see if the ingredient is ranged or exact'''
        return self.name
    
    def isRanged(self) -> bool:
        '''see if the ingredient is ranged or exact'''
        return self.is_ranged
    
    def changeisRanged(self, new_isRanged: bool = None) -> None:
        '''Changes the isRanged status of the ingredient to the provided parameter. If none provided then just switches from t <-> f'''
        if new_isRanged is None:
            self.is_ranged = not self.is_ranged
        else:
            self.is_ranged = new_isRanged
    
    def getRelevantQuantities(self) -> list[int|float]:
        '''
        get the relevant quantities of the incredient ->
        if exact: index 0 is value ->
        if ranged: index 0 is lower bound and index 1 is upper bound
        '''
        if self.is_ranged:
            return [self.lowerBound, self.upperBound]
        else:
            return [self.exact]
        
    def getQuantities(self) -> dict:
        '''
        get all the quantities of the incredient as a list
        [0]: exact
        [1]: lower bound
        [2]: upper bound
        '''
        return [self.exact, self.lowerBound, self.upperBound]
    
    def increaseQuantityExact(self, quantity: int|float) -> None:
        '''increase the exact quantity of the ingredient by a specific amount'''
        self.exact += quantity

    def decreaseQuantityExact(self, quantity: int|float) -> None:
        '''decrease the exact quantity of the ingredient by a specific amount -> throws exception if there is no more ingredient remaining'''
        if self.exact - quantity <= 0:
            raise Exception("Cannnot remove that amount of quantity!")
        else:
            self.exact -= quantity

    def updateQuantityExact(self, quantity: int|float) -> None:
        '''set the exact quantity of the ingredient by a specific amount'''
        self.exact = quantity

    def updateQuantityRange(self, newLower: int|float, newUpper: int|float) -> None:
        '''update the range quantities of the ingredient by a specific amount'''
        self.lowerBound = newLower
        self.upperBound = newUpper