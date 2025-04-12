def load_test_queries(query_set_name):
    if query_set_name == "example_set":
        test_queries = [
        {"$and": [{"IngredientName": 'onion' }, {"Quantity": { "$lt": 1.5 } }]}
        
    ]
        
def run_test_queries(queries, collection):
    for i, query in enumerate(queries):
        results = list(collection.find(query))
        print(f"Query {i+1}: {query} -> {len(results)} results found")

# run_test_queries(test_queries)

# https://stackoverflow.com/questions/25586901/how-to-find-document-and-single-subdocument-matching-given-criterias-in-mongodb

# https://www.mongodb.com/community/forums/t/mongodb-query-to-filter-documents-and-subdocuments/205536

# https://www.mongodb.com/community/forums/t/search-aggragation-elemmatches-equivalent/151220/6
# https://www.mongodb.com/docs/atlas/atlas-search/embedded-document/