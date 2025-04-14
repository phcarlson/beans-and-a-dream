import os
from database import DBClient, load_test_queries, create_reg_index
import asyncio
import pandas as pd
import time
import numpy as np 
from datetime import datetime

""" This module/script iterates over a variety of index designs to test for the MongoDB Atlas database queries, 
by measuring queries per second (QPS) and storing the outputs to a csv for downstream analysis. """

async def keep_querying(e, collection, counter_lock, counter, latencies, worker_id):
    while not e.is_set():
        query = build_query(worker_id)
        
        start_time = time.time()
        result = await collection.find_one(query)
        latency = time.time() - start_time 
        
        if result:
            async with counter_lock:
                counter[0] += 1
            latencies.append(latency)  
      
async def benchmark_curr_db_indexes(collection, duration_of_test=10, max_workers=20):
    stop_event = asyncio.Event() 
    counter_lock = asyncio.Lock()  
    success_counter = [0]
    latencies = []
    # The moment create_task is called, it starts running the async querying 
    tasks = []
    for i in range(max_workers):
        task = asyncio.create_task(keep_querying(stop_event, collection, counter_lock, success_counter, latencies, i))
        tasks.append(task)

    # Wait to gather results before flagging to stop
    await asyncio.sleep(duration_of_test)
    stop_event.set()  

    analytics = {
        "Duration": duration_of_test,
        "QPS": success_counter[0] / duration_of_test,
        "TotalSuccessfulQueries": success_counter[0],
        "AvgLatency": np.mean(latencies),
        "MaxLatency": np.max(latencies),
        "MinLatency": np.min(latencies),
        "MedianLatency": np.median(latencies),
        "MaxWorkers": max_workers
    }

    print(f"Duration: {duration_of_test}s")
    print(f"Total successful queries: {analytics['TotalSuccessfulQueries']}")
    print(f"QPS: {analytics['QPS']} (across {max_workers} max workers)")
    print(f"Average latency: {analytics['AvgLatency']}")
    print(f"Max latency: {analytics['MaxLatency']}")
    print(f"Min latency: {analytics['MinLatency']}")
    print(f"Median latency: {analytics['MedianLatency']}")
    
    return analytics

def build_query(worker_id):
    # Varies the 'Quantity' slightly per worker to prevent caching
    # return {
    #     "Ingredients": {
    #         "$elemMatch": {
    #             "IngredientName": "onion",
    #             "Quantity": {"$lte": 1.5 + (worker_id % 5) * 0.1}
    #         }
    #     }
    # }

    return {
        "Ingredients": {
            "$all": [
                {
                    "$elemMatch": {
                        "IngredientName": "onion",
                        "Quantity": {"$lte": 3.5 + (worker_id % 5) * 0.1}
                    }
                },
                {
                    "$elemMatch": {
                        "IngredientName": "olive oil",
                        "Quantity": {"$lte": 0.75 + (worker_id % 3) * 0.1}
                    }
                },
                 {
                    "$elemMatch": {
                        "IngredientName": "garlic",
                        "Quantity": {"$lte": 1}
                    }
                }
            ]
        }
    }

async def run_reg_index_benchmarks(duration_of_test=10):
    # Create the client ONCE for the experimental session
    client = DBClient()
    database_name = "test_db_ranges"
    collection_name = "test_recipes_range_support"
    db = await client.get_database(database_name)
    collection = db[collection_name]

    # First make a query to form connection if not already formed 
    await collection.find_one()

    # Path to all of our test reg indexes
    path_to_search_indexes = "src/database/reg_indexes_to_try"

    # Iterate over each possible one in the folder to set up and run the benchmark
    qps_results = []
    for index_file_name in [file for file in os.listdir(path_to_search_indexes) if file.endswith('.json')]:
        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        print("DROPPING INDEXES FOR A CLEAN SLATE....")
        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        await collection.drop_indexes()
        
        indexes_made = 0
        async for index in await collection.list_indexes():
            indexes_made += 1

        if indexes_made != 1:
            print(f"Dropping all extra indexes failed for some reason. Cannot perform test for indexes in {index_file_name}") 
            # Skip to next index if this one failed
            continue  
        else:
            index_name = os.path.splitext(index_file_name)[0]
            succeeded = await create_reg_index(client=client, 
                                               collection=collection,
                                               index_name=index_name, 
                                               setup_instance=None, 
                                               setup_file_name=index_file_name)
            if succeeded:

                print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
                print("RUNNING QPS....")
                print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

                for max_workers in [5, 10, 20, 40, 80]:
                    qps_result = await benchmark_curr_db_indexes(collection, duration_of_test, max_workers)
                    qps_result["IndexSetupFileName"] = index_name
                    qps_results.append(qps_result)


    df = pd.DataFrame(qps_results)
    benchmark_file_name = f"reg_benchmark_results_{datetime.now()}.csv"
    df.to_csv(benchmark_file_name, index=False)
    print(f"\nSaved results to '{benchmark_file_name}'")
    # await client.close_connection()
    
async def main():   
    duration_of_test = 20
    await run_reg_index_benchmarks(duration_of_test)

if __name__ == "__main__":
    asyncio.run(main())
