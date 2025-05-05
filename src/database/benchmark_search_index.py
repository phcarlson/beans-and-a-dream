import asyncio
import time
import pandas as pd
import numpy as np
from datetime import datetime
from asyncio import Queue
from tqdm.asyncio import tqdm
import os
from ast import literal_eval 

import matplotlib.pyplot as plt
import seaborn as sns

from database import DBClient, create_reg_index, csv_query_reader, create_search_index, atlas_search_creator

""" This module/script iterates over a variety of index designs to test for the MongoDB Atlas database queries,
by measuring queries per second (QPS) and storing the outputs to a csv for downstream analysis. """

# Per https://docs.python.org/3/library/asyncio-queue.html
async def keep_querying(collection, counter_lock, counter, latencies, query_queue):

    while True:

        query = await query_queue.get()
        
        started_at = time.monotonic()
        
        # To actually activate the query because it is done lazily, iterate over results
        results = await collection.aggregate(query)
        list_of_results = await results.to_list()
        latency = time.monotonic() - started_at

        query_queue.task_done()

        if list_of_results != None:
            async with counter_lock:
                counter[0] += 1
            latencies.append(latency)

async def benchmark_curr_db_indexes(db, collection, test_queries, max_workers=20):
    try:
        counter_lock = asyncio.Lock()
        success_counter = [0]
        latencies = []

        # Wipe the cache so it doesn't cheat from prev runs
        await db.command(
        {
            "planCacheClear": "test_recipes_range_support"
        }
    )

        query_queue = asyncio.Queue()

        # Add queries to the queue
        for query in test_queries:
            query_queue.put_nowait(query)

        # Create worker tasks
        tasks = []
        for i in range(max_workers):
            task = asyncio.create_task(
                keep_querying(collection, counter_lock, success_counter, latencies, query_queue))
            tasks.append(task)
    
        print(datetime.now())
        started_at = time.monotonic()
        print(started_at)
        await query_queue.join()
        ended_at = time.monotonic()
        total_duration = ended_at - started_at
        print(ended_at)
        print(datetime.now())

        for task in tasks:
            task.cancel()

        # Wait until all worker tasks are cancelled
        await asyncio.gather(*tasks, return_exceptions=True)

        # Gather and print analytics
        analytics = {
            "Duration": total_duration,
            "QPS": success_counter[0] / total_duration,
            "TotalSuccessfulQueries": success_counter[0],
            "AvgLatency": np.mean(latencies),
            "MaxLatency": np.max(latencies),
            "MinLatency": np.min(latencies),
            "MedianLatency": np.median(latencies),
            "MaxWorkers": max_workers
        }

        print(f"Duration: {analytics['Duration']:.2f}s")
        print(f"Total successful queries: {analytics['TotalSuccessfulQueries']}")
        print(f"QPS: {analytics['QPS']:.2f} (across {max_workers} workers)")
        print(f"Average latency: {analytics['AvgLatency']:.4f}s")
        print(f"Max latency: {analytics['MaxLatency']:.4f}s")
        print(f"Min latency: {analytics['MinLatency']:.4f}s")
        print(f"Median latency: {analytics['MedianLatency']:.4f}s")

        return analytics
    
    except Exception as e:
        print(f"Error: {e}")
        raise


async def run_search_index_benchmark(test_query_file_name, index_file_name):
    # Create the client ONCE for the experimental session
    client = DBClient()
    database_name = "test_db_ranges"
    collection_name = "test_recipes_range_support"
    db = await client.get_database(database_name)
    collection = db[collection_name]

    # First make a query to form connection if not already formed 
    await collection.find_one()

    # Iterate over each possible one in the folder to set up and run the benchmark
    qps_results = []
  
    index_name = os.path.splitext(index_file_name)[0]
    succeeded = await create_search_index(
                                    collection=collection,
                                    index_name=index_name,
                                    search_index_model_instance=None,
                                    search_index_model_file_name=index_file_name)

    test_queries_df = pd.read_csv(test_query_file_name)
    test_queries_df["Search"] = test_queries_df["Search"].apply(literal_eval)
    test_queries = test_queries_df["Search"].tolist()
    if succeeded:
        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        print("RUNNING QPS....")
        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

        for max_workers in [5, 10, 20, 40, 80, 160, 320, 640, 1280, 2560]:
            qps_result = await benchmark_curr_db_indexes(db, collection, test_queries, max_workers)
            qps_result["IndexSetupFileName"] = index_name
            qps_results.append(qps_result)

            df = pd.DataFrame(qps_results)
            benchmark_file_name = f"partial_random_search_benchmark_results_{datetime.now()}.csv"
            df.to_csv(benchmark_file_name, index=False)

    # Save results to CSV
        benchmark_file_name = f"search_benchmark_result_{index_file_name}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
        df = pd.DataFrame(qps_results)
        df.to_csv(benchmark_file_name, index=False)
        print(f"\nSaved results to '{benchmark_file_name}'")
        await client.close_connection()

async def main():
    # test_query_file_name = "final_complex_reg_queries.csv"
    test_query_file_name = "random_dynamic.csv"

    # File to benchmark adjusted manually due to the delay in constructing a large search index
    await run_search_index_benchmark(test_query_file_name, 'dynamic.json')

if __name__ == "__main__":
    asyncio.run(main())
