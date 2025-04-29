import asyncio
import time
import pandas as pd
import numpy as np
from datetime import datetime
from asyncio import Queue
from tqdm.asyncio import tqdm
import os

import matplotlib.pyplot as plt
import seaborn as sns

from database import DBClient, create_reg_index, csv_query_reader

""" This module/script iterates over a variety of index designs to test for the MongoDB Atlas database queries,
by measuring queries per second (QPS) and storing the outputs to a csv for downstream analysis. """

# Per https://docs.python.org/3/library/asyncio-queue.html
async def keep_querying(collection, counter_lock, counter, latencies, query_queue):
    while True:
        query = await query_queue.get()

        started_at = time.monotonic()
        
        # To actually activate the query because it is done lazily, iterate over results
        cursor = collection.find(query).limit(10)
        count = 0
        async for _ in cursor:
            count += 1  # Count the documents
        latency = time.monotonic() - started_at

        query_queue.task_done()

        if count != None:
            async with counter_lock:
                counter[0] += 1
            latencies.append(latency)

async def benchmark_curr_db_indexes(database, collection, test_queries, max_workers=20):
    counter_lock = asyncio.Lock()
    success_counter = [0]
    latencies = []

  # Wipe the cache so it doesn't cheat from prev runs
    await database.command(
    {
        "planCacheClear": collection.name
    })

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
   
    started_at = time.monotonic()
    print(datetime.now())
    await query_queue.join()
    ended_at = time.monotonic()
    total_duration = ended_at - started_at
    print(ended_at)
    print(datetime.now())

    for task in tasks:
        task.cancel()

    # Wait until all worker tasks are cancelled.
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


async def run_reg_index_benchmarks(test_query_file_name):
    # Create the client ONCE for the experimental session
    client = DBClient()
    database_name = "test_db_ranges"
    collection_name = "test_recipes_range_support"
    db = await client.get_database(database_name)
    collection = db[collection_name]

    # Load up the test queries
    test_queries_df = await csv_query_reader(collection, test_query_file_name)
    test_queries = test_queries_df["Query"].tolist()
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

                for max_workers in [5, 10, 20, 40, 80, 160, 320, 640, 1280, 2560]:
                    qps_result = await benchmark_curr_db_indexes(db, collection, test_queries, max_workers)
                    qps_result["IndexSetupFileName"] = index_name
                    qps_results.append(qps_result)
                    df = pd.DataFrame(qps_results)
                    benchmark_file_name = f"partial_medium_reg_benchmark_results_{datetime.now()}.csv"
                    df.to_csv(benchmark_file_name, index=False)

    df = pd.DataFrame(qps_results)
    benchmark_file_name = f"reg_benchmark_results_{datetime.now()}.csv"
    df.to_csv(benchmark_file_name, index=False)
    print(f"\nSaved results to '{benchmark_file_name}'")
    await client.close_connection()

async def main():
    test_query_file_name = "src/database/official_testing_criteria/simple_leq_queries.csv"

    await run_reg_index_benchmarks(test_query_file_name)

if __name__ == "__main__":
    asyncio.run(main())
