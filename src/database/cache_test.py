import asyncio
import time
import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns

from database import DBClient, csv_query_reader

async def keep_querying(collection, counter, latencies, query_queue):
    while True:
        query = await query_queue.get()

        started_at = time.monotonic()
        
        # To actually activate the query because it is done lazily, iterate over results
        cursor = collection.find(query).limit(10)
        count = 0
        async for _ in cursor:
            count += 1
        latency = time.monotonic() - started_at

        query_queue.task_done()

        if count != None:
            counter[0] += 1
            latencies.append(latency)

async def load_to_queue_and_run_once(collection, test_queries, latencies, success_counter, num_workers=20):
    # Just like benchmark reg index, puts queries in the queue, creates the tasks, waits for all to finish before cancelling them
    query_queue = asyncio.Queue()

    # Add queries to the queue
    for query in test_queries:
        query_queue.put_nowait(query)

    # Create worker tasks
    tasks = []
    for i in range(num_workers):
        task = asyncio.create_task(
            keep_querying(collection, success_counter, latencies, query_queue))
        tasks.append(task)
   
    print(datetime.now())
    await query_queue.join()
    print(datetime.now())

    for task in tasks:
        task.cancel()

    # Wait until all worker tasks are cancelled.
    await asyncio.gather(*tasks, return_exceptions=True)

async def load_to_queue_and_run_repeatedly(collection, queries, repetitions):
    ''' Loads queries up, keep querying num repetition times and then combine latency results'''
    all_latencies = []
    for _ in range(repetitions):
        latencies = []
        success_counter = [0]
        await load_to_queue_and_run_once(collection, queries, latencies, success_counter)
        all_latencies.append(latencies)
    # Unravel the latencies collected per repetition for these queries
    return [latency for latencies in all_latencies for latency in latencies]
    
async def benchmark_caching(db, collection, first_queries, test_query, warmup_queries, repetitions=5):
    # At least TRY to clear the dang cache even if we can't fully
    await db.command({"planCacheClear": collection.name})

    # Get cache goings
    await load_to_queue_and_run_repeatedly(collection, first_queries, 1)

    # Now warm up cache for query of interest
    await load_to_queue_and_run_repeatedly(collection, test_query, repetitions)

    # Then get the 'warm' latencies
    warm_latencies = await load_to_queue_and_run_repeatedly(collection, test_query, repetitions)

    # Then flood the cache
    await load_to_queue_and_run_repeatedly(collection, warmup_queries, 1)

    # Try it again for query of interest now 'cold'
    cold_latencies = await load_to_queue_and_run_repeatedly(collection, test_query, repetitions)

    return warm_latencies, cold_latencies


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# AI attribution: 
# Source = 
    # ChatGPT
# Prompt = 
    # the benchmarking code above + "Plot the avg latency for warm vs cold cache given the list of latencies per case"
# Significant response =
    # Wrapped code below, with minor adjustmenets to the title and save file 
# Verifications = plotting, https://www.statology.org/seaborn-boxplot-mean/ to check in on the green arrow being the mean and line being the median
def plot_cache_benchmark(warm_latencies, cold_latencies):
    df = pd.DataFrame({
        'Warm': warm_latencies,
        'Cold': cold_latencies
    })
    df_melted = df.melt(var_name='Phase', value_name='Latency (s)')

    plt.figure(figsize=(10, 6))
    sns.boxplot(data=df_melted, x='Phase', y='Latency (s)', showmeans=True)
    plt.title(f'Query Latency Before and After Polluting the Cache')
    plt.tight_layout()
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    plt.savefig(f'latency_comparison_{timestamp}.png')
    plt.show()
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

async def main():
    client = DBClient()
    database_name = "test_db_ranges"
    collection_name = "test_recipes_range_support"
    db = await client.get_database(database_name)
    collection = db[collection_name]

    # Get connection going
    await collection.find_one() 

    # Load up queries to flood the cache
    query_df = await csv_query_reader(collection, "src/database/official_testing_queries/simple_leq_queries.csv")
    all_queries = query_df["Query"].tolist()

    # This is the query of interest
    test_query = all_queries[2000:2001]     
    # The first 2k are to get the cache rolling
    first_queries = all_queries[:2000]
    # These are the ones to flood it 
    flooding_queries = all_queries[2001:]  

    warm_latencies, cold_latencies = await benchmark_caching(db, collection, first_queries, test_query, flooding_queries, repetitions=10)

    plot_cache_benchmark(warm_latencies, cold_latencies)

    await client.close_connection()

if __name__ == "__main__":
    asyncio.run(main())
