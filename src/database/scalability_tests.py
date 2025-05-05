import asyncio
import time
from datetime import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from database import DBClient
from ast import literal_eval 

#  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# AI attribution: 
# Source = 
    # ChatGPT
# Prompts = 
    #  the code from src/database/benchmark_search_index.py + 
    # "Take the queueing and search benchmarking from this file, fix num workers to 40, and instead benchmark avg latency as number of users increases (simulating say 100 per user). Then plot it"
# Significant response =
    # Wrapped code below, removing its comments and verifying step by step with own comments, additonally producing the plot to examine expected labels. 
    # After recognizing its plot was incorrect, I fixed that. It also iterated over more workers, I fixed that to 40. It also only did a static num queries whereas I had to make it dynamic.
    # Then massaging to produce the right color, size, save names, etc
    # Also got rid of irrelevant things like tracking worker id, and load csv for plotting from directory vs in memory
    # Everything was verified step by step.

async def worker(query_queue, collection, latencies, success_counter):
    '''function that routinely queries from queue until it's empty'''
    while True:
        query = await query_queue.get()
        # Stop if nothing left
        if query is None:
            query_queue.task_done()  
            break

        try:
            start = time.monotonic()
            results = await collection.aggregate(query)
            # Doing this to prevent the lazy retrieval and actually gauge the query time fully
            _ = await results.to_list()
            end = time.monotonic()
            success_counter[0] += 1
            latencies.append(end - start)

        except Exception as e:
            print(f"Error executing query: {e}")
        
        finally:
            # This indicates this part of the queue was good and to move on
            query_queue.task_done()

async def run_benchmark_with_workers(num_workers, queries, collection, think_time):
    queue = asyncio.Queue()
    latencies = []
    success_counter = [0]

    # Add the subset of queries to be handled at once
    for query in queries:
        await queue.put(query)

    tasks = [
        asyncio.create_task(worker(i, queue, collection, latencies, success_counter, think_time))
        for i in range(num_workers)
    ]

    start = time.monotonic()
    await queue.join()
    duration = time.monotonic() - start

    for task in tasks:
        await task  # Ensure all workers exit cleanly

    return {
        "Workers": num_workers,
        "Duration": duration,
        "QPS": success_counter[0] / duration,
        "AvgLatency": np.mean(latencies),
        "MaxLatency": np.max(latencies),
        "MinLatency": np.min(latencies),
        "MedianLatency": np.median(latencies)
    }

async def run_user_scaling_benchmark():
    '''function nearly identical to the benchmark_search_index except fixed number of workers and an increasily large num queries beyond 10k, up to 256k'''

    client = DBClient()
    db = await client.get_database("test_db_ranges")
    collection = db["test_recipes_range_support"]

    test_queries_df = pd.read_csv('test_searches_for_scalability.csv')
    test_queries_df["Search"] = test_queries_df["Search"].apply(literal_eval)
    test_queries = test_queries_df["Search"].tolist()

    user_counts = [1, 5, 10, 20, 40, 80, 160, 320, 640, 1280, 2560]  
    queries_per_user = 100 
    num_workers = 40

    results = []

    for users in user_counts:
        total_needed = users * queries_per_user
        if total_needed > len(test_queries):
            print(f"Not enough queries for {users} users, skipping.")
            continue
        print(f"\nSimulating {users} users with {num_workers} workers...")

        result = await run_benchmark_with_workers(
            num_workers,
            # Only take the subset 
            test_queries[:total_needed], 
            collection,
        )
        print(result)
        results.append(result)

    await client.close_connection()
    return results


def plot_user_scaling():
    df = pd.read_csv("user_scaling_benchmark_2025-05-04_06-42-43.csv").iloc[1:]

    plt.figure(figsize=(14, 6))
    sns.lineplot(data=df, x="NumQueries", y="AvgLatency", marker="o", color="orange")
    # plt.xscale("log")
    plt.title("Latency vs Number of Queries")
    plt.xlabel("Number of Queries")
    plt.ylabel("Avg Latency (seconds)")
    plt.tight_layout()
    plt.savefig(f"user_latency_vs_queries.png")
    plt.show()

    plt.figure(figsize=(14, 6))
    sns.lineplot(data=df, x="NumQueries", y="AvgLatency", marker="o", color="blue")
    # plt.xscale("log")
    plt.title("Latency vs Number of Queries")
    plt.xlabel("Number of Queries")
    plt.ylabel("Avg Latency (seconds)")
    plt.tight_layout()
    plt.savefig(f"throughput_vs_queries.png")
    plt.show()

    # Save data to CSV
    df.to_csv(f"user_scaling_benchmark.csv", index=False)


if __name__ == "__main__":
    # results = asyncio.run(run_user_scaling_benchmark())
    plot_user_scaling()

#  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
