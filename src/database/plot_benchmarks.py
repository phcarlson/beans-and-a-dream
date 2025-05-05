import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# First, load up our high prob common sampled (70/30) and low prob common sampled (30/70) for both the regular database and search engine benchmark results
search_7_df = pd.read_csv("src/database/official_testing_results/OFFICIAL_search_benchmark_results_simple_0.7_common.csv")
search_7_df["IndexType"] = "Search"
search_7_df["ProbCommonSampled"] = "0.7"

reg_7_df = pd.read_csv("src/database/official_testing_results/OFFICIAL_reg_benchmark_results_simple_0.7_common.csv")
reg_7_df["IndexType"] = "Regular"
reg_7_df["ProbCommonSampled"] = "0.7"

search_3_df = pd.read_csv("src/database/official_testing_results/OFFICIAL_search_benchmark_results_simple_0.3_common.csv")
search_3_df["IndexType"] = "Search"
search_3_df["ProbCommonSampled"] = "0.3"

reg_3_df = pd.read_csv("src/database/official_testing_results/OFFICIAL_reg_benchmark_results_simple_0.3_common.csv")
reg_3_df["IndexType"] = "Regular"
reg_3_df["ProbCommonSampled"] = "0.3"

# Merge benchmarking results, now that they are labeled, so we can handle and plot them together 
# Per pandas, "ignore_index being True means the resulting axis will be labeled 0, ..., n - 1... useful if you are concatenating objects"
merged_results_df = pd.concat([search_7_df, search_3_df, reg_7_df, reg_3_df], ignore_index=True)

#  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# AI attribution: 
# Source = 
    # ChatGPT
# Prompts = 
    # 1) the rest of existing code for plotting in this file before applying keys + "How do I also add to the key whether it's a search index or reg index" 
    # 2) the rest of existing code after applying keys + "How do I make it so that the search index is dashed line and reg index is solid"
    # 3) "How do I fix the legend / line overlap"
# Significant response =
    # 1) and 2) Wrapped code below, removing its comments and VERIFYING step by step with OWN COMMENTS, additonally duplicating the plot for many different y values and producing the plots to examine expected labels
    # 3) For the legend overlap fix, the answer was:
        #   plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)
        #   plt.tight_layout(rect=[0, 0, 0.85, 1])  # leave space for the legend"
    # Then added my own loop over 0.7 vs 0.3, massaging to produce the right colors, size, labels, save names, etc

def classify_index_type(index_name):
    if "dynamic" in index_name or "static_on_ingredient_pairs" in index_name:
        return "Search Index"
    else:
        return "Regular Index"


# For the rainbow of colors used to plot, we want to consistently keep the same color associated with the same index
# This fixes that so it remains the same across all plots
base_palette = sns.color_palette(palette="colorblind", n_colors=len(merged_results_df["IndexSetupFileName"].unique()))
color_map = {label: base_palette[i] for i, label in enumerate(merged_results_df["IndexSetupFileName"].unique())}

# Get top 2 biggest QPS so we can plot them separately 
# and make the plots more fine-grained due to wildly diff scaling between best and worst 
# (which was accidentally obscuring patterns in results for the worst performing)
top_indexes = ['ingredients_pairs_exact_matching_ascending', 'ingredients_pairs_exact_matching_descending']
top_df = merged_results_df[merged_results_df["IndexSetupFileName"].isin(top_indexes)]
rest_df = merged_results_df[~merged_results_df["IndexSetupFileName"].isin(top_indexes)]

for subset_label, subset_df in [("Top 2 QPS", top_df), ("Rest of QPS", rest_df)]:
    # Repeat plotting for both the 0.7 and 0.3 cases
    for prob in ["0.3", "0.7"]:
        # Take the part of the df that holds the right common sampling prob for this plot
        prob_df = subset_df[subset_df["ProbCommonSampled"] == prob]
            
        # PLOT 1: QPS vs MaxWorkers on a line plot per https://seaborn.pydata.org/generated/seaborn.lineplot.html
        plt.figure(figsize=(16, 6))
        plt.xscale("log", base=2)

        plot = sns.lineplot(
            data=prob_df,
            x="MaxWorkers",
            y="QPS",
            # In seaborn, the hue parameter determines which column in the data frame should be used for colour encoding per https://datascience.stackexchange.com/questions/46117/meaning-of-hue-in-seaborn-barplot
            hue="IndexSetupFileName",
            # Otherwise puts out weird CI shading for 1 datapoint
            errorbar=None,
            # Make search dashed so it is more clear which index is for what
            style="IndexType", 
            # Forces which one is dashed
            style_order=["Regular", "Search"],
            # See the individual plot dots
            markers=["o", "X"],
            palette = color_map
        )

        plt.title(f"Queries Per Second (QPS) vs Number of Workers (Asyncio Tasks) for Common Ingredient Sampling Rate = {prob}")
        plt.xlabel("Number of Workers")
        plt.ylabel("QPS")                
        # Without this, the legend will overlap
        if subset_label == "Top 2 QPS":
            plt.tight_layout()
        else:
            plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)
            plt.tight_layout()

        # plt.show()
        # This is just changing the 0.7 and 0.3 to 0_7 and 0_3 for saving the file
        filename = f"{subset_label}_qps_vs_workers_sampling_{prob.replace('.', '_')}.png"
        plt.savefig(os.path.join("official_testing_plots", filename))
        plt.close()


        # Also plot the full thing despite the scaling obscuring worst performers to better visualize the zoom-in:
        prob_df = merged_results_df[merged_results_df["ProbCommonSampled"] == prob]
            
        plt.figure(figsize=(16, 6))
        plt.xscale("log", base=2)

        plot = sns.lineplot(
            data=prob_df,
            x="MaxWorkers",
            y="QPS",
            # In seaborn, the hue parameter determines which column in the data frame should be used for colour encoding per https://datascience.stackexchange.com/questions/46117/meaning-of-hue-in-seaborn-barplot
            hue="IndexSetupFileName",
            # Otherwise puts out weird CI shading for 1 datapoint
            errorbar=None,
            # Make search dashed so it is more clear which index is for what
            style="IndexType", 
            # Forces which one is dashed
            style_order=["Regular", "Search"],
            # See the individual plot dots
            markers=["o", "X"],
            palette = color_map
        )
        plt.title(f"Queries Per Second (QPS) vs Number of Workers (Asyncio Tasks) for Common Ingredient Sampling Rate = {prob}")
        plt.xlabel("Number of Workers")
        plt.ylabel("QPS")                
        # Without this, the legend will overlap
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)
        plt.tight_layout()

        # plt.show()
        # This is just changing the 0.7 and 0.3 to 0_7 and 0_3 for saving the file
        filename = f"qps_vs_workers_sampling_{prob.replace('.', '_')}.png"
        plt.savefig(os.path.join("official_testing_plots", filename))
        plt.close()


# Repeat plotting for both the 0.7 and 0.3 cases
for prob in ["0.3", "0.7"]:
    #PLOT 2: AvgLatency vs Num Workers
    prob_df = subset_df[subset_df["ProbCommonSampled"] == prob]
    plt.figure(figsize=(14, 6))
    plt.xscale("log", base=2)

    sns.lineplot(
        data=prob_df,
        x="MaxWorkers",
        y="AvgLatency",
        # In seaborn, the hue parameter determines which column in the data frame should be used for colour encoding per https://datascience.stackexchange.com/questions/46117/meaning-of-hue-in-seaborn-barplot
        hue="IndexSetupFileName",
        # Otherwise puts out weird CI shading for 1 datapoint
        errorbar=None,
        # Make search dashed so it is more clear which index is for what
        style="IndexType", 
        # Forces which one is dashed
        style_order=["Regular", "Search"],
        # See the individual plot dots
        markers=["o", "X"],
        palette = color_map
    )
    plt.title(f"Avg Latency vs Number of Workers (Asyncio Tasks) for Common Ingredient Sampling Rate = {prob}")
    plt.xlabel("Number of Workers")
    plt.ylabel("Avg Latency")
    # Without this, the legend will overlap
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)
    plt.tight_layout()

    # plt.show()
    # This is just changing the 0.7 and 0.3 to 0_7 and 0_3 for saving the file
    filename = f"avg_latency_vs_workers_sampling_{prob.replace('.', '_')}.png"
    plt.savefig(os.path.join("official_testing_plots", filename))
    plt.close()

    # PLOT 3: Max Latency vs Num Workers
    plt.figure(figsize=(14, 6))
    plt.xscale('log', base=2)
    sns.lineplot(
        data=prob_df,
        x="MaxWorkers",
        y="MaxLatency",
        # In seaborn, the hue parameter determines which column in the data frame should be used for colour encoding per https://datascience.stackexchange.com/questions/46117/meaning-of-hue-in-seaborn-barplot
        hue="IndexSetupFileName",
        # Otherwise puts out weird CI shading for 1 datapoint
        errorbar=None,
        # Make search dashed so it is more clear which index is for what
        style="IndexType", 
        # Forces which one is dashed
        style_order=["Regular", "Search"],
        # See the individual plot dots
        markers=["o", "X"],
        palette = color_map
    )
    plt.title(f"Max Latency vs Number of Workers (Asyncio Tasks) for Common Ingredient Sampling Rate = {prob}")
    plt.xlabel("Number of Workers")
    plt.ylabel("Max Latency")
    # Without this, the legend will overlap
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)
    plt.tight_layout()
    # plt.show()
    # This is just changing the 0.7 and 0.3 to 0_7 and 0_3 for saving the file
    filename = f"max_latency_vs_workers_sampling_{prob.replace('.', '_')}.png"
    plt.savefig(os.path.join("official_testing_plots", filename))
    plt.close()

    # PLOT 4: Median Latency vs Num Workers
    plt.figure(figsize=(14, 6))
    plt.xscale("log", base=2)

    sns.lineplot(
        data=prob_df,
        x="MaxWorkers",
        y="MedianLatency",
        # In seaborn, the hue parameter determines which column in the data frame should be used for colour encoding per https://datascience.stackexchange.com/questions/46117/meaning-of-hue-in-seaborn-barplot
        hue="IndexSetupFileName",
        # Otherwise puts out weird CI shading for 1 datapoint
        errorbar=None,
        # Make search dashed so it is more clear which index is for what
        style="IndexType", 
        # Forces which one is dashed
        style_order=["Regular", "Search"],
        # See the individual plot dots
        markers=["o", "X"],
        palette = color_map
    )
    plt.title(f"Median Latency vs Number of Workers (Asyncio Tasks) for Common Ingredient Sampling Rate = {prob}")
    plt.xlabel("Number of Workers")
    plt.ylabel("Median Latency")
    # Without this, the legend will overlap
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)
    plt.tight_layout()
    # plt.show()
    # This is just changing the 0.7 and 0.3 to 0_7 and 0_3 for saving the file
    filename = f"median_latency_vs_workers_sampling_{prob.replace('.', '_')}.png"
    plt.savefig(os.path.join("official_testing_plots", filename))
    plt.close()


# We want a bar plot for the max (best QPS out of all num workers used) given:
    # the index used, whether it's search/reg, and whether it is 0.7 or 0.3 common sampled
max_qps_df = merged_results_df.groupby(["IndexSetupFileName", "IndexType", "ProbCommonSampled"], as_index=False)["QPS"].max()

# Basically, depending on the common sampling used and index type, we adjust the more informative label accoredingly
def get_label(row):
    if row["IndexType"] == "Regular":
        return "Regular DB Index With Samping 0.3 common, 0.7 rare" if row["ProbCommonSampled"] == "0.3" else  "Regular DB Index With Samping .7 common, .3 rare"
    else:
        return "Search Index With Samping 0.3 common, 0.7 rare" if row["ProbCommonSampled"] == "0.3" else  "Search Index With Samping .7 common, .3 rare"
max_qps_df["IndexTypeAndCommonSamplingRate"] = max_qps_df.apply(get_label, axis=1)

# Get search and reg lables to place them with the right color
search_labels = [label for label in max_qps_df["IndexTypeAndCommonSamplingRate"] if "Search" in label]
regular_labels = [label for label in max_qps_df["IndexTypeAndCommonSamplingRate"] if "Regular" in label]
dark_blue, light_blue = sns.color_palette("Blues", 2)
dark_orange, light_orange = sns.color_palette("Oranges", 2)

# Create the palette where we:
# 0.7 with darker colors, 0.3 with lighter colors
# blue with reg index, orange with search index
bar_palette = {}
for label in max_qps_df["IndexTypeAndCommonSamplingRate"].unique():
    is_search = "Search" in label
    is_07 = "0.7" in label

    if is_search and is_07:
        bar_palette[label] = dark_orange
    elif is_search:
        bar_palette[label] = light_orange
    elif not is_search and is_07:
        bar_palette[label] = dark_blue
    else:
        bar_palette[label] = light_blue

plt.figure(figsize=(14, 7))
sns.barplot(
    x="IndexSetupFileName",
    y="QPS",
    hue="IndexTypeAndCommonSamplingRate",
    data=max_qps_df,
    palette=bar_palette,
    errorbar=None
)
plt.title("Best QPS (Out of All # Workers Tried) by Index Setup, For Common Ingredient Sampling Rate = 0.7 VS 0.3")
# This is for the index file name labels per par plot to not overlap terribly
plt.xticks(rotation=20, ha="right")
plt.tight_layout()

filename = f"best_QPS_bar_plot.png"
plt.savefig(os.path.join("official_testing_plots", filename))
plt.close()
#  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~