import time
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Benchmarking timing function
def timing(func, runs=5, label=""):
    times = []
    for _ in range(runs):
        start = time.perf_counter()
        func()
        end = time.perf_counter()
        times.append(end - start)
    avg_time = np.mean(times)
    print(f"{label:<15} | Avg: {avg_time:.6f}s over {runs} runs")
    return avg_time

def run_benchmark(methods_dict, runs=5, timing_func=timing):
    if timing_func is None:
        raise ValueError("Please provide a timing function")

    results = {}
    for label, func in methods_dict.items():
        results[label] = timing_func(func, runs=runs, label=label)
    return results


# Function to convert timings to relative speeds
def relative_speeds(time_dict):
    min_time = min(time_dict.values())
    speed_dict = {
        label: time / min_time for label, time in time_dict.items()
    }
    return speed_dict

# Helper function to plot results
def plot_results(results_dict, title, filename=None, relative=False, xtick_rotation=0):
    if relative:
        y_label = "Relative Time (x)"
    else:
        y_label = "Time (s)"
    print(f"\n{title}")
    df_plot = pd.DataFrame([
        {"Method": key, y_label: value}
        for key, value in results_dict.items()
    ])
    print(df_plot)
    plt.figure(figsize=(12,8))
    ax = sns.barplot(df_plot, x="Method", y=y_label, hue="Method", palette="pastel", legend=False)
    for container in ax.containers:
        ax.bar_label(container, fmt="%.4f", fontsize=10)
    plt.title(title)
    if xtick_rotation > 0:
        plt.xticks(rotation=xtick_rotation, ha='right')
    plt.tight_layout()
    if filename:
        plt.savefig(filename)
    plt.show()