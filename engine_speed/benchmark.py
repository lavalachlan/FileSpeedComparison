import time
import numpy as np

def func(func, runs=5, label=""):
    times = []
    for _ in range(runs):
        start = time.perf_counter()
        func()
        end = time.perf_counter()
        times.append(end - start)
    avg_time = np.mean(times)
    print(f"{label:<15} | Avg: {avg_time:.6f}s over {runs} runs")
    return avg_time