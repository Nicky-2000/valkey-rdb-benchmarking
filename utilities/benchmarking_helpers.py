from concurrent.futures import ProcessPoolExecutor, as_completed
import hashlib
import random
import string
import subprocess
import time
import os
import sys
import pandas as pd
import psutil
import valkey  # Assuming valkey-py is installed
import pyarrow as pa
import pyarrow.parquet as pq

def save_results_to_csv(results, num_keys, value_size, output_dir=".", csv_file_name=None):
    """
    Generates a descriptive filename and saves benchmark results to a CSV file.

    Args:
        results (list[dict]): A list of dictionaries representing the data.
        num_keys (int): The number of keys used in the benchmark.
        value_size (int): The size of values in bytes.
        output_dir (str, optional): The directory to save the CSV in. Defaults to current directory.
    """
    df = pd.DataFrame(results)
    os.makedirs(output_dir, exist_ok=True)
    
    if not csv_file_name:
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        keys_str = f"{num_keys // 1_000_000}M" if num_keys % 1_000_000 == 0 else f"{num_keys}"
        filename = f"benchmark_results_{keys_str}_keys_{value_size}_bytes_{timestamp}.csv"
        csv_path = os.path.join(output_dir, filename)
    else: 
        csv_path = os.path.join(output_dir, csv_file_name)
    
    df.to_csv(csv_path, index=False)
    print(f"Results saved to: {csv_path}")
    