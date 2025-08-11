import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.ticker import PercentFormatter


def plot_save_duration(df_yes_compression: pd.DataFrame | None, df_no_compression: pd.DataFrame | None, title_prefix: str = ""):
    """
    Plots the client_save_duration vs. num_threads for both compression states.
    """
    if df_no_compression is None and df_yes_compression is None:
        print("Error: No data provided")

    plt.figure(figsize=(10, 6))
    
    # Plot for "Compression ON"
    sns.lineplot(data=df_yes_compression, x='num_threads', y='save_duration_seconds', marker='o', label='Compression ON')
    
    # Plot for "Compression OFF"
    sns.lineplot(data=df_no_compression, x='num_threads', y='save_duration_seconds', marker='X', label='Compression OFF')

    plt.title(f'{title_prefix}Valkey Save Duration vs. Number of Threads')
    plt.xlabel('Number of Threads')
    plt.ylabel('Save Duration (seconds)')
    plt.grid(True, linestyle='--', alpha=0.7)
    
    all_threads = pd.concat([df_yes_compression['num_threads'], df_no_compression['num_threads']]).unique()
    plt.xticks(sorted(all_threads))
    plt.legend()
    plt.tight_layout()
    plt.show()


def plot_throughput(df_yes_compression: pd.DataFrame, df_no_compression: pd.DataFrame, title_prefix: str = ""):
    """
    Plots 'actual_throughput(M/s)' vs. num_threads for both compression states.
    """
    
    plt.figure(figsize=(12, 7))

    # Plot for "Compression ON"
    sns.lineplot(data=df_yes_compression, x='num_threads', y='actual_throughput_mb_s', 
                 marker='o', label='Actual Throughput (Compression ON)')

    # Plot for "Compression OFF"
    sns.lineplot(data=df_no_compression, x='num_threads', y='actual_throughput_mb_s', 
                 marker='X', label='Actual Throughput (Compression OFF)')

    plt.title(f'{title_prefix}Valkey Save Throughput vs. Number of Threads')
    plt.xlabel('Number of Threads')
    plt.ylabel('Throughput (M/s)')
    plt.grid(True, linestyle='--', alpha=0.7)
    all_threads = pd.concat([df_yes_compression['num_threads'], df_no_compression['num_threads']]).unique()
    plt.xticks(sorted(all_threads))
    plt.legend()
    plt.tight_layout()
    plt.show()

def plot_cpu_utilization(df_yes_compression: pd.DataFrame, df_no_compression: pd.DataFrame, title_prefix: str = ""):
    """
    Plots 'CPU Utilization (%)' vs. num_threads for both compression states.
    """
    plt.figure(figsize=(12, 7))

    # Plot for "Compression ON"
    sns.lineplot(data=df_yes_compression, x='num_threads', y='cpu_utilization_percent', 
                 marker='o', label='CPU Utilization (Compression ON)')

    # Plot for "Compression OFF"
    sns.lineplot(data=df_no_compression, x='num_threads', y='cpu_utilization_percent', 
                 marker='X', label='CPU Utilization (Compression OFF)')

    plt.title(f'{title_prefix}CPU Utilization (%) vs. Number of Threads')
    plt.xlabel('Number of Threads')
    plt.ylabel('CPU Utilization (%)')
    plt.grid(True, linestyle='--', alpha=0.7)
    all_threads = pd.concat([df_yes_compression['num_threads'], df_no_compression['num_threads']]).unique()
    plt.xticks(sorted(all_threads))
    plt.legend()
    plt.tight_layout()
    plt.show()

def plot_key_bytes_throughput(df_yes_compression: pd.DataFrame, df_no_compression: pd.DataFrame, title_prefix: str = ""):
    """
    Plots 'key_bytes_throughput(M/s)' vs. num_threads for both compression states.
    """
    plt.figure(figsize=(12, 7))

    # Plot for "Compression ON"
    sns.lineplot(data=df_yes_compression, x='num_threads', y='valkey_data_throughput_mb_s', 
                 marker='o', label='Key Bytes Throughput (Compression ON)')

    # Plot for "Compression OFF"
    sns.lineplot(data=df_no_compression, x='num_threads', y='valkey_data_throughput_mb_s', 
                 marker='X', label='Key Bytes Throughput (Compression OFF)')

    plt.title(f'{title_prefix}Valkey Save Throughput vs. Number of Threads')
    plt.xlabel('Number of Threads')
    plt.ylabel('Key Bytes Throughput (M/s)')
    plt.grid(True, linestyle='--', alpha=0.7)
    all_threads = pd.concat([df_yes_compression['num_threads'], df_no_compression['num_threads']]).unique()
    plt.xticks(sorted(all_threads))
    plt.legend()
    plt.tight_layout()
    plt.show()

# You can apply the same pattern to your other functions like plot_cpu_utilization, etc.


def plot_percent_change(df_yes_compression: pd.DataFrame, df_no_compression: pd.DataFrame, metric_column: str, title_prefix: str = ""):
    """
    Plots the percentage change of a metric relative to the 1-thread baseline.
    """
    plt.figure(figsize=(12, 7))

    # --- Process "Compression ON" DataFrame ---
    # Find the baseline value (performance at 1 thread)
    baseline_yes_comp = df_yes_compression[df_yes_compression['num_threads'] == 1][metric_column].iloc[0]
    # Calculate the percentage of the baseline
    df_yes_compression['pct_change'] = (df_yes_compression[metric_column] / baseline_yes_comp)

    # --- Process "Compression OFF" DataFrame ---
    baseline_no_comp = df_no_compression[df_no_compression['num_threads'] == 1][metric_column].iloc[0]
    df_no_compression['pct_change'] = (df_no_compression[metric_column] / baseline_no_comp)

    # --- Plotting ---
    sns.lineplot(data=df_yes_compression, x='num_threads', y='pct_change', marker='o', label='Compression ON')
    sns.lineplot(data=df_no_compression, x='num_threads', y='pct_change', marker='X', label='Compression OFF')
    
    # Add a horizontal line at 100% for the baseline
    plt.axhline(y=1.0, color='r', linestyle='--', label='Baseline (1 Thread)')

    # Format the y-axis to show percentages
    plt.gca().yaxis.set_major_formatter(PercentFormatter(1.0))

    plt.title(f'{title_prefix}Percent Change in {metric_column.replace("_", " ").title()} vs. Threads')
    plt.xlabel('Number of Threads')
    plt.ylabel(f'Performance vs. Baseline')
    plt.grid(True, linestyle='--', alpha=0.7)
    
    all_threads = pd.concat([df_yes_compression['num_threads'], df_no_compression['num_threads']]).unique()
    plt.xticks(sorted(all_threads))
    plt.legend()
    plt.tight_layout()
    plt.show()