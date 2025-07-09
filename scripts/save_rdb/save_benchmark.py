# Steps 
# 1. Start Server
# 2. Populate Server With Keys
# 3. Start Save
# 4. Record Data
#   - Save Time
#   - CPU Usuage
#   - Num Keys
#   - Value Size (Bytes)
#   - IO Write
# 5. Change Number of threads
# 6. Save Again


import subprocess
import time
import os
import sys
import argparse

import pandas as pd
from utilities.benchmarking_helpers import start_valkey_server, stop_valkey_server, run_single_node_save, populate_data_standalone, get_db_key_count, save_results_to_csv



# --- Configuration Constants ---
VALKEY_SERVER_PATH = "./src/valkey-server"
VALKEY_CLI_PATH = "./src/valkey-cli"
TEST_CONF_TEMPLATE = (
    "testconfs/valkey_rdb_benchmark_base.conf"  # Assumes a non-cluster config
)
DEFAULT_TEMP_SUBDIR = "valkey_rdb_benchmark_standalone_run"
DEFAULT_START_PORT = 7001
DEFAULT_DB_FILE = "dump.rdb"
DEFAULT_LOG_FILE = "valkey.log"
DEFAULT_KEY_SIZE_BYTES = 100
DEFAULT_NUM_KEYS_MILLIONS = int(1) 
RDB_SNAPSHOT_THREADS = 1


JUST_WRITE_KEYS = True

def delete_file(file_path):
    print(f"Attempting to delete file: {file_path}")
    # Check if the file exists before attempting to delete it
    if os.path.exists(file_path):
        try:
            os.remove(file_path)
            print(f"Successfully deleted: {file_path}")
        except OSError as e:
            print(f"Error deleting file {file_path}: {e}")
    else:
        print(f"File not found, skipping deletion: {file_path}")

def run_save_benchmark_standalone(
    start_port: int,
    conf_path: str,
    num_keys: int,
    key_value_size: int,
    temp_base_dir: str,
    rdb_snapshot_threads: int,
    rdbcompression: int, 
    rdbchecksum: int,
):
    """
    Runs a benchmark for the BGSAVE operation on a single Valkey standalone instance,
    including client-side timing and parsing server-side logs.
    """
    # Create a unique temporary directory for this run
    if not os.path.exists(temp_base_dir):
        os.makedirs(temp_base_dir)
        print(f"Created temporary directory: {temp_base_dir}")
    else:
        print(f"Using existing temporary directory: {temp_base_dir}")
        subprocess.run(
            ["rm", "-rf", os.path.join(temp_base_dir, "*")], check=True
        )  # Clear previous contents

    print("\n--- Starting Valkey Standalone Server ---")
    # Start a single Valkey server in standalone mode
    data_dir = str(os.path.join(temp_base_dir, f"node_data_{start_port}"))
    log_file = str(os.path.join(temp_base_dir, f"node_log_{start_port}.log"))

    process, client, node_info = start_valkey_server(
        port=start_port,
        conf_path=conf_path,
        data_dir=data_dir,
        log_file_path=log_file,
        cluster_mode=False,
        extra_sleep_time=1,
        rdb_snapshot_threads=rdb_snapshot_threads,
        rdbcompression=rdbcompression, 
        rdbchecksum=rdbchecksum
    )
    if not process:
        print("Failed to start Valkey server. Aborting benchmark.", file=sys.stderr)
        return None

    try:
        populate_data_standalone(node_info, num_keys, key_value_size)
        
        if JUST_WRITE_KEYS:
            return
        
        
        num_keys_initial = get_db_key_count(start_port)
        
        assert num_keys == num_keys_initial, (
            f"num_keys does not match the actual number of keys on the db num_keys: {num_keys}, num_keys_db: {num_keys_initial}"
        )
        
        print(f"\n--- Keys before save = {num_keys_initial} ---\n")
        results = []
        for num_threads in [1, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20]:
            client.config_set("rdb-snapshot-threads", num_threads)
            
            # Run BGSAVE on the single node using the already connected client
            print(f"\n{'='*60}")
            print(f"--- TEST ITERATION: rdb-snapshot-threads = {num_threads} ---")
            print(f"{'='*60}") 
            save_results = run_single_node_save(client, process ,temp_base_dir, DEFAULT_LOG_FILE)
            # Calculate throughput, handling potential division by zero
            save_duration = save_results["client_save_duration"]
            throughput = 0
            actual_throughput = 0
            if save_results["save_status"] == "ok" and save_duration > 0:
                throughput = ((num_keys * key_value_size) / save_duration) * (10**-6)
                actual_throughput = (save_results["io_write_bytes"] / save_duration) * (10**-6)

            # --- Get RDB File Size ---
            rdb_file_path = os.path.join(data_dir, "dump.rdb")
            rdb_file_size_bytes = 0
            if os.path.exists(rdb_file_path):
                rdb_file_size_bytes = os.path.getsize(rdb_file_path)
            else:
                print(f"Warning: RDB file not found at {rdb_file_path}. Cannot get its size.", file=sys.stderr)
            
            # Aggregate all results
            new_result = {
                "keys": num_keys,
                "value_size": key_value_size,
                "num_threads": num_threads,
                **save_results, # Unpack the results 
                "key_bytes_throughput(M/s)": throughput,
                "actual_throughput(M/s)": actual_throughput,
                "rdb_file_size_bytes": rdb_file_size_bytes
            }
            results.append(new_result)
            
            print(f"\n--- Cleaning up dump.rdb for {num_threads} threads... ---")
            delete_file(rdb_file_path)
            print(f"--- DONE with {num_threads} threads. ---")
        
        print(f"\n{'='*50}")
        print("--- All Tests Completed ---")
        print(f"Total results collected: {len(results)}")
        print(f"{'='*50}\n")

        return results

    finally:
        # Stop the single server process
        if not JUST_WRITE_KEYS:
            stop_valkey_server(process, client, start_port)




def main():
    parser = argparse.ArgumentParser(
        description="Valkey RDB Persistence Benchmark Tool for Standalone"
    )
    parser.add_argument(
        "--start-port",
        type=int,
        default=DEFAULT_START_PORT,
        help=f"Starting port for Valkey cluster nodes (default: {DEFAULT_START_PORT})",
    )
    parser.add_argument(
        "--num-keys",
        type=float,
        default=DEFAULT_KEY_SIZE_BYTES,
        help=f"Number of keys to populate in millions (default: {DEFAULT_KEY_SIZE_BYTES})",
    )
    parser.add_argument(
        "--value-size",
        type=int,
        default=DEFAULT_NUM_KEYS_MILLIONS,
        help=f"Size of the value in bytes for populated keys (default: {DEFAULT_NUM_KEYS_MILLIONS})",
    )
    parser.add_argument(
        "--rdb-snapshot-threads",
        type=int,
        default=RDB_SNAPSHOT_THREADS,
        help=f"Num threads to save keys with (default: {RDB_SNAPSHOT_THREADS})",
    )
    parser.add_argument(
        "--conf",
        type=str,
        default=TEST_CONF_TEMPLATE,
        help=f"Path to the Valkey server configuration file template (default: {TEST_CONF_TEMPLATE}). This config should NOT contain cluster-enabled yes, as it's added by the script.",
    )
    parser.add_argument(
        "--temp-dir",
        type=str,
        # default=os.path.join(os.getcwd(), f"{DEFAULT_TEMP_SUBDIR}_{time.time_ns()}"),
        default=os.path.join("/dev/shm", f"{DEFAULT_TEMP_SUBDIR}_{time.time_ns()}"),
        help="Base directory for temporary data and logs for the cluster",
    )
    parser.add_argument(
        "--rdbcompression",
        type=str,
        default="yes",
        help="if 'yes' then we will use LZF compression. If 'no' then compression will be disabled",
    )
    
    parser.add_argument(
        "--rdbchecksum",
        type=str,
        default="yes",
        help="if 'yes' then we will calculate a checksum. If 'no' then checksum calculation will be turned off",
    )
    parser.add_argument(
        "--tempfs",
        type=bool,
        default=False,
        help="if True then we will use a temp file system base. If False then we will use Disk",
    )
    
    args = parser.parse_args()
    if args.tempfs == True:
        temp_dir = os.path.join("/dev/shm", f"{DEFAULT_TEMP_SUBDIR}_{time.time_ns()}")
    else: 
        temp_dir = os.path.join(os.getcwd(), f"{DEFAULT_TEMP_SUBDIR}_{time.time_ns()}"),

    num_keys = int(args.num_keys * 1e6)
    global JUST_WRITE_KEYS
    JUST_WRITE_KEYS = args.just_populate

    print("--- Starting RDB Persistence Benchmark for Standalone Valkey ---")
    print(f"Num Keys: {num_keys}")
    print(f"Key Size: {args.value_size}")
    print(f"RDB Snapshot Threads: {args.rdb_snapshot_threads}")
    results = run_save_benchmark_standalone(
        args.start_port, args.conf, num_keys, args.value_size, temp_dir, args.rdb_snapshot_threads, args.rdbcompression, args.rdbchecksum
    )
    
    OUTPUT_DIR = "/usr/local/google/home/nickykhorasani/Documents/valkey/benchmarking_python_scripts/benchmarking_results"

    csv_file_name = f"benchmark__temp_fs_{args.tempfs}_{args.num_keys}M_keys_{args.value_size}byte_values_{args.rdbcompression}_compression_{args.rdbchecksum}_checksum.csv"
    save_results_to_csv(results, num_keys, args.value_size, output_dir=OUTPUT_DIR, csv_file_name=csv_file_name)


if __name__ == "__main__":
    main()