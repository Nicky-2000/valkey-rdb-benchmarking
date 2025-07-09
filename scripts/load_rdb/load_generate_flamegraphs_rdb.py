import subprocess
import time
import os
import sys
import argparse
import signal
import psutil

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

PERF_PATH = "/google/bin/releases/kernel-tools/perf5/usr/bin/perf5"

FLAMEGRAPH_OUTPUT_DIR = "/usr/local/google/home/nickykhorasani/Documents/valkey/benchmarking_python_scripts/flamegraphs"

FLAME_GRAPH_REPO_PATH = "/usr/local/google/home/nickykhorasani/Documents/FlameGraph"

# Global variable to store the perf process
perf_process = None

def start_perf(pid, output_dir=FLAMEGRAPH_OUTPUT_DIR):
    global perf_process
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    perf_data_path = os.path.join(output_dir, "perf.data")
    print(f"Starting perf record for PID {pid}, output to {perf_data_path}")
    perf_process = subprocess.Popen(
        [PERF_PATH, "record", "-F", "999", "-g", "-p", str(pid), "-o", perf_data_path],
        preexec_fn=os.setsid  # Start in a new process group
    )
    print("Perf process started.")


def end_perf():
    global perf_process
    if perf_process:
        print("Stopping perf process...")
        try:
            os.killpg(os.getpgid(perf_process.pid), signal.SIGINT)  # Send Ctrl+C # <--- Change here
            perf_process.wait(timeout=10)  # Wait for it to terminate
            print("Perf process stopped.")
        except (subprocess.TimeoutExpired, ProcessLookupError) as e:
            print(f"Error stopping perf process: {e}. Attempting to terminate.")
            perf_process.terminate()
            perf_process.wait()
        perf_process = None
    else:
        print("Perf process was not running.")
        
        
def generate_flamegraph(output_dir=FLAMEGRAPH_OUTPUT_DIR, flamegraph_file_name="save_flamegraph.svg"):
    print("Generating flame graph...")
    perf_data_path = os.path.join(output_dir, "perf.data")
    perf_stacks_path = os.path.join(output_dir, "valkey.perf.stacks")
    folded_stacks_path = os.path.join(output_dir, "valkey.folded.stacks")
    svg_output_path = os.path.join(output_dir, flamegraph_file_name)

    if not os.path.exists(perf_data_path):
        print(f"Error: perf.data not found at {perf_data_path}. Cannot generate flame graph.")
        return

    try:
        # perf script
        print(f"Running: perf script -i {perf_data_path} > {perf_stacks_path}")
        with open(perf_stacks_path, "w") as f:
            subprocess.run([PERF_PATH, "script", "-i", perf_data_path], check=True, stdout=f)

        # stackcollapse-perf.pl
        stackcollapse_script = f"{FLAME_GRAPH_REPO_PATH}/stackcollapse-perf.pl"
        if not os.path.exists(stackcollapse_script):
            print(f"Error: stackcollapse-perf.pl not found at {stackcollapse_script}. Please ensure FlameGraph repository is cloned correctly.")
            return
        print(f"Running: {stackcollapse_script} {perf_stacks_path} > {folded_stacks_path}")
        with open(folded_stacks_path, "w") as f:
            subprocess.run([stackcollapse_script, perf_stacks_path], check=True, stdout=f)

        # flamegraph.pl
        flamegraph_script = f"{FLAME_GRAPH_REPO_PATH}/flamegraph.pl"
        if not os.path.exists(flamegraph_script):
            print(f"Error: flamegraph.pl not found at {flamegraph_script}. Please ensure FlameGraph repository is cloned correctly.")
            return
        print(f"Running: {flamegraph_script} {folded_stacks_path} > {svg_output_path}")
        with open(svg_output_path, "w") as f:
            subprocess.run([flamegraph_script, folded_stacks_path], check=True, stdout=f)

        print(f"Flame graph generated successfully at {svg_output_path}")

    except subprocess.CalledProcessError as e:
        print(f"Error during flame graph generation: {e}")
    except FileNotFoundError as e:
        print(f"Command not found: {e}. Make sure 'perf' is installed and FlameGraph scripts are executable and in the correct path.")


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



def generate_flamegraph_rdb_load(
    start_port: int,
    conf_path: str,
    num_keys: int,
    key_value_size: int,
    temp_base_dir: str,
    rdbcompression: str,
    rdbchecksum: str
):
    """
    Orchestrates profiling the RDB load to generate a flame graph and detailed metrics.
    """
    # --- 1. Setup & RDB Preparation Phase ---
    print(f"\n{'='*60}")
    print("--- Phase 1: Preparing RDB file for load test ---")
    print(f"{'='*60}")
    
    if not os.path.exists(temp_base_dir):
        os.makedirs(temp_base_dir)
    else:
        subprocess.run(["rm", "-rf", os.path.join(temp_base_dir, "*")], check=True)
    
    data_dir = os.path.join(temp_base_dir, f"node_data_{start_port}")
    prep_log_file = os.path.join(temp_base_dir, "valkey_prep.log")

    # Start a server to create the RDB file
    process, client, node_info = start_valkey_server(
        port=start_port, conf_path=conf_path, data_dir=data_dir, log_file_path=prep_log_file,
        rdbcompression=rdbcompression, rdbchecksum=rdbchecksum
    )
    if not process:
        print("Failed to start Valkey server for RDB creation.", file=sys.stderr)
        return None

    try:
        populate_data_standalone(node_info, num_keys, key_value_size)
        print("Saving database to RDB File.")
        client.save()
        print("RDB file created successfully.")
    finally:
        stop_valkey_server(process, client, start_port)

    # --- 2. Profiling Phase ---
    print(f"\n{'='*60}")
    print("--- Phase 2: Profiling RDB Load Operation ---")
    print(f"{'='*60}")

    # Use a new, clean log file for the load test
    load_log_file = os.path.join(temp_base_dir, "valkey_load.log")
    if os.path.exists(load_log_file):
        os.remove(load_log_file)

    command = [
        VALKEY_SERVER_PATH, conf_path, "--port", str(start_port), "--dir", data_dir,
        "--logfile", load_log_file, "--rdbcompression", rdbcompression, "--rdbchecksum", rdbchecksum,
        "--save", ""
    ]

    load_process = None
    load_results = {}
    try:
        # Start the server and immediately begin profiling
        load_process = subprocess.Popen(command, preexec_fn=os.setsid)
        valkey_proc = psutil.Process(load_process.pid)
        
        # Start perf and capture initial metrics
        start_perf(load_process.pid, output_dir=FLAMEGRAPH_OUTPUT_DIR)
        cpu_times_before = valkey_proc.cpu_times()
        io_counters_before = valkey_proc.io_counters()
        ctx_switches_before = valkey_proc.num_ctx_switches()

        # Monitor for completion
        load_start_time = time.perf_counter()
        timeout, ready = 600, False
        while time.perf_counter() - load_start_time < timeout:
            if os.path.exists(load_log_file):
                with open(load_log_file, "r") as f:
                    if "Ready to accept connections" in f.read():
                        ready = True
                        break
            time.sleep(0.1)
        
        load_duration = time.perf_counter() - load_start_time
        
        # Stop perf and capture final metrics
        end_perf()
        if not ready:
            raise Exception(f"Timeout: Server did not finish loading RDB in {timeout}s.")
        
        cpu_times_after = valkey_proc.cpu_times()
        io_counters_after = valkey_proc.io_counters()
        memory_info_after = valkey_proc.memory_info()
        ctx_switches_after = valkey_proc.num_ctx_switches()

        print(f"RDB Load detected as complete in {load_duration:.4f} seconds.")

        # Calculate deltas for detailed metrics
        cpu_total_time = (cpu_times_after.user - cpu_times_before.user) + \
                         (cpu_times_after.system - cpu_times_before.system)
        
        load_results = {
            "load_duration_s": load_duration,
            "cpu_utilization": (cpu_total_time / load_duration) * 100 if load_duration > 0 else 0,
            "io_read_bytes": io_counters_after.read_bytes - io_counters_before.read_bytes,
            "memory_rss_bytes_after_load": memory_info_after.rss,
            "context_switches_voluntary": ctx_switches_after.voluntary - ctx_switches_before.voluntary,
            "context_switches_involuntary": ctx_switches_after.involuntary - ctx_switches_before.involuntary,
            "load_status": "ok"
        }

    finally:
        if load_process and load_process.poll() is None:
            load_process.terminate()
            load_process.wait()

    # --- 3. Analysis Phase ---
    print(f"\n{'='*60}")
    print("--- Phase 3: Generating Flame Graph ---")
    print(f"{'='*60}")
    flamegraph_file_name = f"load_{key_value_size}byte_values_{rdbcompression}_compression_{rdbchecksum}_checksum.svg"
    generate_flamegraph(output_dir=FLAMEGRAPH_OUTPUT_DIR, flamegraph_file_name=flamegraph_file_name)

    rdb_file_path = os.path.join(data_dir, "dump.rdb")
    rdb_file_size_bytes = os.path.getsize(rdb_file_path) if os.path.exists(rdb_file_path) else 0
    
    final_results = [{
        "keys": num_keys,
        "value_size": key_value_size,
        "rdb_file_size_bytes": rdb_file_size_bytes,
        "rdbcompression": rdbcompression,
        "rdbchecksum": rdbchecksum,
        **load_results # Unpack all the detailed metrics
    }]
    # Delete rdb file
    delete_file(rdb_file_path)
    print("\n--- RDB Load Flame Graph Generation Complete ---")
    return final_results


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
        default=os.path.join(os.getcwd(), f"{DEFAULT_TEMP_SUBDIR}_{time.time_ns()}"),
        help="Base directory for temporary data and logs for the cluster",
    )
    
    parser.add_argument(
        "--just-populate",
        type=bool,
        default=False,
        help="if True we will just populate the server with keys and exit",
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
    
    args = parser.parse_args()
    
    num_keys = int(args.num_keys * 1e6)
    global JUST_WRITE_KEYS
    JUST_WRITE_KEYS = args.just_populate

    print("--- Starting RDB Generate Flamegraph for RDB Load ---")
    print(f"Num Keys: {num_keys}")
    print(f"Key Size: {args.value_size}")
    print(f"RDB Snapshot Threads: {args.rdb_snapshot_threads}")
    print(f"rdbcompression: {args.rdbcompression}")
    print(f"rdbchecksum: {args.rdbchecksum}")
    results = generate_flamegraph_rdb_load(
        args.start_port, args.conf, num_keys, args.value_size, args.temp_dir, args.rdbcompression, args.rdbchecksum
    )
    csv_file_name = f"load_{args.value_size}byte_values_{args.rdbcompression}_compression_{args.rdbchecksum}_checksum.csv"
    save_results_to_csv(results, num_keys, args.value_size, output_dir=FLAMEGRAPH_OUTPUT_DIR, csv_file_name=csv_file_name)


if __name__ == "__main__":
    main()