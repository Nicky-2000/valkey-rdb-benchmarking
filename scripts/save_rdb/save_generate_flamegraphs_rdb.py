import subprocess
import time
import os
import sys
import signal
import logging
from utilities.parse_args import parse_benchmark_args, BenchmarkConfig, display_config
from utilities.file_system_utilities import setup_directory_for_run, delete_file
from utilities.valkey_server_utilities import (
    start_standalone_valkey_server,
    stop_valkey_server,
    wait_for_server_to_start,
)
from utilities.populate_server import populate_data_standalone
from utilities.valkey_commands import (
    get_db_key_count,
    trigger_blocking_save,
    verify_data,
)


import pandas as pd
from utilities.benchmarking_helpers import (
    start_valkey_server,
    stop_valkey_server,
    run_single_node_save,
    populate_data_standalone,
    get_db_key_count,
    save_results_to_csv,
)


# --- Configuration Constants ---
VALKEY_SERVER_PATH = "../src/valkey-server"
VALKEY_CLI_PATH = "../src/valkey-cli"
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
        preexec_fn=os.setsid,  # Start in a new process group
    )
    print("Perf process started.")


def end_perf():
    global perf_process
    if perf_process:
        print("Stopping perf process...")
        try:
            os.killpg(
                os.getpgid(perf_process.pid), signal.SIGINT
            )  # Send Ctrl+C # <--- Change here
            perf_process.wait(timeout=10)  # Wait for it to terminate
            print("Perf process stopped.")
        except (subprocess.TimeoutExpired, ProcessLookupError) as e:
            print(f"Error stopping perf process: {e}. Attempting to terminate.")
            perf_process.terminate()
            perf_process.wait()
        perf_process = None
    else:
        print("Perf process was not running.")


def generate_flamegraph(
    output_dir=FLAMEGRAPH_OUTPUT_DIR, flamegraph_file_name="save_flamegraph.svg"
):
    print("Generating flame graph...")
    perf_data_path = os.path.join(output_dir, "perf.data")
    perf_stacks_path = os.path.join(output_dir, "valkey.perf.stacks")
    folded_stacks_path = os.path.join(output_dir, "valkey.folded.stacks")
    svg_output_path = os.path.join(output_dir, flamegraph_file_name)

    if not os.path.exists(perf_data_path):
        print(
            f"Error: perf.data not found at {perf_data_path}. Cannot generate flame graph."
        )
        return

    try:
        # perf script
        print(f"Running: perf script -i {perf_data_path} > {perf_stacks_path}")
        with open(perf_stacks_path, "w") as f:
            subprocess.run(
                [PERF_PATH, "script", "-i", perf_data_path], check=True, stdout=f
            )

        # stackcollapse-perf.pl
        stackcollapse_script = f"{FLAME_GRAPH_REPO_PATH}/stackcollapse-perf.pl"
        if not os.path.exists(stackcollapse_script):
            print(
                f"Error: stackcollapse-perf.pl not found at {stackcollapse_script}. Please ensure FlameGraph repository is cloned correctly."
            )
            return
        print(
            f"Running: {stackcollapse_script} {perf_stacks_path} > {folded_stacks_path}"
        )
        with open(folded_stacks_path, "w") as f:
            subprocess.run(
                [stackcollapse_script, perf_stacks_path], check=True, stdout=f
            )

        # flamegraph.pl
        flamegraph_script = f"{FLAME_GRAPH_REPO_PATH}/flamegraph.pl"
        if not os.path.exists(flamegraph_script):
            print(
                f"Error: flamegraph.pl not found at {flamegraph_script}. Please ensure FlameGraph repository is cloned correctly."
            )
            return
        print(f"Running: {flamegraph_script} {folded_stacks_path} > {svg_output_path}")
        with open(svg_output_path, "w") as f:
            subprocess.run(
                [flamegraph_script, folded_stacks_path], check=True, stdout=f
            )

        print(f"Flame graph generated successfully at {svg_output_path}")

    except subprocess.CalledProcessError as e:
        print(f"Error during flame graph generation: {e}")
    except FileNotFoundError as e:
        print(
            f"Command not found: {e}. Make sure 'perf' is installed and FlameGraph scripts are executable and in the correct path."
        )



def generate_flamegraph_rdb_save(config: BenchmarkConfig):
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
        rdbchecksum=rdbchecksum,
    )
    if not process:
        print("Failed to start Valkey server. Aborting benchmark.", file=sys.stderr)
        return None

    try:
        populate_data_standalone(node_info, num_keys, key_value_size)

        num_keys_initial = get_db_key_count(start_port)

        assert num_keys == num_keys_initial, (
            f"num_keys does not match the actual number of keys on the db num_keys: {num_keys}, num_keys_db: {num_keys_initial}"
        )

        print(f"\n--- Keys before save = {num_keys_initial} ---\n")
        results = []
        for num_threads in [1, 2, 4, 6, 8, 10]:
            client.config_set("rdb-snapshot-threads", num_threads)

            # Run BGSAVE on the single node using the already connected client
            print(f"\n{'=' * 60}")
            print(f"--- TEST ITERATION: rdb-snapshot-threads = {num_threads} ---")
            print(f"{'=' * 60}")

            # Start Perf on the process
            print(f"Valkey Server PID: {process.pid}")
            start_perf(process.pid, output_dir=FLAMEGRAPH_OUTPUT_DIR)

            save_results = run_single_node_save(
                client, process, temp_base_dir, DEFAULT_LOG_FILE
            )

            # End perf on the process.
            end_perf()

            # Generate Flame Graph in this folder
            flamegraph_file_name = f"save_{key_value_size}byte_values_{rdbcompression}_compression_{rdbchecksum}_checksum_{num_threads}_threads.svg"
            generate_flamegraph(
                output_dir=FLAMEGRAPH_OUTPUT_DIR,
                flamegraph_file_name=flamegraph_file_name,
            )

            # Save the rest of the data.
            # Calculate throughput, handling potential division by zero
            save_duration = save_results["client_save_duration"]
            throughput = 0
            actual_throughput = 0
            if save_results["save_status"] == "ok" and save_duration > 0:
                throughput = ((num_keys * key_value_size) / save_duration) * (10**-6)
                actual_throughput = (save_results["io_write_bytes"] / save_duration) * (
                    10**-6
                )

            # --- Get RDB File Size ---
            rdb_file_path = os.path.join(data_dir, "dump.rdb")
            rdb_file_size_bytes = 0
            if os.path.exists(rdb_file_path):
                rdb_file_size_bytes = os.path.getsize(rdb_file_path)
            else:
                print(
                    f"Warning: RDB file not found at {rdb_file_path}. Cannot get its size.",
                    file=sys.stderr,
                )

            # Aggregate all results
            new_result = {
                "keys": num_keys,
                "value_size": key_value_size,
                "num_threads": num_threads,
                **save_results,  # Unpack the results
                "key_bytes_throughput(M/s)": throughput,
                "actual_throughput(M/s)": actual_throughput,
                "rdb_file_size_bytes": rdb_file_size_bytes,
            }
            results.append(new_result)

            print(f"\n--- Cleaning up dump.rdb for {num_threads} threads... ---")
            delete_file(rdb_file_path)
            print(f"--- DONE with {num_threads} threads. ---")

        print(f"\n{'=' * 50}")
        print("--- All Tests Completed ---")
        print(f"Total results collected: {len(results)}")
        print(f"{'=' * 50}\n")

        return results

    finally:
        stop_valkey_server(process, client, start_port)


def main():
    config = parse_benchmark_args()
    display_config(config)

    num_keys = int(args.num_keys * 1e6)

    print("--- Generating Flamegraph for RDB Save Standalone Valkey ---")
    results = generate_flamegraph_rdb_save(
        args.start_port,
        args.conf,
        num_keys,
        args.value_size,
        temp_dir,
        args.rdb_snapshot_threads,
        args.rdbcompression,
        args.rdbchecksum,
    )
    csv_file_name = f"save_{args.value_size}byte_values_{args.rdbcompression}_compression_{args.rdbchecksum}_checksum.csv"
    save_results_to_csv(
        results,
        num_keys,
        args.value_size,
        output_dir=FLAMEGRAPH_OUTPUT_DIR,
        csv_file_name=csv_file_name,
    )


if __name__ == "__main__":
    main()
