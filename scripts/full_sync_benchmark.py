import logging
from pathlib import Path
import time
import subprocess
import valkey
import os

# --- Local Utility Imports ---
from utilities.parse_args import (
    parse_benchmark_args,
    display_config,
    setup_logging,
    BenchmarkConfig,
)
from utilities.file_system_utilities import (
    save_results_to_csv,
    setup_directory_for_run,
    delete_file,
)
from utilities.valkey_server_utilities import (
    start_standalone_valkey_server,
    stop_valkey_server,
    wait_for_server_to_start,
)
from utilities.populate_server import populate_data_standalone, populate_data_with_benchmark, KEY_SIZE_BYTES
from utilities.valkey_commands import get_db_key_count, profile_blocking_save

# --- New Imports for Full Sync Benchmark ---
from concurrent.futures import ThreadPoolExecutor

# Define default ports for primary and replica
PRIMARY_PORT_DEFAULT = 7000
REPLICA_PORT_DEFAULT = 7001


def run_valkey_benchmark_get(
    host: str,
    port: int,
    num_keys: int,
    threads: int,
    clients: int,
    pipeline: int,
    runtime_seconds: int = 60,
):
    """
    Runs valkey-benchmark with GET commands in a separate process.
    This is a non-blocking call.
    """
    valkey_benchmark_path = os.environ.get("VALKEY_BENCHMARK_PATH")
    if not valkey_benchmark_path:
        logging.error("VALKEY_BENCHMARK_PATH environment variable is not set.")
        return None

    command = [
        valkey_benchmark_path,
        "-h",
        host,
        "-p",
        str(port),
        "-t",
        "GET",
        "-n",
        str(num_keys),
        "--threads",
        str(threads),
        "-c",
        str(clients),
        "-P",
        str(pipeline),
        "-r",
        str(num_keys),  # Random keys within the populated range
        "--csv",  # Output in CSV format for easier parsing if needed
        "--stat-interval",
        "1",  # Report stats every second
        "--raw",  # Raw output for easier parsing
        "--csv",  # Output in CSV format
        "--no-summary",  # Don't print final summary, we'll parse raw output
        "--timeout",
        str(runtime_seconds + 10),  # Add a buffer for timeout
    ]

    logging.info(f"Starting concurrent GET benchmark: {' '.join(command)}")
    # Use Popen to run in the background
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        universal_newlines=True,
    )
    return process


import logging
import time
import subprocess
import valkey
from pathlib import Path

# Assume get_db_key_count is an existing function


def monitor_full_sync_via_logs(replica_log_path: Path, timeout_seconds: int = 3600):
    """
    Monitors the full synchronization process by watching the replica's log file.
    Returns the duration of the sync in seconds or None if it fails/times out.
    """
    logging.info(
        f"Monitoring replica log file at {replica_log_path} for sync completion."
    )
    print(replica_log_path)
    start_time = time.monotonic()

    try:
        # Open the log file and seek to the end
        with open(replica_log_path, "r", encoding="utf-8") as log_file:
            log_file.seek(0, 2)  # Go to the end of the file

            while time.monotonic() - start_time < timeout_seconds:
                line = log_file.readline()
                if not line:
                    time.sleep(0.1)  # Wait for new log entries
                    continue

                # Check for the sync completion message
                if "PRIMARY <-> REPLICA sync: Finished with success" in line:
                    # Sync is complete, now check if the key count matches.
                    # This requires a client connection, which isn't in this function's signature
                    # So for this log-only version, we'll assume a successful log line implies success.
                    # In a real script, you'd add a separate client-based verification step here.
                    logging.info("Detected successful sync from log file.")
                    return time.monotonic() - start_time

                # You could also add checks for errors
                if (
                    "Replication error" in line
                    or "SYNC with master in non blocking mode failed" in line
                ):
                    logging.error(f"Detected replication error in log: {line.strip()}")
                    return None

    except FileNotFoundError:
        logging.error(f"Replica log file not found at {replica_log_path}.")
        return None
    except Exception as e:
        logging.error(
            f"An error occurred while reading the replica log file: {e}", exc_info=True
        )
        return None

    logging.error(f"Full sync timed out after {timeout_seconds} seconds.")
    return None


def full_sync_benchmark(config: BenchmarkConfig, output_dir: Path):
    """
    Runs a benchmark for Valkey full synchronization.
    This version keeps the primary server running for multiple replica tests.
    """
    primary_process = None
    primary_client = None
    all_results = []

    # List of thread counts to test
    thread_counts_to_test = [1, 2, 3, 4, 6, 8, 10]

    try:
        # --- 1. Initial Setup and Server Start (Primary) ---
        primary_temp_dir = (
            Path(config.temp_dir) / f"primary_data_{PRIMARY_PORT_DEFAULT}"
        )
        setup_directory_for_run(str(primary_temp_dir))

        logging.info(
            f"Starting Primary Valkey server on port {PRIMARY_PORT_DEFAULT}..."
        )
        primary_config = BenchmarkConfig(**config.__dict__)
        primary_config.start_port = PRIMARY_PORT_DEFAULT
        primary_config.temp_dir = str(primary_temp_dir)

        # Configure primary for diskless sync if not already set in the conf file
        primary_config.repl_diskless_sync = (
            True  # This needs to be a valid config attribute
        )

        primary_process = start_standalone_valkey_server(primary_config)
        if not primary_process:
            return None
        primary_client = wait_for_server_to_start(primary_config)
        if not primary_client:
            return None
        logging.info("Primary Valkey server started.")

        # --- 2. Populate Data on Primary (Only runs once) ---
        logging.info(
            f"Populating {config.num_keys_millions}M keys on Primary server..."
        )
        populate_data_standalone(config, return_keys=False)
        num_keys_expected = primary_config.num_keys_millions * 1e6
        initial_primary_key_count = get_db_key_count(primary_config)

        if initial_primary_key_count != num_keys_expected:
            logging.error(
                f"Primary key population mismatch: Expected {num_keys_expected:,} keys but DB has {initial_primary_key_count:,}."
            )
            return None
        logging.info(
            f"Primary server populated with {initial_primary_key_count:,} keys."
        )

        # --- 3. Run Benchmark Loop for each rdb-threads setting ---
        for num_threads in thread_counts_to_test:
            replica_process = None
            replica_client = None
            read_benchmark_process = None

            try:
                logging.info(
                    f"--- Starting new full sync test for rdb-threads = {num_threads} ---"
                )

                # Configure the primary server's rdb-threads for this iteration
                primary_client.config_set("rdb-threads", num_threads)

                # --- 4. Start a fresh Replica Server for this test ---
                replica_port = REPLICA_PORT_DEFAULT
                replica_temp_dir = (
                    Path(config.temp_dir)
                    / f"replica_data_{replica_port}_threads_{num_threads}"
                )
                setup_directory_for_run(str(replica_temp_dir))

                replica_config = BenchmarkConfig(**config.__dict__)
                replica_config.start_port = replica_port
                replica_config.temp_dir = str(replica_temp_dir)

                replica_process = start_standalone_valkey_server(replica_config)
                if not replica_process:
                    continue  # Skip to next thread count if replica fails to start
                replica_client = wait_for_server_to_start(replica_config)
                if not replica_client:
                    continue

                # --- 5. Initiate Full Sync ---
                initial_replica_key_count = get_db_key_count(replica_config)
                assert initial_replica_key_count == 0

                logging.info(
                    f"Initiating full sync: Replica ({replica_port}) REPLICAOF Primary ({PRIMARY_PORT_DEFAULT})..."
                )
                replica_client.replicaof("127.0.0.1", PRIMARY_PORT_DEFAULT)

                # --- 6. Monitor Sync Progress via logs ---
                # This assumes your start_standalone_valkey_server function
                # configures the server to log to a file named 'valkey_server.log'
                replica_log_file = Path(replica_temp_dir) / "node_log_7001.log"
                sync_duration = monitor_full_sync_via_logs(
                    replica_log_file,
                )

                if sync_duration is None:
                    logging.error("Full sync did not complete successfully.")
                    continue

                logging.info(f"Full sync completed in {sync_duration:.2f} seconds.")

                # --- 7. Verify Replica Key Count ---
                final_replica_key_count = get_db_key_count(replica_config)
                if final_replica_key_count != num_keys_expected:
                    logging.error(
                        f"Replica key count mismatch after sync: Expected {num_keys_expected:,} keys but DB has {final_replica_key_count:,}."
                    )
                    continue
                logging.info(
                    f"Replica successfully synced with {final_replica_key_count:,} keys."
                )

                # --- 8. Collect Results ---
                all_results.append(
                    {
                        "test_type": "full_sync",
                        "rdb_threads": num_threads,
                        "num_keys_millions": config.num_keys_millions,
                        "value_size_bytes": config.value_size_bytes,
                        "sync_duration_seconds": sync_duration,
                        "primary_port": PRIMARY_PORT_DEFAULT,
                        "replica_port": replica_port,
                        "status": "success",
                    }
                )
            except Exception as e:
                logging.critical(
                    f"An unhandled exception occurred during iteration for rdb-threads={num_threads}.",
                    exc_info=True,
                )
            finally:
                # --- Cleanup Replica for this iteration ---
                if replica_process:
                    logging.info(
                        f"Stopping Replica Valkey server for {num_threads} threads."
                    )
                    stop_valkey_server(replica_process, replica_client)

        logging.info("--- All full sync benchmark iterations completed ---")
        return all_results

    except Exception as e:
        logging.critical(
            "An unhandled exception occurred during the full sync benchmark.",
            exc_info=True,
        )
        return None
    finally:
        # --- Final Cleanup of Primary ---
        if primary_process:
            logging.info("--- Final cleanup: Stopping Primary Valkey server. ---")
            stop_valkey_server(primary_process, primary_client)


def main():
    """Main entry point for the full sync benchmark script."""
    config = parse_benchmark_args()
    setup_logging(config.log_file)
    display_config(config)

    # --- Create a unique, timestamped directory for this run's output ---
    try:
        run_id = time.strftime("%Y%m%d_%H%M%S")
        project_root = Path(__file__).resolve().parents[1]
        dir_name = "full_sync_benchmark"
        output_dir = project_root / "results" / f"{dir_name}_{run_id}"
        output_dir.mkdir(parents=True, exist_ok=True)
        logging.info(f"All output for this run will be saved in: {output_dir}")
    except Exception as e:
        logging.critical(
            f"Failed to create output directory. Aborting. Error: {e}", exc_info=True
        )
        return

    logging.info("--- Starting Full Sync Benchmark ---")
    results = full_sync_benchmark(config, output_dir=output_dir)

    if results:
        logging.info(f"Benchmark finished. Collected {len(results)} results.")

        csv_file_name = f"full_sync_summary_{config.num_keys_millions}keys_{config.value_size_bytes}B.csv"

        save_results_to_csv(
            results=results,
            output_dir=str(output_dir),
            file_name=csv_file_name,
        )
    else:
        logging.error("Full sync benchmark failed to produce any results.")


if __name__ == "__main__":
    main()
