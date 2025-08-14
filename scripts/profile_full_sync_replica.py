import logging
from pathlib import Path
import time
import subprocess
import valkey
import os
import psutil
from typing import Dict, Any

# --- Local Utility Imports ---
from utilities.parse_args import (
    parse_benchmark_args,
    display_config,
    setup_logging,
    colorize,
    LOG_COLORS,
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
from utilities.populate_server import KEY_SIZE_BYTES
from utilities.valkey_commands import get_db_key_count


PRIMARY_IP = "10.128.0.8"  # <<-- IMPORTANT: Use the IP of your primary VM
PRIMARY_PORT_DEFAULT = 7000
REPLICA_PORT_DEFAULT = 7001


def monitor_full_sync_via_logs(replica_log_path: Path, timeout_seconds: int = 3600):
    """
    Monitors the full synchronization process by watching the replica's log file.
    Returns the duration of the sync in seconds or None if it fails/times out.
    """
    logging.info(
        f"Monitoring replica log file at {replica_log_path} for sync completion."
    )
    start_time = time.monotonic()

    try:
        with open(replica_log_path, "r", encoding="utf-8") as log_file:
            log_file.seek(0, 2)
            while time.monotonic() - start_time < timeout_seconds:
                line = log_file.readline()
                if not line:
                    time.sleep(0.1)
                    continue

                if "PRIMARY <-> REPLICA sync: Finished with success" in line:
                    logging.info(colorize("Detected successful sync from log file.", LOG_COLORS.GREEN))
                    return time.monotonic() - start_time
                
                if ("Replication error" in line or "SYNC with master in non blocking mode failed" in line):
                    logging.error(f"Detected replication error in log: {line.strip()}")
                    return None

    except FileNotFoundError:
        logging.error(f"Replica log file not found at {replica_log_path}.")
    except Exception as e:
        logging.error(f"An error occurred while reading the replica log file: {e}", exc_info=True)

    logging.error(f"Full sync timed out after {timeout_seconds} seconds.")
    return None


def full_sync_replica_benchmark(config: BenchmarkConfig, output_dir: Path):
    all_results = []
    
    try:
        # Get the expected key count from the primary for verification
        primary_client = valkey.Valkey(host=PRIMARY_IP, port=PRIMARY_PORT_DEFAULT)
        num_keys_expected = get_db_key_count(primary_client)
    except Exception as e:
        logging.error(f"Could not connect to primary at {PRIMARY_IP}:{PRIMARY_PORT_DEFAULT}. Is it running and populated?", exc_info=True)
        return None

    thread_counts_to_test = [1, 2, 3, 4, 6, 8, 10]

    for num_threads in thread_counts_to_test:
        replica_process = None
        replica_client = None

        try:
            logging.info(colorize(f"--- Starting new full sync test for rdb-threads = {num_threads} ---", LOG_COLORS.GREEN))
            
            # Start a fresh Replica Server for this test
            replica_temp_dir = Path(config.temp_dir) / f"replica_data_{REPLICA_PORT_DEFAULT}_threads_{num_threads}"
            setup_directory_for_run(str(replica_temp_dir))
            
            replica_config = BenchmarkConfig(**config.__dict__)
            replica_config.start_port = REPLICA_PORT_DEFAULT
            replica_config.temp_dir = str(replica_temp_dir)
            replica_config.bind = "0.0.0.0"
            
            replica_process = start_standalone_valkey_server(replica_config)
            if not replica_process:
                continue
            replica_client = wait_for_server_to_start(replica_config)
            if not replica_client:
                continue

            # Capture metrics before sync starts
            replica_proc = psutil.Process(replica_process.pid)
            replica_io_before = replica_proc.io_counters()
            replica_cpu_before = replica_proc.cpu_times()

            # Initiate Full Sync
            sync_start_time = time.monotonic()
            logging.info(f"Initiating full sync to primary at {PRIMARY_IP}:{PRIMARY_PORT_DEFAULT}")
            replica_client.replicaof(PRIMARY_IP, PRIMARY_PORT_DEFAULT)
            
            # Monitor Sync Progress via logs
            replica_log_file = Path(replica_temp_dir) / f"node_log_{REPLICA_PORT_DEFAULT}.log"
            sync_duration = monitor_full_sync_via_logs(replica_log_file)

            if sync_duration is None:
                logging.error("Full sync did not complete successfully.")
                continue

            # Capture metrics after sync completes
            sync_end_time = time.monotonic()
            total_time = sync_end_time - sync_start_time
            replica_io_after = replica_proc.io_counters()
            replica_cpu_after = replica_proc.cpu_times()

            # Verify Replica Key Count
            final_replica_key_count = get_db_key_count(replica_config)
            if final_replica_key_count != num_keys_expected:
                logging.error(f"Replica key count mismatch: Expected {num_keys_expected:,} keys but DB has {final_replica_key_count:,}.")
                continue
            logging.info(f"Replica successfully synced with {final_replica_key_count:,} keys.")
            
            # Calculate deltas and collect results
            replica_cpu_time = (replica_cpu_after.user - replica_cpu_before.user) + (replica_cpu_after.system - replica_cpu_before.system)
            replica_io_read_bytes = replica_io_after.read_bytes - replica_io_before.read_bytes
            replica_throughput_mb_s = (replica_io_read_bytes / total_time) * 1e-6 if total_time > 0 else 0
            
            all_results.append({
                "test_type": "replica_full_sync",
                "rdb_threads": num_threads,
                "num_keys_millions": config.num_keys_millions,
                "value_size_bytes": config.value_size_bytes,
                "sync_duration_seconds": total_time,
                "primary_ip": PRIMARY_IP,
                "replica_ip": "10.128.0.9", # Or get it programmatically
                "replica_port": REPLICA_PORT_DEFAULT,
                "replica_cpu_time_seconds": replica_cpu_time,
                "replica_io_read_mb_s": replica_throughput_mb_s,
                "status": "success",
            })
            
        except Exception as e:
            logging.critical(f"An unhandled exception occurred during iteration for rdb-threads={num_threads}.", exc_info=True)
        finally:
            if replica_process:
                logging.info(f"Stopping Replica Valkey server for {num_threads} threads.")
                stop_valkey_server(replica_process, replica_client)
    
    return all_results


def main():
    config = parse_benchmark_args()
    setup_logging(config.log_file)
    display_config(config)

    try:
        run_id = time.strftime("%Y%m%d_%H%M%S")
        project_root = Path(__file__).resolve().parents[1]
        dir_name = "multi_vm_replica_benchmark"
        output_dir = project_root / "results" / f"{dir_name}_{run_id}"
        output_dir.mkdir(parents=True, exist_ok=True)
        logging.info(f"All output for this run will be saved in: {output_dir}")
    except Exception as e:
        logging.critical(f"Failed to create output directory. Aborting. Error: {e}", exc_info=True)
        return

    logging.info("--- Starting Multi-VM Replica Benchmark ---")
    results = full_sync_replica_benchmark(config, output_dir=output_dir)

    if results:
        logging.info(f"Benchmark finished. Collected {len(results)} results.")
        csv_file_name = f"replica_sync_summary_{config.num_keys_millions}keys_{config.value_size_bytes}B.csv"
        save_results_to_csv(
            results=results,
            output_dir=str(output_dir),
            file_name=csv_file_name,
        )
    else:
        logging.error("Replica benchmark failed to produce any results.")


if __name__ == "__main__":
    main()