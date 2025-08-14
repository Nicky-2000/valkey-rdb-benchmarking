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
from utilities.populate_server import populate_data_standalone
from utilities.valkey_commands import get_db_key_count


PRIMARY_PORT_DEFAULT = 7000

def profile_primary_bgsave(
    primary_process: subprocess.Popen,
    primary_log_file: Path
) -> Dict[str, Any]:
    """
    Monitors the primary's log file for BGSAVE start and end markers and
    profiles system metrics during that period.
    """
    logging.info(f"Monitoring primary log file at {primary_log_file} for BGSAVE.")
    
    primary_proc = psutil.Process(primary_process.pid)
    
    start_time = None
    bgsave_duration = None
    
    try:
        with open(primary_log_file, "r", encoding="utf-8") as log_file:
            log_file.seek(0, 2)
            while True:
                line = log_file.readline()
                if not line:
                    time.sleep(0.1)
                    continue

                if start_time is None and "Starting BGSAVE" in line:
                    start_time = time.monotonic()
                    logging.info(colorize("Detected 'Starting BGSAVE' in primary log. Starting profiling.", LOG_COLORS.GREEN))
                    io_before = primary_proc.io_counters()
                    cpu_before = primary_proc.cpu_times()
                    continue

                if start_time is not None and "Ending BGSAVE" in line:
                    bgsave_duration = time.monotonic() - start_time
                    logging.info(colorize("Detected 'Ending BGSAVE' in primary log. Stopping profiling.", LOG_COLORS.GREEN))
                    io_after = primary_proc.io_counters()
                    cpu_after = primary_proc.cpu_times()
                    break
    
    except FileNotFoundError:
        logging.error(f"Primary log file not found at {primary_log_file}.")
        return {"status": "error", "error_message": "Log file not found"}
    except Exception as e:
        logging.error(f"An error occurred while reading the primary log: {e}", exc_info=True)
        return {"status": "error", "error_message": str(e)}

    if bgsave_duration is None:
        logging.error("BGSAVE start or end log line not found within timeout.")
        return {"status": "error", "error_message": "BGSAVE markers not found"}
        
    cpu_time_seconds = (cpu_after.user - cpu_before.user) + (cpu_after.system - cpu_before.system)
    io_write_bytes = io_after.write_bytes - io_before.write_bytes
    throughput_mb_s = (io_write_bytes / bgsave_duration) * 1e-6 if bgsave_duration > 0 else 0

    return {
        "status": "ok",
        "bgsave_duration_seconds": bgsave_duration,
        "primary_cpu_time_seconds": cpu_time_seconds,
        "primary_io_write_mb_s": throughput_mb_s,
    }

def run_primary_benchmark(config: BenchmarkConfig, output_dir: Path):
    primary_process = None
    primary_client = None
    all_results = []
    thread_counts_to_test = [1, 2, 3, 4, 6, 8, 10]

    try:
        # --- 1. Initial Setup and Server Start (Primary) - **Run once** ---
        primary_temp_dir = Path(config.temp_dir) / f"primary_data_{PRIMARY_PORT_DEFAULT}"
        setup_directory_for_run(str(primary_temp_dir))
        
        logging.info(f"Starting Primary Valkey server on port {PRIMARY_PORT_DEFAULT}...")
        
        primary_config = BenchmarkConfig(**config.__dict__)
        primary_config.start_port = PRIMARY_PORT_DEFAULT
        primary_config.temp_dir = str(primary_temp_dir)
        primary_config.repl_diskless_sync = True
        primary_config.bind = "0.0.0.0"
        
        primary_process = start_standalone_valkey_server(primary_config)
        if not primary_process:
            return None
        primary_client = wait_for_server_to_start(primary_config)
        if not primary_client:
            return None
        logging.info("Primary Valkey server started.")
        
        # --- 2. Populate Data on Primary - **Run once** ---
        logging.info(f"Populating {config.num_keys_millions}M keys on Primary server...")
        populate_data_standalone(config, return_keys=False)
        num_keys_expected = primary_config.num_keys_millions * 1e6
        initial_primary_key_count = get_db_key_count(primary_config)
        
        if initial_primary_key_count != num_keys_expected:
            logging.error(f"Primary key population mismatch: Expected {num_keys_expected:,} keys but DB has {initial_primary_key_count:,}.")
            return None
        logging.info(f"Primary server populated with {initial_primary_key_count:,} keys.")
        
        # --- 3. Loop through thread counts and wait for syncs ---
        for num_threads in thread_counts_to_test:
            logging.info(colorize(f"--- Primary ready for test with rdb-threads = {num_threads} ---", LOG_COLORS.CYAN))
            
            # Use CONFIG SET to change the thread count without restarting
            primary_client.config_set("rdb-threads", num_threads)
            
            logging.info(colorize(f"Waiting for replica to connect and trigger a full sync...", LOG_COLORS.CYAN))
            
            primary_log_file = Path(primary_config.temp_dir) / f"node_log_{PRIMARY_PORT_DEFAULT}.log"
            start_wait_time = time.monotonic()
            
            # Wait for replica to connect
            while time.monotonic() - start_wait_time < 3600:
                info = primary_client.info('replication')
                if info.get('loading') == '1' or info.get('master_sync_in_progress') == '1':
                    logging.info(colorize("Detected a full sync request from a replica.", LOG_COLORS.GREEN))
                    break
                time.sleep(1)
            else:
                logging.error("Timeout: No replica connected within the allowed time. Aborting this test.")
                continue
                
            # Profile the BGSAVE that was triggered by the replica
            profiling_results = profile_primary_bgsave(primary_process, primary_log_file)
            
            if profiling_results['status'] == 'ok':
                all_results.append({
                    "test_type": "primary_bgsave",
                    "rdb_threads": num_threads,
                    "num_keys_millions": config.num_keys_millions,
                    "value_size_bytes": config.value_size_bytes,
                    "primary_port": PRIMARY_PORT_DEFAULT,
                    **profiling_results,
                })
                logging.info(colorize("Primary BGSAVE profiling complete.", LOG_COLORS.GREEN))
            else:
                logging.error("Primary BGSAVE profiling failed.")
                continue
                
            # Wait for a brief period to allow the replica to finish
            time.sleep(10)
            # You can add a check here to ensure the replica is disconnected
            # primary_client.replicaof("NO ONE") can be used to reset the primary's replication state

        return all_results

    except Exception as e:
        logging.critical("An unhandled exception occurred during the primary benchmark.", exc_info=True)
        return None
    finally:
        # --- Final Cleanup of Primary - **Run once** ---
        if primary_process:
            logging.info("--- Final cleanup: Stopping Primary Valkey server. ---")
            stop_valkey_server(primary_process, primary_client)


def main():
    config = parse_benchmark_args()
    setup_logging(config.log_file)
    display_config(config)

    try:
        run_id = time.strftime("%Y%m%d_%H%M%S")
        project_root = Path(__file__).resolve().parents[1]
        dir_name = "multi_vm_primary_benchmark"
        output_dir = project_root / "results" / f"{dir_name}_{run_id}"
        output_dir.mkdir(parents=True, exist_ok=True)
        logging.info(f"All output for this run will be saved in: {output_dir}")
    except Exception as e:
        logging.critical(f"Failed to create output directory. Aborting. Error: {e}", exc_info=True)
        return

    logging.info("--- Starting Multi-VM Primary Benchmark ---")
    results = run_primary_benchmark(config, output_dir=output_dir)

    if results:
        logging.info(f"Benchmark finished. Collected {len(results)} results.")
        csv_file_name = f"primary_bgsave_summary_{config.num_keys_millions}keys_{config.value_size_bytes}B.csv"
        save_results_to_csv(
            results=results,
            output_dir=str(output_dir),
            file_name=csv_file_name,
        )
    else:
        logging.error("Primary benchmark failed to produce any results.")


if __name__ == "__main__":
    main()