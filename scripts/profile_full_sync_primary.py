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


import logging
from pathlib import Path
import time
import subprocess
import psutil
import re  # Import the regex module
from typing import Dict, Any


import logging
from pathlib import Path
import time
import subprocess
import psutil
import re
from typing import Dict, Any

def profile_primary_bgsave(
    primary_process: subprocess.Popen,
    primary_log_file: Path
) -> Dict[str, Any]:
    """
    Monitors the primary's log file for the child process PID and profiles its
    system metrics during the diskless RDB transfer.
    """
    logging.info(f"Monitoring primary log file at {primary_log_file} for BGSAVE child process.")
    
    child_pid = None
    logging.info("PRIMARY LOG FILE: ")
    logging.info(primary_log_file)
    try:
        # --- 1. Find the PID of the child process from the log ---
        logging.info("Tailing log file to find BGSAVE child PID...")
        with open(primary_log_file, "r", encoding="utf-8") as log_file:
            log_file.seek(0, 2)
            while child_pid is None:
                line = log_file.readline()
                if not line:
                    time.sleep(0.1)
                    continue
                logging.info(f"Log Line: {line}")
                match = re.search(r'started by pid (\d+) to pipe', line)
                if match:
                    child_pid = int(match.group(1))
                    logging.info(colorize(f"Detected BGSAVE child process with PID: {child_pid}.", LOG_COLORS.GREEN))
                    break
        
        if child_pid is None:
            logging.error("Failed to find BGSAVE child PID in log.")
            return {"status": "error", "error_message": "Child PID not found"}

        logging.info(f"Creating psutil.Process object for PID: {child_pid}...")
        child_proc = psutil.Process(child_pid)
        logging.info("psutil.Process object created successfully.")
        
        # --- 2. Profile the child process ---
        logging.info("Attempting to get initial metrics...")
        start_time = time.monotonic()
        io_before = child_proc.io_counters()
        cpu_before = child_proc.cpu_times()
        logging.info("Initial metrics retrieved. Waiting for process to exit...")

        # Wait for the child process to exit. This is a blocking call.
        child_proc.wait()
        
        logging.info("BGSAVE child process has exited. Getting final metrics...")
        bgsave_duration = time.monotonic() - start_time
        # IMPORTANT: You must get the final metrics *after* the process has exited.
        # Calling io_counters() or cpu_times() on a dead process can raise an error
        # psutil.wait() returns the exit code, but we still need the final metrics.
        # Let's re-get the Process object, which might be a 'ZombieProcess'
        try:
            # We need to re-instantiate the Process object to get final metrics
            final_child_proc = psutil.Process(child_pid)
            io_after = final_child_proc.io_counters()
            cpu_after = final_child_proc.cpu_times()
        except psutil.NoSuchProcess:
            logging.warning("Process disappeared before final metrics could be captured. Metrics may be inaccurate.")
            io_after = io_before # Use a fallback
            cpu_after = cpu_before
        
    except FileNotFoundError:
        logging.error(f"Primary log file not found at {primary_log_file}.")
        return {"status": "error", "error_message": "Log file not found"}
    except psutil.NoSuchProcess as e:
        # This is a key error to catch - it means the process exited too fast
        logging.error(f"BGSAVE child process with PID {child_pid} was not found. It may have exited before profiling could begin.", exc_info=True)
        return {"status": "error", "error_message": f"Process not found: {e}"}
    except Exception as e:
        logging.error(f"An error occurred while profiling the child process: {e}", exc_info=True)
        return {"status": "error", "error_message": str(e)}

    if bgsave_duration is None:
        logging.error("BGSAVE process duration could not be determined.")
        return {"status": "error", "error_message": "BGSAVE duration not found"}
    
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
        initial_primary_key_count = get_db_key_count(primary_client)
        
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
            # start_wait_time = time.monotonic()
            
            # Wait for replica to connect
            # while time.monotonic() - start_wait_time < 3600:
            #     info = primary_client.info('replication')
            #     if info.get('loading') == '1' or info.get('master_sync_in_progress') == '1':
            #         logging.info(colorize("Detected a full sync request from a replica.", LOG_COLORS.GREEN))
            #         break
            #     time.sleep(1)
            # else:
            #     logging.error("Timeout: No replica connected within the allowed time. Aborting this test.")
            #     continue
                
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
            time.sleep(5)
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