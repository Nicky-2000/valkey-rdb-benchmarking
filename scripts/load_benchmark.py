import logging
import os
import time
import psutil
from pathlib import Path
import re # Import the regex module

# --- Local Utility Imports ---
from utilities.parse_args import (
    LOG_COLORS,
    colorize,
    parse_benchmark_args,
    display_config,
    setup_logging,
    BenchmarkConfig,
)
from utilities.file_system_utilities import delete_file, save_results_to_csv, setup_directory_for_run
from utilities.valkey_server_utilities import (
    start_standalone_valkey_server,
    stop_valkey_server,
    wait_for_server_to_start,
)
from utilities.populate_server import populate_data_standalone
from utilities.valkey_commands import get_db_key_count, trigger_blocking_save
from utilities.flamegraph_profiler import FlamegraphProfiler


def prepare_rdb_file(config: BenchmarkConfig):
    """
    Starts a temporary server to create and save an RDB file for the load test.
    """
    logging.info("--- Phase 1: Preparing RDB file for load test ---")
    process = None
    client = None
    try:
        # Start a server instance with a clean data directory
        config.rdb_threads = 10
        process = start_standalone_valkey_server(config, clear_data_dir=True)
        if not process:
            return False

        client = wait_for_server_to_start(config)
        if not client:
            return False

        # Populate with data and save to disk
        populate_data_standalone(config)
        trigger_blocking_save(client)
        logging.info("RDB file created successfully.")
        return True
    except Exception as e:
        logging.critical(f"Failed to prepare RDB file. Error: {e}", exc_info=True)
        return False
    finally:
        if process:
            stop_valkey_server(process, client)


import threading
import statistics

def monitor_system_metrics(pid, metrics_list: list, stop_event, sample_interval=0.5):
    """
    Monitors system metrics for a given process ID until a stop event is set.
    """
    try:
        proc = psutil.Process(pid)
        while not stop_event.is_set():
            cpu_percent = proc.cpu_percent(interval=None)
            memory_info = proc.memory_info()
            io_counters = proc.io_counters()
            
            metrics_list.append({
                'cpu_percent': cpu_percent,
                'memory_rss_bytes': memory_info.rss,
                'read_bytes': io_counters.read_bytes,
                'write_bytes': io_counters.write_bytes,
            })
            time.sleep(sample_interval)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        logging.warning(f"Could not monitor process {pid}. It may have terminated.")
    except Exception as e:
        logging.error(f"Error during system metrics monitoring: {e}")
        

def _wait_for_rdb_load(config):
    """
    Helper function to wait for the RDB load completion log line.
    Returns the load duration and a status flag.
    """
    load_log_file = Path(config.temp_dir) / f"node_log_{config.start_port}_load.log"
    load_duration = None
    
    file_wait_start_time = time.monotonic()
    while not os.path.exists(load_log_file) and (time.monotonic() - file_wait_start_time < 30):
        time.sleep(0.1)

    if not os.path.exists(load_log_file):
        logging.error(f"Log file {load_log_file} did not appear within timeout.")
        return None, False

    logging.info(f"Monitoring log file {load_log_file} for RDB load completion...")
    log_start_time = time.monotonic()
    
    with open(load_log_file, "r", encoding="utf-8") as log_fd:
        log_fd.seek(0, 2)
        while load_duration is None and (time.monotonic() - log_start_time < 600):
            line = log_fd.readline()
            if not line:
                time.sleep(0.1)
                continue
            
            match = re.search(r'DB loaded from disk: ([\d.]+) seconds', line.strip())
            if match:
                load_duration = float(match.group(1))
                logging.info(colorize(f"Detected RDB load completion from log: {load_duration:.3f} seconds.", LOG_COLORS.GREEN))
                break
    
    if load_duration is None:
        logging.error(f"RDB load completion log line not found in {load_log_file} within timeout.")
        return None, False
        
    return load_duration, True

def profile_rdb_load(config, output_dir: Path) -> dict | None:
    logging.info("--- Phase 2: Profiling RDB Load Operation ---")
    process = None
    metrics_list = []
    stop_event = threading.Event()
    monitor_thread = None
    client = None

    try:
        process = start_standalone_valkey_server(config, clear_data_dir=False, log_file_suffix="_load")
        if not process:
            return None

        monitor_thread = threading.Thread(
            target=monitor_system_metrics,
            args=(process.pid, metrics_list, stop_event),
            daemon=True
        )
        monitor_thread.start()

        flamegraph_generated = False
        profiler_output_dir = output_dir / f"flamegraph_threads-{config.rdb_threads}"

        if config.gen_flamegraph:
            logging.info(colorize(f"Starting Flamegraph profiler for {config.rdb_threads} threads...", LOG_COLORS.CYAN))
            with FlamegraphProfiler(pid=process.pid, output_dir=profiler_output_dir) as profiler:
                load_duration, load_ok = _wait_for_rdb_load(config)
        else:
            load_duration, load_ok = _wait_for_rdb_load(config)
        
        if not load_ok:
            return {"status": "error", "error_message": "RDB load log not found"}

        logging.info(colorize(f"Valkey server finished loading RDB in {load_duration:.4f} seconds.", LOG_COLORS.GREEN))

        # NEW: Stop the monitoring thread and collect final metrics while the process is still running.
        stop_event.set()
        if monitor_thread and monitor_thread.is_alive():
            monitor_thread.join(timeout=5)

        # FIX: Collect final metrics BEFORE the process is stopped.
        valkey_proc_psutil = psutil.Process(process.pid)
        cpu_times = valkey_proc_psutil.cpu_times()
        memory_info = valkey_proc_psutil.memory_info()

        # FIX: Manually connect and shut down the server.
        client = wait_for_server_to_start(config)
        num_keys_loaded = get_db_key_count(client)
        logging.info(colorize(f"Num Keys Loaded: {num_keys_loaded}", LOG_COLORS.YELLOW))
        if client:
            logging.info(f"RDB load complete. Shutting down Valkey process {process.pid}...")
            stop_valkey_server(process, client)
        else:
            logging.error("Could not connect to server after RDB load to shut it down.")
            
        # FIX: The profiler.generate() call is now safe since the perf process is stopped by the `with` block
        # and we've waited for everything to finalize.
        if config.gen_flamegraph:
            if 'profiler' in locals() and profiler:
                profiler.generate()
                flamegraph_generated = True

        avg_cpu_percent = statistics.mean([m['cpu_percent'] for m in metrics_list]) if metrics_list else 0
        avg_memory_rss_bytes = statistics.mean([m['memory_rss_bytes'] for m in metrics_list]) if metrics_list else 0
        
        first_io = metrics_list[0] if metrics_list else {'read_bytes': 0, 'write_bytes': 0}
        last_io = metrics_list[-1] if metrics_list else {'read_bytes': 0, 'write_bytes': 0}
        total_read_bytes = last_io['read_bytes'] - first_io['read_bytes']
        total_write_bytes = last_io['write_bytes'] - first_io['write_bytes']
        
        return {
            "load_duration_seconds": load_duration,
            "avg_cpu_percent": avg_cpu_percent,
            "avg_memory_rss_bytes": avg_memory_rss_bytes,
            "total_disk_read_bytes": total_read_bytes,
            "total_disk_write_bytes": total_write_bytes,
            "cpu_user_time_seconds": cpu_times.user,
            "cpu_system_time_seconds": cpu_times.system,
            "memory_rss_bytes_final": memory_info.rss,
            "status": "ok",
            "flamegraph_generated": flamegraph_generated
        }

    except Exception as e:
        logging.critical(f"An error occurred during RDB load profiling. Error: {e}", exc_info=True)
        return {"status": "error", "error_message": str(e)}
    finally:
        stop_event.set()
        if monitor_thread and monitor_thread.is_alive():
            monitor_thread.join(timeout=5)
        if process and process.poll() is None:
            stop_valkey_server(process, client)
            

def load_benchmark(config: BenchmarkConfig, output_dir: Path):
    """
    Main orchestration function for the RDB load benchmark.
    """
    # 1. Create the RDB file to be tested
    config.rdb_threads = 10 # Save data with 10 threads for speed
    if not prepare_rdb_file(config):
        return None
    
    # 3. Aggregate final metrics for reporting
    data_dir = Path(config.temp_dir) / f"node_data_{config.start_port}"
    rdb_file_path = data_dir / "dump.rdb"
    rdb_file_size_bytes = rdb_file_path.stat().st_size if rdb_file_path.exists() else 0
    rdb_threads_to_profile = [1,2,3,4,6, 8, 10, 15]
    
    final_results = []
    for rdb_threads in rdb_threads_to_profile:
        logging.info(colorize(f"--- Starting RDB Load Benchmark with {rdb_threads} threads ---", LOG_COLORS.CYAN))

    # 2. Profile the server loading that RDB file
        config.rdb_threads = rdb_threads
        logging.info(colorize(f"---- Profiling RDB Load with {rdb_threads} rdb_threads", LOG_COLORS.GREEN))
        
        load_results = profile_rdb_load(config, output_dir)
        if not load_results or load_results.get("status") != "ok":
            logging.error("Failed to profile the RDB load.")
            return None

        load_duration = load_results.get("load_duration_seconds", 0)
        actual_throughput = 0
        if load_duration > 0:
            actual_throughput = (rdb_file_size_bytes / load_duration) * (10**-6) # MB/s

        result = {
            "threads": rdb_threads,
            "keys": config.num_keys_millions,
            "value_size": config.value_size_bytes,
            "rdbcompression": config.rdb_compression,
            "rdbchecksum": config.rdb_checksum,
            "actual_throughput_mb_s": actual_throughput,
            "rdb_file_size_bytes": rdb_file_size_bytes,
            **load_results,
        }
        final_results.append(result)
    delete_file(rdb_file_path)
    return final_results # Return as a list for consistency with save_results_to_csv


def main():
    """Main entry point for the RDB load benchmark script."""
    config = parse_benchmark_args()
    setup_logging(config.log_file)
    display_config(config)

    # Create a unique, timestamped directory for this run's output
    try:
        run_id = time.strftime("%Y%m%d_%H%M%S")
        project_root = Path(__file__).resolve().parents[1]
        dir_name = "load_benchmark_with_flamegraphs" if config.gen_flamegraph else "load_benchmark"
        output_dir = project_root / "results" / f"{dir_name}_{run_id}"
        output_dir.mkdir(parents=True, exist_ok=True)
        logging.info(f"All output for this run will be saved in: {output_dir}")
    except Exception as e:
        logging.critical(f"Failed to create output directory. Aborting. Error: {e}", exc_info=True)
        return

    logging.info("--- Starting RDB Load Benchmark ---")
    results = load_benchmark(config, output_dir=output_dir)

    if results:
        logging.info("Benchmark finished. Collected results.")
        csv_file_name = (
            f"load_summary_{config.num_keys_millions}keys_{config.value_size_bytes}B"
            f"_comp-{config.rdb_compression}_csum-{config.rdb_checksum}.csv"
        )
        save_results_to_csv(
            results=results,
            output_dir=str(output_dir),
            file_name=csv_file_name,
        )
    else:
        logging.error("Benchmark failed to produce any results.")


if __name__ == "__main__":
    main()