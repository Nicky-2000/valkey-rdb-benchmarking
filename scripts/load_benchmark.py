import logging
import time
import psutil
from pathlib import Path

# --- Local Utility Imports ---
from utilities.parse_args import (
    LOG_COLORS,
    colorize,
    parse_benchmark_args,
    display_config,
    setup_logging,
    BenchmarkConfig,
)
from utilities.file_system_utilities import save_results_to_csv, setup_directory_for_run
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


def profile_rdb_load(config: BenchmarkConfig, output_dir: Path) -> dict | None:
    """
    Profiles the RDB load time of a new Valkey server instance.
    Optionally generates a flame graph.
    """
    logging.info("--- Phase 2: Profiling RDB Load Operation ---")
    process = None
    try:
        # We need to capture the process as soon as it starts to profile accurately
        # Therefore, we can't use wait_for_server_to_start directly here.
        
        # Start the server but don't clear the data dir this time
        process = start_standalone_valkey_server(config, clear_data_dir=False, log_file_suffix="_load")
        if not process:
            return None
        
        valkey_proc_psutil = psutil.Process(process.pid)
        
        # Start profiling and timing simultaneously
        load_start_time = time.monotonic()
        # Conditionally run the profiler
        if config.gen_flamegraph:
            with FlamegraphProfiler(pid=process.pid, output_dir=output_dir) as profiler:
                # Wait for server to finish loading RDB (i.e., become ready)
                wait_for_server_to_start(config, timeout_seconds=600)
            
            flamegraph_file_name = "flamegraph_load.svg"
            profiler.generate(flamegraph_file_name)
        else:
            # If not generating a flamegraph, just wait for it to be ready
            wait_for_server_to_start(config, timeout_seconds=600)

        load_duration = time.monotonic() - load_start_time
        logging.info(colorize(f"Valkey server finished loading RDB in {load_duration:.4f} seconds.", LOG_COLORS.GREEN))

        # Manually collect psutil info after load is complete
        cpu_times = valkey_proc_psutil.cpu_times()
        memory_info = valkey_proc_psutil.memory_info()
        
        return {
            "load_duration_seconds": load_duration,
            "cpu_user_time_seconds": cpu_times.user,
            "cpu_system_time_seconds": cpu_times.system,
            "memory_rss_bytes": memory_info.rss,
            "status": "ok"
        }

    except Exception as e:
        logging.critical(f"An error occurred during RDB load profiling. Error: {e}", exc_info=True)
        return {"status": "error"}
    finally:
        if process:
            # The server from this phase is our final state, so we stop it here.
            stop_valkey_server(process, None) 


def load_benchmark(config: BenchmarkConfig, output_dir: Path):
    """
    Main orchestration function for the RDB load benchmark.
    """
    # 1. Create the RDB file to be tested
    config.rdb_threads = 10 # Save data with 10 threads for speeddd
    if not prepare_rdb_file(config):
        return None
    
    rdb_threads_to_profile = [1,2,3,4]
    final_results = []
    for rdb_threads in rdb_threads_to_profile:
        logging.info(colorize(f"--- Starting RDB Load Benchmark with {rdb_threads} threads ---", LOG_COLORS.CYAN))

    # 2. Profile the server loading that RDB file
        config.rdb_threads = rdb_threads
        load_results = profile_rdb_load(config, output_dir)
        if not load_results or load_results.get("status") != "ok":
            logging.error("Failed to profile the RDB load.")
            return None

        # 3. Aggregate final metrics for reporting
        data_dir = Path(config.temp_dir) / f"node_data_{config.start_port}"
        rdb_file_path = data_dir / "dump.rdb"
        rdb_file_size_bytes = rdb_file_path.stat().st_size if rdb_file_path.exists() else 0
        load_duration = load_results.get("load_duration_seconds", 0)

        actual_throughput = 0
        if load_duration > 0:
            actual_throughput = (rdb_file_size_bytes / load_duration) * (10**-6)  # MB/s

        result = {
            "keys": config.num_keys_millions,
            "value_size": config.value_size_bytes,
            "rdbcompression": config.rdb_compression,
            "rdbchecksum": config.rdb_checksum,
            "actual_throughput_mb_s": actual_throughput,
            "rdb_file_size_bytes": rdb_file_size_bytes,
            **load_results,
        }
        final_results.append(result)
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