import logging
from pathlib import Path
import time

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
from utilities.populate_server import (
    populate_data_standalone,
    KEY_SIZE_BYTES,
    populate_data_with_benchmark,
)
from utilities.valkey_commands import get_db_key_count, profile_blocking_save
from utilities.flamegraph_profiler import FlamegraphProfiler


def save_benchmark(config: BenchmarkConfig, output_dir: Path):
    """
    Runs a benchmark to generate flame graphs for the SAVE command
    across a variety of rdb-threads settings.
    """
    process = None
    client = None

    try:
        # --- 1. Initial Setup and Server Start ---
        setup_directory_for_run(config.temp_dir)
        compression_options = []
        if config.rdb_compression == "both":
            compression_options = ["yes", "no"]
        else:
            compression_options.append(config.rdb_compression)

        config.rdb_compression = "yes"  # Set this so we can start the server

        process = start_standalone_valkey_server(config)
        if not process:
            return None

        client = wait_for_server_to_start(config)
        if not client:
            return None

        # --- 2. Populate Data ---
        # populate_data_with_benchmark(config)
        populate_data_standalone(config, return_keys=False)

        num_keys_expected = config.num_keys_millions * 1e6
        # keys_to_test = populate_data_standalone(config)
        initial_key_count = get_db_key_count(client)

        if initial_key_count != num_keys_expected:
            logging.error(
                f"Key population mismatch: Expected {num_keys_expected:,} keys but DB has {initial_key_count:,}."
            )
            return None

        # --- 3. Iterate, Profile, and Collect Results ---
        all_results = []
        thread_counts_to_test = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 20, 25, 30]

        for comp_option in compression_options:
            logging.info(
                colorize(
                    f"--- Running test with rdbcompression = {comp_option} ---",
                    LOG_COLORS.CYAN,
                )
            )
            # Enables us to only have to load keys one time to test save with both compression on and off
            config.rdb_compression = comp_option  # For data saving
            client.config_set("rdbcompression", comp_option)

            for num_threads in thread_counts_to_test:
                logging.info(
                    colorize(
                        f"--- Running test with rdb-threads = {num_threads} ---",
                        LOG_COLORS.CYAN,
                    )
                )
                client.config_set("rdb-threads", num_threads)

                try:
                    # Use the FlamegraphProfiler context manager
                    # Conditionally run the profiler if the flag is set
                    if config.gen_flamegraph:
                        with FlamegraphProfiler(
                            pid=process.pid, output_dir=output_dir
                        ) as profiler:
                            save_result = profile_blocking_save(client, process)

                        flamegraph_file_name = f"flamegraph_{num_threads}threads.svg"
                        profiler.generate(flamegraph_file_name)
                    else:
                        # Otherwise, just run the save command without the profiler
                        save_result = profile_blocking_save(client, process)

                except FileNotFoundError as e:
                    logging.critical(
                        f"Profiling failed: A required tool was not found. Check PERF_PATH and FLAME_GRAPH_REPO_PATH in your .env file. Error: {e}"
                    )
                    continue

                # --- 4. Process and Aggregate Results ---
                data_dir = Path(config.temp_dir) / f"node_data_{config.start_port}"
                rdb_file_path = data_dir / "dump.rdb"

                rdb_file_size_bytes = (
                    rdb_file_path.stat().st_size if rdb_file_path.exists() else 0
                )

                save_duration = save_result.get("save_duration_seconds", 0)
                actual_throughput = 0
                valkey_data_throughput = 0
                num_keys = int(config.num_keys_millions * 1e6)
                if save_result.get("status") == "ok" and save_duration > 0:
                    actual_throughput = (rdb_file_size_bytes / save_duration) * (10**-6)
                    # valkey_data_throughput = (
                    #     (num_keys * (config.value_size_bytes + KEY_SIZE_BYTES))
                    #     / save_duration
                    # ) * (10**-6)

                # Combine all results into a single dictionary
                final_result = {
                    "keys": num_keys,
                    "value_size": config.value_size_bytes,
                    "num_threads": num_threads,
                    "rdbcompression": config.rdb_compression,
                    "rdbchecksum": config.rdb_checksum,
                    "valkey_data_throughput_mb_s": valkey_data_throughput,
                    "actual_throughput_mb_s": actual_throughput,
                    "rdb_file_size_bytes": rdb_file_size_bytes,
                    **save_result,  # Unpack the detailed profiling results
                }
                all_results.append(final_result)

                logging.info(
                    f"Finished test for {num_threads} threads. Deleting RDB file for next run."
                )
                delete_file(str(rdb_file_path))

        logging.info("--- All benchmark iterations completed ---")
        return all_results

    except Exception as e:
        logging.critical(
            "An unhandled exception occurred during the benchmark.", exc_info=True
        )
        return None
    finally:
        if process:
            logging.info("--- Final cleanup: Stopping Valkey server. ---")
            stop_valkey_server(process, client)


def main():
    """Main entry point for the SAVE benchmark script."""
    config = parse_benchmark_args()
    setup_logging(config.log_file)
    display_config(config)

    # --- Create a unique, timestamped directory for this run's output ---
    try:
        run_id = time.strftime("%Y%m%d_%H%M%S")
        project_root = Path(__file__).resolve().parents[1]
        # Make directory name more descriptive
        dir_name = (
            "save_benchmark_with_flamegraphs"
            if config.gen_flamegraph
            else "save_benchmark"
        )
        output_dir = project_root / "results" / f"{dir_name}_{run_id}"
        output_dir.mkdir(parents=True, exist_ok=True)
        logging.info(f"All output for this run will be saved in: {output_dir}")
    except Exception as e:
        logging.critical(
            f"Failed to create output directory. Aborting. Error: {e}", exc_info=True
        )
        return

    logging.info("--- Starting RDB SAVE Benchmark ---")
    results = save_benchmark(config, output_dir=output_dir)

    if results:
        logging.info(f"Benchmark finished. Collected {len(results)} results.")
        tempfs = "yes" if "dev/shm/" in config.temp_dir else "no"
        csv_file_name = (
            f"save_summary_workload_{config.workload_type.value}_{config.num_keys_millions}keys_{config.value_size_bytes}B_tempfs_{tempfs}"
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
