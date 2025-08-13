import logging
from utilities.parse_args import parse_benchmark_args, BenchmarkConfig, display_config
from utilities.file_system_utilities import setup_directory_for_run
from utilities.valkey_server_utilities import (
    start_standalone_valkey_server,
    stop_valkey_server,
    wait_for_server_to_start,
)
from utilities.populate_server import WorkloadType, populate_data_standalone, KEY_SIZE_BYTES
from utilities.valkey_commands import (
    get_db_key_count,
    trigger_blocking_save,
    verify_data,
)


def rdb_save_validity_test(config: BenchmarkConfig) -> bool:
    """
    Runs a full test cycle: start, populate, save, stop, restart, and verify.

    Args:
        config: The benchmark configuration object.

    Returns:
        True if the entire test cycle passes, False otherwise.
    """
    process = None
    client = None
    try:
        # --- 1. Initial Setup and Server Start ---
        setup_directory_for_run(config.temp_dir)
        process = start_standalone_valkey_server(config)
        if not process:
            logging.critical("Failed to start Valkey server initially. Aborting test.")
            return False

        client = wait_for_server_to_start(config)
        if not client:
            logging.critical("Valkey server did not become reachable. Aborting test.")
            return False

        # --- 2. Populate Data and Verify Initial State ---
        keys_to_test = populate_data_standalone(config, return_keys=True)
        
        initial_key_count = get_db_key_count(config)

        if initial_key_count != len(keys_to_test):
            logging.error(
                f"Mismatch: Expected {len(keys_to_test):,} keys but DB has {initial_key_count:,}."
            )
            return False

        # --- 3. Trigger SAVE and Stop Server ---
        trigger_blocking_save(client)
        logging.info("Save completed. Stopping server...")

        stop_valkey_server(process, client)
        process, client = None, None  # Reset for restart

        # --- 4. Restart Server and Verify Reload ---
        logging.info("Restarting server to test RDB reload...")
        process = start_standalone_valkey_server(
            config, clear_data_dir=False, log_file_suffix="_restarted"
        )
        if not process:
            logging.critical("Failed to restart Valkey server. Aborting test.")
            return False

        client = wait_for_server_to_start(config, timeout_seconds=480)
        if not client:
            logging.critical("Restarted Valkey server did not become reachable.")
            return False

        reloaded_key_count = get_db_key_count(config)
        logging.info(f"Server reloaded {reloaded_key_count:,} keys from RDB file.")

        if initial_key_count != reloaded_key_count:
            logging.error(
                f"Key count mismatch after reload! Before: {initial_key_count:,}, After: {reloaded_key_count:,}"
            )
            return False

        # --- 5. Final Data Verification ---
        if not verify_data(client, keys_to_test):
            logging.error("Data verification failed after RDB reload.")
            return False

        logging.info("RDB reload and data verification successful.")
        return True

    except Exception as e:
        logging.critical(
            "An unhandled exception occurred during the test.", exc_info=True
        )
        return False

    finally:
        # --- Cleanup: Ensure server is always stopped ---
        if process:
            logging.info("Ensuring Valkey server is stopped in cleanup.")
            stop_valkey_server(process, client)


def main():
    """Main entry point for the RDB save validity test script."""
    # The `parse_benchmark_args` function should now handle setting up logging
    config = parse_benchmark_args()
    display_config(config)

    logging.info("--- Starting RDB Save Validity Test for Standalone Valkey ---")

    success = rdb_save_validity_test(config)

    if success:
        logging.info("✅✅✅ TEST PASSED ✅✅✅")
    else:
        logging.error("❌❌❌ TEST FAILED ❌❌❌")


if __name__ == "__main__":
    main()
