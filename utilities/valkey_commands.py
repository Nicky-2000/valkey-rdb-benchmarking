import logging
import subprocess
import time
import psutil
import valkey
from utilities.parse_args import BenchmarkConfig
from utilities.key_value_generation_utilities import make_deterministic_val 

def get_db_key_count(config: BenchmarkConfig) -> int:
    """
    Connects to a Valkey instance and returns the number of keys in the database.

    Args:
        config: The BenchmarkConfig object with connection details.

    Returns:
        The number of keys as an integer, or 0 if an error occurs.
    """
    client = None
    try:
        # Establish a connection to the Valkey server
        client = valkey.Valkey(host="127.0.0.1", port=config.start_port, decode_responses=True)
        
        #  Get the number of keys
        db_size = client.dbsize()
        logging.info(f"Successfully retrieved DBSIZE from port {config.start_port}. Key count: {db_size}")
        return db_size
        
    except valkey.exceptions.ConnectionError as e:
        # Log a specific error if the connection fails
        logging.error(f"Could not connect to Valkey on port {config.start_port} to get DB size.", exc_info=True)
        
    except Exception as e:
        # Log any other unexpected errors during the process
        logging.error(f"An unexpected error occurred while getting DB size on port {config.start_port}.", exc_info=True)
        
    finally:
        # Ensure the connection is always closed to prevent resource leaks
        if client:
            client.connection_pool.disconnect()
            logging.debug("Valkey client connection closed.")
            
    return 0

def trigger_blocking_save(client: valkey.Valkey) -> dict:
    """
    Triggers a blocking SAVE on a Valkey client and measures its duration.

    This is a lightweight function that does not perform detailed system profiling.

    Args:
        client: An active valkey.Valkey client instance.

    Returns:
        A dictionary with the save duration and status.
    """
    node_port = client.connection_pool.connection_kwargs.get("port", "N/A")
    logging.info(f"Starting blocking SAVE command on port {node_port}...")

    try:
        start_time = time.monotonic()
        client.save()  # This call blocks until the save is complete
        duration = time.monotonic() - start_time

        logging.info(f"Blocking SAVE on port {node_port} completed in {duration:.4f} seconds.")

        # Optionally, verify with the server's INFO command
        info = client.info("persistence")
        last_save_status = info.get("rdb_last_save_status", "unknown")
        
        if last_save_status == "ok":
            logging.info(f"Server on port {node_port} confirmed successful save.")
        else:
            logging.warning(f"Server on port {node_port} reported last save status as '{last_save_status}'.")

        return {
            "port": node_port,
            "save_duration_seconds": duration,
            "status": last_save_status,
        }

    except valkey.exceptions.ValkeyError as e:
        logging.error(f"A Valkey error occurred during SAVE on port {node_port}.", exc_info=True)
        return {
            "port": node_port,
            "save_duration_seconds": -1,
            "status": "error",
            "error_message": str(e),
        }
    except Exception as e:
        logging.error(f"An unexpected error occurred during SAVE on port {node_port}.", exc_info=True)
        return {
            "port": node_port,
            "save_duration_seconds": -1,
            "status": "error",
            "error_message": str(e),
        }

def profile_blocking_save(client: valkey.Valkey, server_process: subprocess.Popen) -> dict:
    """
    Triggers a SAVE and profiles system metrics (CPU, I/O, memory) during the operation.

    Args:
        client: An active valkey.Valkey client.
        server_process: The subprocess.Popen object for the Valkey server.

    Returns:
        A dictionary containing detailed performance metrics.
    """
    node_port = client.connection_pool.connection_kwargs.get("port", "N/A")
    logging.info(f"Starting profiled SAVE on port {node_port} (PID: {server_process.pid})...")

    try:
        valkey_proc = psutil.Process(server_process.pid)
        
        # --- Capture metrics before SAVE ---
        cpu_before = valkey_proc.cpu_times()
        io_before = valkey_proc.io_counters()
        mem_before = valkey_proc.memory_info()
        ctx_before = valkey_proc.num_ctx_switches()

        # --- Execute the blocking SAVE command ---
        save_start_time = time.monotonic()
        client.save()
        save_duration = time.monotonic() - save_start_time

        # --- Capture metrics after SAVE ---
        cpu_after = valkey_proc.cpu_times()
        io_after = valkey_proc.io_counters()
        mem_after = valkey_proc.memory_info()
        ctx_after = valkey_proc.num_ctx_switches()

        logging.info(f"Profiled SAVE on port {node_port} finished in {save_duration:.4f} seconds.")

        # --- Calculate and return the results ---
        cpu_total_time = (cpu_after.user - cpu_before.user) + (cpu_after.system - cpu_before.system)
        cpu_utilization = (cpu_total_time / save_duration) * 100 if save_duration > 0 else 0

        return {
            "port": node_port,
            "save_duration_seconds": save_duration,
            "cpu_utilization_percent": cpu_utilization,
            "cpu_total_time_seconds": cpu_total_time,
            "io_read_bytes": io_after.read_bytes - io_before.read_bytes,
            "io_write_bytes": io_after.write_bytes - io_before.write_bytes,
            "memory_rss_bytes": mem_after.rss,
            "context_switches_voluntary": ctx_after.voluntary - ctx_before.voluntary,
            "context_switches_involuntary": ctx_after.involuntary - ctx_before.involuntary,
        }
    except psutil.NoSuchProcess:
        logging.error(f"Process with PID {server_process.pid} not found for profiling.", exc_info=True)
        return {"port": node_port, "status": "error", "error_message": "Process not found"}
    except Exception as e:
        logging.error(f"An unexpected error occurred during profiled SAVE on port {node_port}.", exc_info=True)
        return {"port": node_port, "status": "error", "error_message": str(e)}

def verify_data(
    client: valkey.Valkey, keys_to_verify: list[str], value_size: int
) -> bool:
    """
    Verifies the existence and correctness of keys in a Valkey instance.

    This function fetches keys in batches using MGET, compares them to their
    expected deterministic values, and logs the progress and final result.

    Args:
        client: An active valkey.Valkey client instance.
        keys_to_verify: A list of keys that should exist in the database.
        value_size: The expected size of the value for each key.

    Returns:
        True if all keys are verified successfully, False otherwise.
    """
    logging.info("--- Starting Data Verification ---")
    if not keys_to_verify:
        logging.warning("Verification list is empty. Nothing to do.")
        return True

    total_keys = len(keys_to_verify)
    errors_found = 0
    batch_size = 5000  # Number of keys to fetch with each MGET command
    
    logging.info(f"Preparing to verify {total_keys:,} keys with a batch size of {batch_size:,}.")
    
    start_time = time.monotonic()

    for i in range(0, total_keys, batch_size):
        key_batch = keys_to_verify[i : i + batch_size]
        
        try:
            # Use mget for efficient batch retrieval
            actual_values = client.mget(key_batch)

            # Compare each key's actual value with its expected value
            for j, key in enumerate(key_batch):
                expected_value = make_deterministic_val(key, value_size)
                
                # The client decodes responses, so we compare strings directly
                if actual_values[j] != expected_value:
                    logging.warning(f"Key MISMATCH: '{key}'. Expected '{expected_value}', got '{actual_values[j]}'.")
                    errors_found += 1
        
        except valkey.exceptions.ValkeyError as e:
            logging.error(f"A Valkey error occurred during MGET for a batch starting with key '{key_batch[0]}'.", exc_info=True)
            errors_found += len(key_batch) # Assume all keys in the failed batch are errors
        
        except Exception as e:
            logging.error("An unexpected error occurred during data verification.", exc_info=True)
            errors_found += len(key_batch)
            # Stop verification on unexpected errors
            break 

    # --- Final Summary ---
    elapsed_time = time.monotonic() - start_time
    logging.info(f"Verification finished in {elapsed_time:.2f} seconds.")
    
    if errors_found == 0:
        logging.info(f"✅ Success! All {total_keys:,} keys were verified correctly.")
        return True
    else:
        logging.error(f"❌ Verification Failed. Found {errors_found:,} errors out of {total_keys:,} keys.")
        return False
    
def save_results_to_csv(results: list[dict], output_dir: str, file_name: str):
    """
    Saves a list of dictionary results to a CSV file.

    Args:
        results: A list of dictionaries, where each dict is a row.
        output_dir: The directory to save the CSV file.
        file_name: The name of the file to save (e.g., "results.csv").
    """
    if not results:
        logging.warning("No results to save to CSV.")
        return

    try:
        output_path = os.path.join(output_dir, file_name)
        os.makedirs(output_dir, exist_ok=True)
        
        df = pd.DataFrame(results)
        df.to_csv(output_path, index=False)
        
        logging.info(f"Benchmark results successfully saved to: {output_path}")

    except Exception as e:
        logging.error(f"Failed to save results to CSV at {output_path}.", exc_info=True)
