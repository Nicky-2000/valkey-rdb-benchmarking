import logging
import subprocess
import time
import psutil
import valkey
from utilities.parse_args import LOG_COLORS, BenchmarkConfig, colorize
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

        logging.info(colorize(f"Profiled SAVE on port {node_port} finished in {save_duration:.4f} seconds.", LOG_COLORS.GREEN))

        # --- Calculate and return the results ---
        cpu_total_time = (cpu_after.user - cpu_before.user) + (cpu_after.system - cpu_before.system)
        cpu_utilization = (cpu_total_time / save_duration) * 100 if save_duration > 0 else 0

        return {
            "status": "ok",
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


from typing import List, Tuple, Any
from valkey.commands.json.path import Path

def verify_data(
    client: valkey.Valkey, kv_to_verify: List[Tuple[str, Any]]
) -> bool:
    """
    Verifies the existence and correctness of keys in a Valkey instance.

    This function fetches keys in batches using MGET or JSON.MGET, compares them to
    their expected values, and logs the progress and final result.

    Args:
        client: An active valkey.Valkey client instance.
        kv_to_verify: A list of (key, expected_value) tuples to verify. The expected_value
                      can be a string for a string workload or a dictionary for a JSON workload.

    Returns:
        True if all key-value pairs are verified successfully, False otherwise.
    """
    logging.info("--- Starting Data Verification ---")
    if not kv_to_verify:
        logging.warning("Verification list is empty. Nothing to do.")
        return True

    total_keys = len(kv_to_verify)
    errors_found = 0
    batch_size = 5000  # Number of key-value pairs to fetch in each batch
    
    logging.info(f"Preparing to verify {total_keys:,} key-value pairs with a batch size of {batch_size:,}.")
    
    start_time = time.monotonic()

    # Determine if we need to use MGET or JSON.MGET based on the data type
    is_json_workload = isinstance(kv_to_verify[0][1], (dict, list))
    keys_to_verify = [kv[0] for kv in kv_to_verify]
    
    for i in range(0, total_keys, batch_size):
        key_batch = keys_to_verify[i : i + batch_size]
        
        try:
            if is_json_workload:
                # Use JSON.MGET for JSON data
                actual_values = client.json().mget(key_batch, Path.root_path())
            else:
                # Use MGET for string data
                actual_values = client.mget(key_batch)

            # Compare each key's actual value with its expected value
            for j, key in enumerate(key_batch):
                expected_value = kv_to_verify[i + j][1]
                actual_value = actual_values[j]
                
                # For JSON data, the return type is a list of dictionaries, so we compare the first element
                # if actual_value is None, it means the key was not found.
                if is_json_workload and actual_value and isinstance(actual_value, list):
                    actual_value = actual_value[0]

                if actual_value != expected_value:
                    logging.warning(f"Key MISMATCH: '{key}'. Expected '{expected_value}', got '{actual_value}'.")
                    errors_found += 1
        
        except valkey.exceptions.ValkeyError as e:
            logging.error(f"A Valkey error occurred during batch retrieval for a batch starting with key '{key_batch[0]}'.", exc_info=True)
            errors_found += len(key_batch) # Assume all keys in the failed batch are errors
        
        except Exception as e:
            logging.error("An unexpected error occurred during data verification.", exc_info=True)
            errors_found += len(key_batch)
            break  # Stop verification on unexpected errors

    # --- Final Summary ---
    elapsed_time = time.monotonic() - start_time
    logging.info(f"Verification finished in {elapsed_time:.2f} seconds.")
    
    if errors_found == 0:
        logging.info(f"✅ Success! All {total_keys:,} key-value pairs were verified correctly.")
        return True
    else:
        logging.error(f"❌ Verification Failed. Found {errors_found:,} errors out of {total_keys:,} keys.")
        return False