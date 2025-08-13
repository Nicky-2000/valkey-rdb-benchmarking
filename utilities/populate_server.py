import valkey
from valkey.commands.json.path import Path
from utilities.key_value_generation_utilities import make_random_key, make_deterministic_val, \
    generate_user_data, generate_session_data, generate_product_data, generate_analytics_data, generate_heavy_product_data
from utilities.parse_args import BenchmarkConfig, WorkloadType

from concurrent.futures import ProcessPoolExecutor, as_completed
import logging
import subprocess
import time
import os
import sys
from enum import Enum

# --- Constants ---
KEY_SIZE_BYTES = 16
NUM_PROCESSES_FOR_DATA_POPULATION = 20

# --- Data Population with valkey-benchmark (Existing Function) ---
def populate_data_with_benchmark(config: BenchmarkConfig) -> bool:
    """
    Populates a standalone Valkey instance using the valkey-benchmark command,
    printing every 100th line of output to the console.

    Args:
        config: The BenchmarkConfig object containing run parameters.

    Returns:
        True if data population was successful, False otherwise.
    """
    valkey_benchmark_path = os.environ.get("VALKEY_BENCHMARK_PATH")
    if not valkey_benchmark_path:
        logging.error("VALKEY_BENCHMARK_PATH environment variable is not set.")
        return False
    if not os.path.exists(valkey_benchmark_path):
        logging.error(f"valkey-benchmark executable not found at: {valkey_benchmark_path}")
        return False

    num_keys = int(config.num_keys_millions * 1e6)
    
    # Construct the valkey-benchmark command
    command = [
        valkey_benchmark_path,
        "-h", "127.0.0.1",
        "-p", str(config.start_port),
        "-t", "SET",
        "-n", str(num_keys),
        "-d", str(config.value_size_bytes),
        "-c", str(config.clients if hasattr(config, 'clients') else 200),
        "-P", str(config.pipeline_size if hasattr(config, 'pipeline_size') else 64),
        "-r", str(num_keys),
        "--sequential",
    ]

    logging.info(f"Starting data population with valkey-benchmark: {' '.join(command)}")
    start_time = time.monotonic()

    try:
        # Use subprocess.Popen to stream output
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,  # Redirect stderr to stdout to consolidate output
            text=True,
            universal_newlines=True
        )

        # Print only every 100th line of output
        line_count = 0
        for line in process.stdout:
            line_count += 1
            if line_count % 100 == 0:
                sys.stdout.write(line)
                sys.stdout.flush()
            
        # Ensure the last line is printed if the total isn't a multiple of 100
        if line_count % 100 != 0:
            sys.stdout.write(line)
            sys.stdout.flush()

        return_code = process.wait()

        end_time = time.monotonic()
        load_time = end_time - start_time

        logging.info("--- Data Population Summary (valkey-benchmark) ---")
        logging.info(f"Target Keys: {num_keys:,}")
        logging.info(f"Data Population Total Time: {load_time:.2f} seconds")
        logging.info(f"Data population with valkey-benchmark finished with return code: {return_code}")

        return return_code == 0

    except FileNotFoundError:
        logging.error(f"valkey-benchmark executable not found. Ensure VALKEY_BENCHMARK_PATH is correct: {valkey_benchmark_path}")
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred during valkey-benchmark execution: {e}", exc_info=True)
        return False

# --- Worker for String Data (Corrected to return kv tuples) ---
def _populate_string_worker(connection_info: tuple[str, int], value_size: int, num_keys_for_worker: int, return_keys: bool):
    """
    A worker process that connects to Valkey, generates its own keys and values, and populates them.
    Returns the count of keys and optionally a list of (key, value) tuples.
    """
    host, port = connection_info
    batch_size = 1 if value_size > 10_000 else 50_000
    kv_list = [] if return_keys else None
    
    try:
        client = valkey.Valkey(host=host, port=port, decode_responses=True)
        client.ping()

        pipe = client.pipeline(transaction=False)
        for i in range(num_keys_for_worker):
            key = make_random_key(key_length=KEY_SIZE_BYTES)
            value = make_deterministic_val(key, value_size)
            pipe.set(key, value)
            
            if return_keys:
                kv_list.append((key, value))
                
            if (i + 1) % batch_size == 0:
                pipe.execute()
        pipe.execute()
        
        # Return the count and the list of (key, value) tuples
        return num_keys_for_worker, kv_list
    except Exception as e:
        logging.error(f"A string worker process failed: {e}", exc_info=True)
        return 0, None
    finally:
        if 'client' in locals() and client:
            client.connection_pool.disconnect()
 

# --- Worker for JSON Data (Corrected) ---
def _populate_json_worker(connection_info: tuple[str, int], workload_type: WorkloadType, num_keys_for_worker: int, start_index: int, return_keys: bool):
    """
    A worker process that generates its own JSON keys and populates them.
    Returns the count of keys and optionally the list of keys.
    """
    host, port = connection_info
    batch_size = 500
    kv_list = [] if return_keys else None
    
    json_generators = {
        WorkloadType.USER_DATA.value: generate_user_data,
        WorkloadType.SESSION_DATA.value: generate_session_data,
        WorkloadType.PRODUCT_DATA.value: generate_product_data,
        WorkloadType.PRODUCT_DATA_HEAVY.value: generate_heavy_product_data,
        WorkloadType.ANALYTICS_DATA.value: generate_analytics_data
    }

    try:
        client = valkey.Valkey(host=host, port=port, decode_responses=True)
        json_client = client.json()
        client.ping()

        pipe = client.pipeline(transaction=False)
        keys_inserted_count = 0
        generate_func = json_generators.get(workload_type.value)
        
        if not generate_func:
            logging.error(f"Invalid JSON workload type received by worker: {workload_type.value}")
            return 0, None

        for i in range(num_keys_for_worker):
            key_index = start_index + i
            key = f"{workload_type.value}:{key_index}"  
            data = generate_func()
            pipe.json().set(key, Path.root_path(), data)
            
            if return_keys:
                # Append the tuple of key and the generated JSON data
                kv_list.append((key, data))

            keys_inserted_count += 1
            if (i + 1) % batch_size == 0:
                pipe.execute()

        pipe.execute()
        # Return the count and the list of (key, value) tuples
        return keys_inserted_count, kv_list
    except Exception as e:
        logging.error(f"A JSON worker process failed: {e}", exc_info=True)
        return 0, None
    finally:
        if 'client' in locals() and client:
            client.connection_pool.disconnect()      
def populate_data_standalone(config: BenchmarkConfig, return_keys: bool = False):
    """
    Populates a Valkey instance using multiple processes, with each worker
    generating its own keys. Conditionally returns a list of all populated (key, value) tuples.

    Args:
        config: The BenchmarkConfig object.
        return_keys: A boolean flag to determine if the function should return the list of keys and values.
                     Defaults to False.
    """    
    num_keys = int(config.num_keys_millions * 1e6)
    logging.info(f"Starting data population for {num_keys:,} {config.workload_type.value} keys...")
    start_time = time.monotonic()
    
    keys_per_worker = num_keys // NUM_PROCESSES_FOR_DATA_POPULATION
    
    total_keys_inserted = 0
    total_kv_list = [] if return_keys else None
    
    with ProcessPoolExecutor(max_workers=NUM_PROCESSES_FOR_DATA_POPULATION) as executor:
        futures = []
        
        # Determine the correct worker and arguments
        if config.workload_type.value  == WorkloadType.STRING.value:
            futures = [
                executor.submit(_populate_string_worker, ("127.0.0.1", config.start_port), config.value_size_bytes, keys_per_worker, return_keys)
                for _ in range(NUM_PROCESSES_FOR_DATA_POPULATION)
            ]
        else: # For all JSON workload types
            futures = [
                executor.submit(_populate_json_worker, ("127.0.0.1", config.start_port), config.workload_type, keys_per_worker, i * keys_per_worker, return_keys)
                for i in range(NUM_PROCESSES_FOR_DATA_POPULATION)
            ]

        # Handle any remaining keys if num_keys isn't perfectly divisible
        remaining_keys = num_keys % NUM_PROCESSES_FOR_DATA_POPULATION
        if remaining_keys > 0:
            if config.workload_type == WorkloadType.STRING:
                futures.append(executor.submit(_populate_string_worker, ("127.0.0.1", config.start_port), config.value_size_bytes, remaining_keys, return_keys))
            else:
                futures.append(executor.submit(_populate_json_worker, ("127.0.0.1", config.start_port), config.workload_type, remaining_keys, NUM_PROCESSES_FOR_DATA_POPULATION * keys_per_worker, return_keys))

        for future in as_completed(futures):
            try:
                keys_inserted, kv_from_worker = future.result()
                if keys_inserted > 0:
                    total_keys_inserted += keys_inserted
                    if return_keys and kv_from_worker:
                        total_kv_list.extend(kv_from_worker)
                    logging.info(f"A worker finished, {total_keys_inserted:,} / {num_keys:,} keys inserted so far.")
            except Exception as e:
                logging.error(f"A task generated an exception: {e}", exc_info=True)

    load_time = time.monotonic() - start_time
    keys_per_second = total_keys_inserted / load_time if load_time > 0 else 0

    logging.info("--- Data Population Summary ---")
    logging.info(f"Workload Type:              {config.workload_type.value}")
    logging.info(f"Target Keys:                {num_keys:,}")
    logging.info(f"Successfully Set:           {total_keys_inserted:,}")
    logging.info(f"Data Population Total Time: {load_time:.2f} seconds")
    logging.info(f"Rate:                       {keys_per_second:,.2f} keys/sec")

    if total_keys_inserted != num_keys:
        logging.warning("Data population may be incomplete. Check logs for errors.")
        
    return total_kv_list