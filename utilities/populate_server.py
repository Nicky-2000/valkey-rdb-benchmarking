from concurrent.futures import ProcessPoolExecutor, as_completed
import logging
import time
import os
import sys
import valkey
from utilities.key_value_generation_utilities import make_random_key, make_deterministic_val
from utilities.parse_args import BenchmarkConfig


NUM_PROCESSES_FOR_DATA_POPULATION = 20
EXPECTED_KEY_VALUES = []

# --- Data Population (Multiprocessed) ---
def _populate_worker(
    keys: list[str],
    connection_info: tuple[str, int],  # Single (host, port) tuple for standalone
    value_size: int,
):
    """
    A worker process that connects to Valkey and populates a given chunk of keys.

    Args:
        keys: A list of keys for this worker to insert.
        connection_info: A (host, port) tuple for the Valkey server.
        value_size: The size of the value to generate for each key.

    Returns:
        The number of keys successfully inserted by this worker.
    """
    host, port = connection_info
    batch_size = 1000 if value_size > 10_000 else 50_000
    try:
        client = valkey.Valkey(host=host, port=port, decode_responses=True)
        client.ping()  # Verify connection

        pipe = client.pipeline(transaction=False)
        num_processed = 0
        for i, key in enumerate(keys):
            value = make_deterministic_val(key, value_size)
            pipe.set(key, value)
            num_processed += 1

            # Execute pipeline batch every 'batch_size' keys
            if (i + 1) % batch_size == 0:
                pipe.execute()

        pipe.execute()  # Execute any remaining commands after the loop (important for last batch)

        return len(keys)  # Indicate success
    except Exception as e:
        logging.error(f"A worker process failed: {e}", exc_info=True)
        return 0 # Return 0 on failure
    finally:
        if 'client' in locals() and client:
            client.connection_pool.disconnect()

def populate_data_standalone(config: BenchmarkConfig):
    """
    Generates keys and populates a standalone Valkey instance using multiple processes.

    Args:
        config: The BenchmarkConfig object containing run parameters.
    """
    num_keys = int(config.num_keys_millions * 1e6)
    logging.info(f"Generating {num_keys:,} keys in memory...")
    start_time = time.monotonic()
    
    # 1. Generate all keys in the main process first.
    #    This ensures the list is identical for every benchmark run.
    keys_to_load = [make_random_key(key_length=16) for _ in range(num_keys)]
    
    generation_time = time.monotonic() - start_time
    logging.info(f"Key generation finished in {generation_time:.2f} seconds.")
    
    logging.info(f"Starting data population with {NUM_PROCESSES_FOR_DATA_POPULATION} processes...")
    start_time = time.monotonic()
    
    # 2. Distribute the keys among worker processes.
    chunk_size = (len(keys_to_load) + NUM_PROCESSES_FOR_DATA_POPULATION - 1) // NUM_PROCESSES_FOR_DATA_POPULATION
    connection_info = ("127.0.0.1", config.start_port)

    total_keys_inserted = 0
    with ProcessPoolExecutor(max_workers=NUM_PROCESSES_FOR_DATA_POPULATION) as executor:
        # Create a list of key chunks for each worker
        key_chunks = [
            keys_to_load[i : i + chunk_size]
            for i in range(0, len(keys_to_load), chunk_size)
        ]
        
        # Submit each chunk as a task to the process pool
        futures = [
            executor.submit(_populate_worker, chunk, connection_info, config.value_size_bytes)
            for chunk in key_chunks
        ]

        # 3. Collect the results as they complete.
        for future in as_completed(futures):
            try:
                keys_inserted = future.result()
                if keys_inserted > 0:
                    total_keys_inserted += keys_inserted
                    logging.info(f"A worker finished, {total_keys_inserted:,} / {num_keys:,} keys inserted so far.")
            except Exception as e:
                logging.error(f"A task generated an exception: {e}", exc_info=True)

    # 4. Log a final summary.
    load_time = time.monotonic() - start_time
    keys_per_second = total_keys_inserted / load_time if load_time > 0 else 0

    logging.info("--- Data Population Summary ---")
    logging.info(f"Target Keys:      {num_keys:,}")
    logging.info(f"Successfully Set: {total_keys_inserted:,}")
    logging.info(f"Total Time:       {load_time:.2f} seconds")
    logging.info(f"Rate:             {keys_per_second:,.2f} keys/sec")

    if total_keys_inserted != num_keys:
        logging.warning("Data population may be incomplete. Check logs for errors.")
        
    return keys_to_load
