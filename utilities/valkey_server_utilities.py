from pathlib import Path
import subprocess
import time
import os
import sys
import valkey
from utilities.parse_args import BenchmarkConfig
import logging


def start_standalone_valkey_server(config: BenchmarkConfig, clear_data_dir: bool = True, log_file_suffix: str = "") -> subprocess.Popen | None:
    """
    Starts a standalone Valkey Server Instance
    """
    logging.info("Starting Valkey Standalone Server")
    
    data_dir = Path(config.temp_dir) / f"node_data_{config.start_port}"
    valkey_log_file_path = Path(config.temp_dir) / f"node_log_{config.start_port}{log_file_suffix}.log"

    # Only clear the directory if the flag is set to True
    if clear_data_dir and data_dir.exists():
        logging.info(f"Clearing existing data directory: {data_dir}")
        subprocess.run(["rm", "-rf", str(data_dir)], check=True)
    
    # Always ensure the directory exists after the optional cleanup
    data_dir.mkdir(parents=True, exist_ok=True)

    # Command to start Valkey server
    command = [
        str(config.valkey_server_path),
        "--port",
        str(config.start_port),
        "--dir",
        str(data_dir),
        "--logfile",
        str(valkey_log_file_path),
        "--io-threads",
        str(config.io_threads),
        "--rdb-threads",
        str(config.rdb_threads),
        "--save",
        "",
        "--rdbcompression",
        config.rdb_compression,
        "--rdbchecksum",
        config.rdb_checksum,
        "--dbfilename",
        "dump.rdb",
        "--bind", "0.0.0.0",
        "--protected-mode", "no",
        "--repl-diskless-sync", "yes",
        "--repl-diskless-load", "swapdb",
        # "--dual-channel-replication-enabled ", "yes",
        
    ]

    # Dynamically add the --loadmodule argument if a path is provided in the config
    if config.valkey_json_module_path:
        if not Path(config.valkey_json_module_path).is_file():
            logging.error(f"Valkey JSON module not found at: {config.valkey_json_module_path}")
            return None
        command.extend(["--loadmodule", config.valkey_json_module_path])
        logging.info(f"Valkey-JSON module will be loaded from: {config.valkey_json_module_path}")

    logging.info(f"Valkey startup command: {' '.join(command)}")
    
    try:
        # Start the server process
        process = subprocess.Popen(command, preexec_fn=os.setsid)
        logging.info(
            f"Valkey server starting with PID: {process.pid} on port {config.start_port}. Log: {valkey_log_file_path}"
        )
        return process
    except Exception as e:
        logging.error(
            f"Error starting Valkey server on port {config.start_port}: {e}",
            exc_info=True,
        )
        return None



def wait_for_server_to_start(config: BenchmarkConfig, timeout_seconds: int = 30, polling_interval: int = 2) -> valkey.Valkey | None:
    """
    Waits for a Valkey server to become reachable by polling it until a timeout.

    Args:
        config: The BenchmarkConfig object with connection details.
        timeout_seconds: The maximum time to wait for the server to start.
        polling_interval: The sleep time between attempts to reach the server.

    Returns:
        A connected valkey.Valkey client instance, or None if it fails to connect.
    """
    start_time = time.monotonic()

    while time.monotonic() - start_time < timeout_seconds:
        try:
            vk_client = valkey.Valkey(
                host="127.0.0.1", port=config.start_port, decode_responses=True
            )
            vk_client.ping()  # Will raise ConnectionError if not reachable
            logging.info(f"Server on port {config.start_port} is reachable.")
            return vk_client
        except valkey.exceptions.ConnectionError as e:
            elapsed = time.monotonic() - start_time
            logging.info(
                f"Waiting for Valkey server on port {config.start_port}... ({elapsed:.1f}s / {timeout_seconds}s). Error: {e}"
            )
        time.sleep(polling_interval)

    logging.error(f"Failed to connect to Valkey server on port {config.start_port} after {timeout_seconds} seconds.")
    return None

def stop_valkey_server(process: subprocess.Popen, client: valkey.Valkey | None):
    """
    Stops a Valkey server instance gracefully, with a forceful kill as a fallback.
    The client object can be None if the server failed to start but the process exists.
    """
    # Derive port from client if available, for logging purposes.
    port = "N/A"
    if client:
        # Safely get the port from the client's connection pool
        port = client.connection_pool.connection_kwargs.get("port", "N/A")

    # Check if the process exists and is running
    if not process or process.poll() is not None:
        logging.info(f"Valkey process for port {port} already stopped or was not provided.")
        return

    logging.info(f"Stopping Valkey server on port {port} (PID: {process.pid})...")

    # 1. Try to shut down gracefully using the client
    if client:
        try:
            # SHUTDOWN is the preferred, clean way to stop the server
            client.shutdown(save=False)
            time.sleep(1)  # Give the server a moment to shut down
        except valkey.exceptions.ConnectionError:
            # This is expected if the server shuts down before the command returns
            logging.info(f"Server on port {port} disconnected as expected during graceful shutdown.")
        except Exception:
            logging.error(f"An error occurred during graceful shutdown for port {port}.", exc_info=True)

    # 2. Forcefully kill the process if it's still running
    if process.poll() is None:
        logging.warning(f"Server process {process.pid} is still running. Killing forcefully (SIGKILL)...")
        try:
            # os.killpg is more reliable for stopping the process and any children
            os.killpg(os.getpgid(process.pid), 9)
        except ProcessLookupError:
            logging.warning(f"Process {process.pid} not found for force kill. It likely terminated.")
        except Exception:
            logging.error(f"Failed to forcefully kill process {process.pid}.", exc_info=True)

    # 3. Wait for the process to terminate and clean up
    process.wait(timeout=20)
    logging.info(f"Valkey server on port {port} has been stopped.")

