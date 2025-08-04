import argparse
import os
from pathlib import Path
import time
import logging
from dataclasses import dataclass, asdict
from dotenv import load_dotenv

# --- Default Values ---
START_PORT_DEFAULT = 7000
NUM_KEYS_MILLIONS_DEFAULT = 10.0
VALUE_SIZE_BYTES_DEFAULT = 100
RDB_THREADS_DEFAULT = 1
IO_THREADS_DEFAULT = 4
TEST_CONF_TEMPLATE_DEFAULT = "default.conf"
TEMP_SUBDIR_DEFAULT = "valkey_temp"
FLAMEGRAPH_OUTPUT_DIR_DEFAULT = "flamegraphs"


# --- Data Class for Configuration ---
@dataclass(frozen=False)
class BenchmarkConfig:
    """
    Holds all configuration parameters for the Valkey rdb benchmark.
    This is a frozen dataclass, meaning its instances are immutable.
    """
    valkey_server_path: str 
    start_port: int
    num_keys_millions: int
    value_size_bytes: int
    conf_template: str
    rdb_threads: int
    rdb_compression: str
    rdb_checksum: str
    io_threads: int
    temp_dir: str
    log_file: str | None # Using | None for Python 3.10+, can be Optional[str] for older versions
    gen_flamegraph: bool

def setup_logging(log_file: str | None):
    """
    Configures logging to either a file or the console.

    Args:
        log_file: The path to the log file. If None, logs are sent to the console.
    """
    # Define the format for log messages
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    
    # Configure basic logging settings
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        filename=log_file,  # If None, logs to console (stderr); otherwise, logs to the file
        filemode='w',       # 'w' overwrites the file each time; use 'a' to append
    )
    
    # If logging to console, also add a handler to explicitly direct to stdout
    # to avoid mixing with potential stderr from other parts of the system.
    if log_file is None:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(log_format))
        # Clear existing handlers and add our specific one
        logging.getLogger().handlers = [console_handler]


def display_config(config: BenchmarkConfig):
    """Logs the benchmark configuration in a readable, aligned format."""
    
    # Building a multi-line string for a single log entry is often cleaner.
    config_dict = asdict(config)
    max_key_length = max(len(key) for key in config_dict)
    
    # Start building the log message string
    log_message = "\n--- Valkey Benchmark Configuration ---\n"
    
    for key, value in config_dict.items():
        formatted_key = key.replace('_', ' ').title()
        if formatted_key == "Num Keys Millions":
            log_message += f"  {"Num Keys":<{max_key_length + 2}}: {int(value * 10**6)}\n"
        else:
            log_message += f"  {formatted_key:<{max_key_length + 2}}: {value}\n"

    log_message += "--------------------------------------"
    
    logging.info(log_message)


def parse_benchmark_args() -> BenchmarkConfig:
    """
    Parses command-line arguments and returns a structured BenchmarkConfig object.
    """
    parser = argparse.ArgumentParser(
        description="Valkey RDB Persistence Benchmark Tool for Standalone"
    )
    
    load_dotenv()
    # --- Get defaults from environment for path arguments ---
    default_server_path = os.getenv("VALKEY_SERVER_PATH")
    
    # Path Configuration
    parser.add_argument(
        "--valkey-server-path", type=str, default=default_server_path,
        # required=default_server_path is None, # Required if not set in .env
        help="Path to valkey-server. Defaults to VALKEY_SERVER_PATH in .env file."
    )
    # Server and Data Configuration
    parser.add_argument(
        "--start-port", type=int, default=START_PORT_DEFAULT,
        help=f"Starting port for Valkey server (default: {START_PORT_DEFAULT})",
    )
    parser.add_argument(
        "--num-keys", type=float, default=NUM_KEYS_MILLIONS_DEFAULT,
        help=f"Number of keys to populate in millions (default: {NUM_KEYS_MILLIONS_DEFAULT})",
    )
    parser.add_argument(
        "--value-size", type=int, default=VALUE_SIZE_BYTES_DEFAULT,
        help=f"Size of the value in bytes for populated keys (default: {VALUE_SIZE_BYTES_DEFAULT})",
    )
    parser.add_argument(
        "--conf", type=str, default=TEST_CONF_TEMPLATE_DEFAULT,
        help=f"Path to the Valkey server configuration file template (default: {TEST_CONF_TEMPLATE_DEFAULT}).",
    )

    # RDB and Server Configuration
    parser.add_argument(
        "--rdb-threads", type=int, default=RDB_THREADS_DEFAULT,
        help=f"Number of threads to save keys with (default: {RDB_THREADS_DEFAULT})",
    )
    parser.add_argument(
        "--rdbcompression", type=str, default="yes", choices=["yes", "no"],
        help="Use LZF compression for RDB files (default: 'yes')",
    )
    parser.add_argument(
        "--rdbchecksum", type=str, default="yes", choices=["yes", "no"],
        help="Calculate a checksum for RDB files (default: 'yes')",
    )
    parser.add_argument(
        "--io-threads", type=int, default=IO_THREADS_DEFAULT,
        help=f"Number of I/O Threads for the Valkey Server (default: {IO_THREADS_DEFAULT})",
    )
    
    # Script Behavior and Environment
    parser.add_argument(
        "--temp-dir", type=str, default=None,
        help="Base directory for temporary data. Overrides --tempfs.",
    )
    parser.add_argument(
        "--tempfs", type=bool, default=False,
        help="Use a tempfs directory (/dev/shm) for temporary data.",
    )
    
    # Logging Configuration
    parser.add_argument(
        "--log-file", type=str, default=None,
        help="Path to a file to write logs to. If not provided, logs are printed to the terminal."
    )
    
    parser.add_argument(
        "--gen-flamegraph", 
        action=argparse.BooleanOptionalAction, 
        default=False,
        help="Generate a flame graph for each test iteration.",
    )

    args = parser.parse_args()
    
    # --- Validate paths ---
    if not Path(args.valkey_server_path).is_file():
        parser.error(f"Valkey server path is not a valid file: {args.valkey_server_path}")

    # --- Post-process arguments and create the dataclass instance ---
    if args.temp_dir:
        temp_dir = args.temp_dir
    elif args.tempfs:
        temp_dir = os.path.join("/dev/shm", f"{TEMP_SUBDIR_DEFAULT}_{time.time_ns()}")
    else:
        temp_dir = os.path.join(os.getcwd(), f"{TEMP_SUBDIR_DEFAULT}_{time.time_ns()}")
    
    setup_logging(args.log_file)

    return BenchmarkConfig(
        valkey_server_path=args.valkey_server_path,
        start_port=args.start_port,
        num_keys_millions=args.num_keys,
        value_size_bytes=args.value_size,
        conf_template=args.conf,
        rdb_threads=args.rdb_threads,
        rdb_compression=args.rdbcompression,
        rdb_checksum=args.rdbchecksum,
        io_threads=args.io_threads,
        temp_dir=temp_dir,
        log_file=args.log_file,
        gen_flamegraph=args.gen_flamegraph
    )