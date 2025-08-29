# Valkey RDB Benchmarking

Python scripts to benchmark and CPU profile Valkey RDB save and load operations.

---

### Getting Started

To get the project running, follow these steps:

1.  **Set up the Python Environment**: This project uses a virtual environment to manage dependencies.

    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip3 install -r requirements.txt
    ```

---

### Configuration

The scripts can be configured using command-line arguments. For the required paths, you can also set environment variables in a `.env` file to avoid specifying them repeatedly.

* `VALKEY_SERVER_PATH`: The absolute path to your `valkey-server` executable. This is **required**.
* `VALKEY_JSON_MODULE_PATH`: The absolute path to the Valkey JSON module, only required if you use a JSON workload.

---

### Flame Graph Configuration

To use the `--gen-flamegraph` option for profiling, you must have `perf` and the **FlameGraph** tools installed.

1.  **Install `perf`**: `perf` is a Linux profiling tool. Its installation varies by distribution, but it is typically part of the `linux-tools` package. You may need to install the version that matches your kernel exactly.

2.  **Clone the FlameGraph Repository**: The profiling scripts use helper utilities from the official FlameGraph repository.

    ```bash
    git clone https://github.com/brendangregg/FlameGraph.git
    ```

3.  **Update your `.env` file**: Set the following variables in your `.env` file to point the scripts to the correct locations.

    ```ini
    PERF_PATH=/path/to/your/perf
    FLAME_GRAPH_REPO_PATH=/path/to/your/FlameGraph
    ```
### Automated Benchmark Scripts

Two bash scripts are provided to run a series of benchmarks with predefined configurations, saving the results automatically.

#### `run_full_load_benchmark.sh`

This script executes a sequence of `scripts.load_benchmark` runs with varying `num-keys` and `value-size` parameters. It activates the Python virtual environment, logs all output to a timestamped file, and deactivates the environment upon completion.

**Usage**
```bash
./run_full_load_benchmark.sh
```

#### `run_full_save_benchmark.sh`
Similar to the load benchmark script, this script runs multiple scripts.save_benchmark tests with different configurations. It handles virtual environment activation, logs output, and deactivation.

**Usage**
```bash
./run_full_save_benchmark.sh
```

---

### VM Setup Script
The setup_vm_for_testing.sh script automates most of the environment setup needed for running these benchmarks. It installs prerequisites, clones necessary repositories, builds Valkey and Valkey-JSON, and configures the Python virtual environment.

Important: You might need to customize this script to match your specific VM or system configuration, especially the repository URLs or installation paths.

What it does:

- Installs essential packages like git, python3-venv, build-essential, linux-perf, and cmake.

- Configures perf to allow non-root users to collect full stack traces.

- Clones the Valkey, Valkey-JSON, and valkey-rdb-benchmarking, and Flamegraph repositories.

- Builds the valkey-server executable and the valkey-json module.

- Sets up the Python virtual environment within the benchmark repository and installs requirements.txt.

- Creates a .env file in the valkey-rdb-benchmarking directory with paths to the built Valkey binaries and the Valkey JSON module.
---

### Benchmarking Commands Overview

All scripts are executed using the `python3 -m <script_name>` syntax.

* `scripts.save_benchmark`: Runs and profiles a Valkey RDB Save operation.
* `scripts.load_benchmark`: Runs and profiles a Valkey RDB Load operation.
* `scripts.save_validity_test`: Runs an RDB Save followed by an RDB Load and confirms that all keys present before the save were loaded back in correctly.

<br>

### Full Sync Benchmarking 
The full sync scripts are designed to be run concurrently on two separate machines or VMs to simulate a realistic master-replica replication scenario.

* `scripts.profile_full_sync_primary`: This script starts a primary server, populates it with keys, and waits for a replica to connect. When the replica connects, a full synchronization is triggered and profiled on the primary side. This is useful for analyzing IO information, CPU usage, and the runtime of the RDB save process initiated by the full sync.

* `scripts.profile_full_sync_replica`: This script starts a replica server that attempts to connect to a primary server. It then triggers a full sync and profiles the operation on the replica side, helping to identify performance bottlenecks during the RDB file load process.

---

### Example Commands:
#### `scripts.save_benchmark`

This script measures the performance of a Valkey `SAVE` operation, which saves the dataset to an RDB file.

**Usage:**

```bash
python3 -m scripts.save_benchmark [options]
```

#### Example:
This command runs a save benchmark with 10 million keys, each with a 100-byte string value, using 10 RDB saving threads.

```bash
python3 -m scripts.save_benchmark --num-keys 10 --value-size 100 --rdb-threads 10
```

---

#### `scripts.load_benchmark`
This script benchmarks the time it takes for a Valkey instance to load a pre-existing RDB file on startup.

**Usage**

```bash
python3 -m scripts.load_benchmark [options]
```

This command loads an RDB file with 5 million keys.

#### Example:

```bash
python3 -m scripts.load_benchmark --num-keys 5 --value-size 200
```

---

#### `scripts.save_validity_test`
This utility verifies the integrity of the RDB save and load process. It populates a Valkey instance, saves the data, then loads it into a new instance to confirm all keys were saved and loaded correctly.

**Usage**
```bash
python3 -m scripts.save_validity_test [options]
```

This command loads an RDB file with 5 million keys.

#### Example:

```bash
python3 -m scripts.save_validity_test --rdb-threads 10
```

### Full Sync Setup and Commands
To perform a full sync benchmark, you must run the primary and replica scripts on two separate VMs.

#### Setup:
You must edit the scripts.profile_full_sync_replica.py file to set the IP address of your primary VM. Look for the `PRIMARY_IP`
```python
# scripts.profile_full_sync_replica.py
PRIMARY_IP = "10.128.0.10"  # <--- Change this to the IP of your primary VM
```

#### Step 1: On Primary VM
Run the primary sync script. This will start the server, load keys and values, and wait for the replica to connect.

**Usage**
```bash
python3 -m scripts.profile_full_sync_primary [options]
```

#### Example:
This example starts the primary and loads 100 million keys of length 100 bytes.


```bash
python3 -m scripts.profile_full_sync_primary --num-keys 100 --value-size 100
```
#### Step 2: On Replica VM
Run the replica sync script. This will start the replica server, connect to the primary, and trigger the full sync.

**Usage**
```bash
python3 -m scripts.profile_full_sync_replica [options]
```

#### Example:
This starts the replica server and triggers a full sync with the primary (NOTE: Pass the same --num-keys value as you did to the primary).
```bash
python3 -m scripts.profile_full_sync_replica --num-keys 100
```




### Command-Line Arguments
Most scripts share a common set of arguments defined in utilities/parse_args.py.

--valkey-server-path <path>: Path to the Valkey server executable.

--start-port <int>: Starting port for the Valkey server (default: 7000).

--num-keys <float>: Number of keys to populate in millions (default: 10.0).

--value-size <int>: Size of the value in bytes (default: 100).

--conf <path>: Path to a Valkey configuration template file (default: default.conf).

--rdb-threads <int>: Number of RDB saving threads (default: 1).

--rdbcompression <yes|no|both>: Use LZF compression (default: yes).

--rdbchecksum <yes|no>: Calculate RDB file checksum (default: yes).

--io-threads <int>: Number of I/O threads for Valkey (default: 4).

--temp-dir <path>: Base directory for temporary data.

--tempfs: Use /dev/shm for temporary data.

--ssd-path <path>: Use a specified SSD path for temporary data.

--workload <string|user|session|product|heavy-product|analytics>: Type of data to populate (default: string).

--log-file <path>: Path to a file for writing logs.

--gen-flamegraph <bool>: Generate a flame graph.

### Workloads 
The --workload argument allows you to specify the type of data to populate the Valkey server with. Right now "Strings" are supported and a few JSON configurations (fake user, session, product or analytics JSON data). The functions used to generated JSON Data are located in `utilities/key_generation_utilities.py`.

### Benchmark Results and Visualizations
All benchmark results are saved to the results/ folder in a timestamped CSV format.

Example result visualizations, including graphs and data analysis, are available in the notebooks/ directory.

### Made By
A silly little intern

Website: [nickykhorasani.com](https://nickykhorasani.com) 