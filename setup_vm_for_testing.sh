#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# --- Environment Variables ---
# You can customize these if your repos have different names
REPO_VALKEY_URL="https://github.com/Nicky-2000/valkey.git"
REPO_VALKEY_JSON_URL="https://github.com/valkey-io/valkey-json.git"
REPO_BENCHMARK_URL="https://github.com/Nicky-2000/valkey-rdb-benchmarking.git"
REPO_FLAMEGRAPH_URL="https://github.com/brendangregg/FlameGraph.git"

REPO_VALKEY_DIR="valkey"
REPO_VALKEY_JSON_DIR="valkey-json"
REPO_BENCHMARK_DIR="valkey-rdb-benchmarking"
REPO_FLAMEGRAPH_DIR="FlameGraph"


# --- 1. Install Prerequisites ---
echo "--- Installing essential packages ---"
sudo apt-get update
sudo apt-get install -y git python3-venv build-essential linux-perf
sudo apt-get install -y cmake

# --- 1a. Configure perf for non-root users ---
echo "--- Configuring perf for non-root access ---"
# This allows non-root users to collect full stack traces
sudo sh -c 'echo -1 > /proc/sys/kernel/perf_event_paranoid'

# --- 2. Clone Repositories ---

# Valkey
echo "--- Cloning repositories ---"
if [ ! -d "$REPO_VALKEY_DIR" ]; then
    git clone "$REPO_VALKEY_URL"
    cd "$REPO_VALKEY_DIR"
    git checkout RDB-Load-multi-threaded # CHANGE THIS TO WHAT BRANCH YOU WANT TO TEST ON
    cd ..
else
    echo "Valkey repository already exists. Skipping clone."
fi

# Valkey JSON Module
if [ ! -d "$REPO_VALKEY_JSON_URL" ]; then
    git clone "$REPO_VALKEY_JSON_URL"
else
    echo "Valkey-Json repository already exists. Skipping clone."
fi

# valkey-rdb-benchmarking
if [ ! -d "$REPO_BENCHMARK_DIR" ]; then
    git clone "$REPO_BENCHMARK_URL"
else
    echo "Benchmark repository already exists. Skipping clone."
fi

# Flamegraph
if [ ! -d "$REPO_FLAMEGRAPH_DIR" ]; then # New step for FlameGraph
    git clone "$REPO_FLAMEGRAPH_URL"
else
    echo "FlameGraph repository already exists. Skipping clone."
fi




# --- 3. Build Valkey Server ---
echo "--- Building Valkey server ---"
cd "$REPO_VALKEY_DIR"
make
cd ..

# --- 3. Build Valkey JSON Module ---
echo "--- Building Valkey server ---"
cd "$REPO_VALKEY_JSON_DIR"
./build.sh
cd ..

# --- 4. Set up Python Environment ---
echo "--- Setting up Python virtual environment and installing requirements ---"
cd "$REPO_BENCHMARK_DIR"
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

deactivate
cd ..

# --- 5. Create .env file with correct paths ---
echo "--- Creating .env file with Valkey binary paths ---"
# Get the absolute path to the cloned valkey directory
VALKEY_ROOT_PATH=$(realpath "$REPO_VALKEY_DIR")
REPO_VALKEY_JSON_ROOT_PATH=$(realpath "$REPO_VALKEY_JSON_DIR")
REPO_FLAMEGRAPH_ROOT_PATH=$(realpath "$REPO_FLAMEGRAPH_DIR")

# Construct the paths to the binaries
VALKEY_SERVER_BIN="$VALKEY_ROOT_PATH/src/valkey-server"
VALKEY_CLI_BIN="$VALKEY_ROOT_PATH/src/valkey-cli"
VALKEY_BENCHMARK_BIN="$VALKEY_ROOT_PATH/src/valkey-benchmark"
VALKEY_JSON_MODULE_BIN="$REPO_VALKEY_JSON_ROOT_PATH/build/src/libjson.so"
FLAMEGRAPH_REPO_PATH="$REPO_FLAMEGRAPH_ROOT_PATH"
PERF_PATH="/usr/bin/perf" # Common path for perf, you might need to adjust

# Create the .env file in the benchmark repo root
ENV_FILE_PATH="$REPO_BENCHMARK_DIR/.env"

cat <<EOF > "$ENV_FILE_PATH"
VALKEY_SERVER_PATH=$VALKEY_SERVER_BIN
VALKEY_CLIENT_PATH=$VALKEY_CLI_BIN
VALKEY_BENCHMARK_PATH=$VALKEY_BENCHMARK_BIN
VALKEY_JSON_MODULE_PATH=$VALKEY_JSON_MODULE_BIN
FLAME_GRAPH_REPO_PATH=$FLAMEGRAPH_REPO_PATH
PERF_PATH=$PERF_PATH
EOF

echo "Created .env file at $ENV_FILE_PATH with the following content:"
cat "$ENV_FILE_PATH"

echo "--- Setup complete! ---"
echo "To activate your Python environment, run: 'source $REPO_BENCHMARK_DIR/venv/bin/activate'"
echo "Remember to source the .env file or ensure your scripts load it."