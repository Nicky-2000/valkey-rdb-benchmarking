#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# --- Environment Variables ---
# You can customize these if your repos have different names
REPO_VALKEY_URL="https://github.com/Nicky-2000/valkey.git"
REPO_BENCHMARK_URL="https://github.com/Nicky-2000/valkey-rdb-benchmarking.git"
REPO_VALKEY_DIR="valkey"
REPO_BENCHMARK_DIR="valkey-rdb-benchmarking"

# --- 1. Install Prerequisites ---
echo "--- Installing essential packages ---"
sudo apt-get update
sudo apt-get install -y git python3-venv build-essential

# --- 2. Clone Repositories ---
echo "--- Cloning repositories ---"
if [ ! -d "$REPO_VALKEY_DIR" ]; then
    git clone "$REPO_VALKEY_URL"
    cd "$REPO_VALKEY_DIR"
    git checkout rdb-save-multi-thread
    cd ..
else
    echo "Valkey repository already exists. Skipping clone."
fi

if [ ! -d "$REPO_BENCHMARK_DIR" ]; then
    git clone "$REPO_BENCHMARK_URL"
else
    echo "Benchmark repository already exists. Skipping clone."
fi

# --- 3. Build Valkey Server ---
echo "--- Building Valkey server ---"
cd "$REPO_VALKEY_DIR"
make
cd ..

# --- 4. Set up Python Environment ---
echo "--- Setting up Python virtual environment and installing requirements ---"
cd "$REPO_BENCHMARK_DIR"
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt


# --- 5. Create .env file with correct paths ---
echo "--- Creating .env file with Valkey binary paths ---"
# Get the absolute path to the cloned valkey directory
VALKEY_ROOT_PATH=$(realpath "$REPO_VALKEY_DIR")

# Construct the paths to the binaries
VALKEY_SERVER_BIN="$VALKEY_ROOT_PATH/src/valkey-server"
VALKEY_CLI_BIN="$VALKEY_ROOT_PATH/src/valkey-cli"
VALKEY_BENCHMARK_BIN="$VALKEY_ROOT_PATH/src/valkey-benchmark"

# Create the .env file in the benchmark repo root
ENV_FILE_PATH="$REPO_BENCHMARK_DIR/.env"

cat <<EOF > "$ENV_FILE_PATH"
VALKEY_SERVER_PATH=$VALKEY_SERVER_BIN
VALKEY_CLIENT_PATH=$VALKEY_CLI_BIN
VALKEY_BENCHMARK_PATH=$VALKEY_BENCHMARK_BIN
EOF

echo "Created .env file at $ENV_FILE_PATH with the following content:"
cat "$ENV_FILE_PATH"

echo "--- Setup complete! ---"
echo "To activate your Python environment, run: 'source $REPO_BENCHMARK_DIR/venv/bin/activate'"
echo "Remember to source the .env file or ensure your scripts load it."