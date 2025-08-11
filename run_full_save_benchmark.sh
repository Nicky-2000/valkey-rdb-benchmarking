#!/bin/bash

# Exit on first error
set -e

# Log file with timestamp
LOG_FILE="benchmark_run_$(date +%Y%m%d_%H%M%S).log"

echo "Activating virtual environment..." | tee "$LOG_FILE"
source venv/bin/activate | tee -a "$LOG_FILE"

if [[ $? -ne 0 ]]; then
    echo "ERROR: Virtual environment activation failed!" | tee -a "$LOG_FILE"
    exit 1
fi
echo "Virtual environment activated." | tee -a "$LOG_FILE"
echo "Starting benchmarks. All output to $LOG_FILE" | tee -a "$LOG_FILE"
echo "----------------------------------------------------" | tee -a "$LOG_FILE"

declare -a benchmarks=(
    "2000 50 both"
    "1000 300 both"
    "500 500 both"
    "50 5000 both"
    "10 20000 both"
    "5 64000 both" # Slightly smaller than lower watermark
    "5 66000 both" # Slightly larger than lower watermark
    "1 100000 both"
    "0.5 262000 both" # Slightly smaller than upper watermark
    "0.5 272000 both" # Slightly larger than upper watermark
    "0.1 1000000 both" # 1MB
)

# Run benchmarks in a loop
for config in "${benchmarks[@]}"; do
    read -r num_keys value_size compression <<< "$config"
    description="Keys: ${num_keys}M, Value Size: ${value_size}B, Compression: ${compression}"

    echo -e "\n--- Running: $description ---" | tee -a "$LOG_FILE"
    python3 -m scripts.save_benchmark \
        --num-keys "${num_keys}" \
        --value-size "${value_size}" \
        --rdbcompression "${compression}" \
        2>&1 | tee -a "$LOG_FILE"

    if [[ $? -ne 0 ]]; then
        echo "ERROR: Test failed for $description" | tee -a "$LOG_FILE"
        echo "----------------------------------------------------" | tee -a "$LOG_FILE"
        # Optionally exit on first benchmark failure: exit 1
    fi
done

echo -e "\n----------------------------------------------------" | tee -a "$LOG_FILE"
echo "All benchmarks finished. Check $LOG_FILE for details." | tee -a "$LOG_FILE"

echo "Deactivating virtual environment..." | tee -a "$LOG_FILE"
deactivate | tee -a "$LOG_FILE"
echo "Script complete." | tee -a "$LOG_FILE"