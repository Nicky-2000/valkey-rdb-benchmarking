# valkey-rdb-benchmarking
Python Scripts to benchmark and CPU profile Valkey RDB Save and Load Operations


python3 -m venv venv

source venv/bin/activate

pip3 install -r requirements.txt

### Running Validity Test (Make sure that keys were saved correctly)
python3 -m scripts.save_validity_test

Example: 
python3 -m scripts.save_validity_test --rdb-threads 10

### Running RDB Save Benchmark
This will collect benchmark data for saving the database with 10 Million Keys with values of length 100.  
python3 -m scripts.save_benchmark --num_keys 10 --value-size 100
