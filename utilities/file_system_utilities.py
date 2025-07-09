import logging
import os
from pathlib import Path
import subprocess
import pandas as pd


def setup_directory_for_run(directory: Path):
    if not os.path.exists(directory):
        os.makedirs(directory)
        logging.info(f"Created directory: {directory} for this run.")
    else:
        logging.info(f"Using existing temporary directory: {directory}")
        subprocess.run(
            ["rm", "-rf", os.path.join(directory, "*")], check=True
        )  # Clear previous contents if they exist


def delete_file(file_path):
    logging.info(f"Attempting to delete file: {file_path}")
    # Check if the file exists before attempting to delete it
    if os.path.exists(file_path):
        try:
            os.remove(file_path)
            logging.info(f"Successfully deleted: {file_path}")
        except OSError as e:
            logging.error(f"Error deleting file {file_path}: {e}")
    else:
        logging.info(f"File not found, skipping deletion: {file_path}")


def save_results_to_csv(results: list[dict], output_dir: str, file_name: str):
    """
    Saves a list of dictionary results to a CSV file.

    Args:
        results: A list of dictionaries, where each dict is a row.
        output_dir: The directory to save the CSV file.
        file_name: The name of the file to save (e.g., "results.csv").
    """
    if not results:
        logging.warning("No results to save to CSV.")
        return

    try:
        output_path = os.path.join(output_dir, file_name)
        os.makedirs(output_dir, exist_ok=True)
        
        df = pd.DataFrame(results)
        df.to_csv(output_path, index=False)
        
        logging.info(f"Benchmark results successfully saved to: {output_path}")

    except Exception as e:
        logging.error(f"Failed to save results to CSV at {output_path}.", exc_info=True)
