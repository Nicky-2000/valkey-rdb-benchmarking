import os
from pathlib import Path
import subprocess


def setup_directory_for_run(directory: Path):
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Created directory: {directory} for this run.")
    else:
        print(f"Using existing temporary directory: {directory}")
        subprocess.run(
            ["rm", "-rf", os.path.join(directory, "*")], check=True
        )  # Clear previous contents if they exist


def delete_file(file_path):
    print(f"Attempting to delete file: {file_path}")
    # Check if the file exists before attempting to delete it
    if os.path.exists(file_path):
        try:
            os.remove(file_path)
            print(f"Successfully deleted: {file_path}")
        except OSError as e:
            print(f"Error deleting file {file_path}: {e}")
    else:
        print(f"File not found, skipping deletion: {file_path}")
