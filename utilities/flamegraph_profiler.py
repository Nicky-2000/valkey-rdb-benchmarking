import logging
import subprocess
import signal
import os
from pathlib import Path

class FlamegraphProfiler:
    """
    A context manager to handle starting and stopping the 'perf' profiler
    and generating a flame graph from its output.
    """
    def __init__(self, pid: int, output_dir: Path):
        self.pid = pid
        self.output_dir = output_dir
        self.perf_process = None

        # Load required paths from environment variables
        self.perf_path = os.getenv("PERF_PATH")
        self.flamegraph_repo_path = os.getenv("FLAME_GRAPH_REPO_PATH")

        if not self.perf_path or not Path(self.perf_path).is_file():
            raise FileNotFoundError("PERF_PATH is not set in .env or is not a valid file.")
        
        if not self.flamegraph_repo_path or not Path(self.flamegraph_repo_path).is_dir():
            raise FileNotFoundError("FLAME_GRAPH_REPO_PATH is not set in .env or is not a valid directory.")

    def __enter__(self):
        """Starts the perf record process when entering the 'with' block."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        perf_data_path = self.output_dir / "perf.data"
        
        logging.info(f"Starting perf record for PID {self.pid}, output to {perf_data_path}")
        
        command = [
            self.perf_path, "record", "-F", "999", "-g", 
            "-p", str(self.pid), "-o", str(perf_data_path)
        ]

        self.perf_process = subprocess.Popen(command, preexec_fn=os.setsid)
        logging.info(f"Perf process started with PID {self.perf_process.pid}.")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Stops the perf process cleanly when exiting the 'with' block."""
        if self.perf_process:
            logging.info(f"Stopping perf process (PID: {self.perf_process.pid})...")
            try:
                # Send SIGINT (Ctrl+C) to stop perf gracefully
                os.killpg(os.getpgid(self.perf_process.pid), signal.SIGINT)
                self.perf_process.wait(timeout=15)
                logging.info("Perf process stopped successfully.")
            except (subprocess.TimeoutExpired, ProcessLookupError) as e:
                logging.error(f"Failed to stop perf gracefully: {e}. Terminating...", exc_info=True)
                self.perf_process.terminate()
                self.perf_process.wait()
        
    def generate(self, flamegraph_file_name: str = "flamegraph.svg"):
        """Generates the flame graph from the collected perf data."""
        logging.info("--- Starting Flame Graph Generation ---")
        perf_data_path = self.output_dir / "perf.data"
        svg_output_path = self.output_dir / flamegraph_file_name

        if not perf_data_path.exists():
            logging.error(f"perf.data not found at {perf_data_path}. Cannot generate flame graph.")
            return

        try:
            # Define paths for intermediate files and scripts
            stackcollapse_script = Path(self.flamegraph_repo_path) / "stackcollapse-perf.pl"
            flamegraph_script = Path(self.flamegraph_repo_path) / "flamegraph.pl"
            
            # 1. perf script: Convert perf.data to human-readable format
            logging.info(f"Running: {self.perf_path} script...")
            script_output = subprocess.check_output([self.perf_path, "script", "-i", str(perf_data_path)])

            # 2. stackcollapse-perf.pl: Collapse the stack traces
            logging.info("Running: stackcollapse-perf.pl...")
            collapsed_stacks = subprocess.check_output(
                [str(stackcollapse_script)], input=script_output
            )

            # 3. flamegraph.pl: Generate the final SVG
            logging.info("Running: flamegraph.pl...")
            with open(svg_output_path, "wb") as f_svg:
                subprocess.run(
                    [str(flamegraph_script)], input=collapsed_stacks, stdout=f_svg, check=True
                )

            logging.info(f"✅ Flame graph generated successfully at: {svg_output_path}")

        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            logging.error("Error during flame graph generation.", exc_info=True)

