import pandas as pd
import sys

# Load data
print(f"Running {sys.argv}")
print(f"Pandas version: {pd.__version__}")
print(f"loading data from {sys.argv[1]}")
print("Job finish successfully")