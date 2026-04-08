# src/constants.py
import os

# project_path -> root of the repo where your config, metadata, extract folders exist
current_file_dir_path = os.path.dirname(os.path.realpath(__file__))
# parent of src/ is project root (dla folder)
project_path = os.path.dirname(current_file_dir_path)
