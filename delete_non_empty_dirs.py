"""
Simple script to delete all directories containing files in extracted_files folder
"""

import os
import shutil
from pathlib import Path

# Path to extracted_files directory
EXTRACTED_FILES_DIR = Path("extracted_files")

def delete_non_empty_directories():
    """
    Deletes all directories in extracted_files that contain any files or subdirectories
    """
    if not EXTRACTED_FILES_DIR.exists():
        print(f"Error: {EXTRACTED_FILES_DIR} does not exist")
        return
    
    deleted_count = 0
    
    # Iterate through all items in extracted_files
    for item in EXTRACTED_FILES_DIR.iterdir():
        if item.is_dir():
            # Check if directory is not empty
            try:
                if any(item.iterdir()):
                    print(f"Deleting: {item.name}")
                    shutil.rmtree(item)
                    deleted_count += 1
            except Exception as e:
                print(f"Error deleting {item.name}: {e}")
    
    print(f"\nDone! Deleted {deleted_count} directories.")

if __name__ == "__main__":
    delete_non_empty_directories()
