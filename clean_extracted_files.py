import os
import shutil

extracted_files_dir = "extracted_files"

for source_dir in os.listdir(extracted_files_dir):
    source_path = os.path.join(extracted_files_dir, source_dir)
    
    if not os.path.isdir(source_path):
        continue
    
    # Keep only .gdb directories
    for item in os.listdir(source_path):
        item_path = os.path.join(source_path, item)
        
        if os.path.isdir(item_path) and (item.lower().endswith('.gdb') or item.lower().endswith('.gitkeep')):
            continue  # Keep .gdb directories
        
        # Delete everything else
        try:
            if os.path.isdir(item_path):
                shutil.rmtree(item_path)
            else:
                os.remove(item_path)
        except:
            pass

