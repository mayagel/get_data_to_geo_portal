# GIS Data Ingestion System - Documentation

## Project Overview

### Objective
Import all GDB (File Geodatabase) files from the archaeological excavations archive at `\\iaahq\Archaeological_Documentation\North\Excavations` into our PostgreSQL/PostGIS database for centralized access via the geographic portal.

### Challenge
The archive contains 452 directories with prefix "A-" (excavation sites), each containing various file types including GIS data, images, documents, and compressed archives. Manual import would be impractical and error-prone.

---

## Solution: Automated GIS Data Ingestion Program

### What the Program Does

1. **Scans** the excavations directory for all folders starting with "A-" prefix
2. **Focuses** only on GIS-relevant files:
   - Compressed files (.zip, .7z, .rar)
   - GDB files and directories
3. **Extracts** compressed files to find nested GDB files
4. **Imports** all GDB layers into versioned database tables
5. **Tracks** processing to enable safe resumption if interrupted

---

## Key Features

### Smart Filtering System

The program filters directories through multiple stages:

1. **Already Processed Filter**
   - Checks `extracted_files/` directory for previously processed sites
   - Skips directories that were already imported

2. **Size Filter** 
   - Calculates size of only GIS resources (compressed + .gdb files)
   - Skips directories where GIS resources exceed 50GB
   - Uses parallel processing for speed (10 threads)

3. **Manual Skip List**
   - `huge_dirs.txt` contains directories to always skip
   - Useful for exceptionally large sites that need manual handling

### Data Organization

**Database Tables Created:**

1. **`North_Excavations_header`** - Summary table with one row per GDB file
   - `ingestion_id` - Unique identifier for each GDB
   - `f_name` - GDB filename
   - `s_dir` - Full source directory path  
   - `main_folder_name` - Directory name (e.g., A-8569_Darchmon_20191030)
   - `poly_ver`, `line_ver`, `point_ver` - Version identifiers for each geometry type
   - `poly_count`, `line_count`, `point_count` - Feature counts
   - `from_compressed` - Flag (1 if from archive, 0 if direct)
   - `creation_date`, `update_date`, `creation_user`, `update_user` - Audit fields

2. **`North_Excavations_header_rows_{geometry}_{version}`** - Data tables
   - Format: `North_Excavations_header_rows_poly_verA`
   - Separate tables for each geometry type (poly/line/point) and schema version
   - Contains all original GDB fields plus metadata:
     - `creation_date`, `update_date`
     - `creation_user`, `update_user`
     - `ingestion_id` - Links to header table

### Version System

The program automatically detects different column schemas and creates versioned tables:

- **Versions**: A, B, C, ..., Z, AA, AB, AC, ..., AZ (52 total versions)
- **Purpose**: Handles variations in GDB schemas without data loss
- **Tracking**: `layers_version.txt` file records:
  ```
  poly_verA: \\iaahq\...\A-8569, A8569.gdb, [column1, column2, ...]
  line_verB: \\iaahq\...\A-7972, A7972.gdb, [column1, column3, ...]
  ```

### Extraction Strategy

When compressed files are found:

1. Extract to `extracted_files/{source_directory}/`
2. Search recursively for all nested .gdb directories
3. Copy found GDBs to root level for easy access
4. Delete non-GDB files to save disk space
5. Keep directory as marker that processing is complete

---

## Resumability & Safety

### The program is fully resumable:

- **Stop anytime** - Can be interrupted and restarted safely
- **No duplicates** - Ingestion IDs continue from database maximum
- **Version awareness** - Loads existing versions from database
- **Progress tracking** - Directories in `extracted_files/` indicate completion

### Automatic Cleanup

- Runs cleanup every 5 directories processed
- Removes all non-GDB files from `extracted_files/`
- Keeps only essential .gdb directories for tracking

---

## Files Generated

### Tracking Files

1. **`extracted_files/`** - Directory containing processed sites
   - Each subdirectory name indicates a processed source directory
   - Contains only .gdb files for reference

2. **`extracted_here_files.txt`** - Log of extracted archives
   - Full paths to compressed files that were extracted
   - Prevents re-extraction of same archives

3. **`huge_dirs.txt`** - Manual skip list
   - Directory names to always skip
   - For sites too large or requiring special handling

4. **`layers_version.txt`** - Version documentation
   - Records which GDB created each version
   - Lists columns for each version
   - Useful for understanding schema variations

### Log Files

- Location: `logs/gis_ingestion_{datetime}.log`
- Contains detailed processing information
- Tracks successes, warnings, and errors

---

## Results

After execution:

- ✅ All GDB files from 452 excavation sites processed (except filtered)
- ✅ Data organized in versioned tables by geometry type
- ✅ Complete audit trail in database
- ✅ Full traceability: source directory → GDB → table → features
- ✅ Ready for access via geographic portal

---

## Usage

```bash
python main.py
```

The program runs automatically and provides progress updates in the log.

---

## Technical Notes

- **Platform**: Windows/Linux compatible
- **Dependencies**: ArcPy, PostgreSQL/PostGIS
- **Performance**: Parallel size checking (10 threads), early exit optimization
- **Safety**: Versioned tables prevent data conflicts, full error handling

