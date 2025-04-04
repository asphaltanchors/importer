# MQI Pipeline Script

This script replaces the original `run_pipeline.sh` with a more flexible Python implementation that supports both full imports and daily incremental imports.

## Features

- **Full Import Mode**: Drops and recreates all data (similar to original script)
- **Daily Import Mode**: Processes files from a directory in date sequence
- **Test Mode**: Scans files and shows what would be processed without executing
- **Dry Run Mode**: Shows commands that would be executed without actually running them
- **Future Support**: Placeholders for file moving and archiving functionality

## Requirements

- Python 3.6+
- Meltano installed and configured
- Database connection configured in Meltano

## Usage

### Full Import

To run a full import (similar to the original script):

```bash
./pipeline.py --full
```

This will:
1. Import the full dataset
2. Import the 90-day dataset
3. Run the matcher

### Daily Import

To process files from a directory in date sequence:

```bash
./pipeline.py --daily /path/to/files/directory
```

This will:
1. Scan the directory for files with the naming pattern `[Type]_MM_DD_YYYY_H_MM_SS.csv`
2. Group files by date
3. For each date with all required files (Item, Customer, Invoice, Sales Receipt):
   - Import the data using Meltano
   - Run DBT transformations
4. After all imports are complete, run the matcher once

### Test Mode

To see what files would be processed without actually running any imports:

```bash
./pipeline.py --daily /path/to/files/directory --test
```

This will show a detailed report of which dates have all required files and which ones would be skipped.

### Dry Run Mode

To see what commands would be executed without actually running them:

```bash
./pipeline.py --daily /path/to/files/directory --dry-run
```

## File Naming Requirements

The script expects files to follow this naming pattern:
- `Invoice_MM_DD_YYYY_H_MM_SS.csv`
- `Sales Receipt_MM_DD_YYYY_H_MM_SS.csv`
- `Customer_MM_DD_YYYY_H_MM_SS.csv`
- `Item_MM_DD_YYYY_H_MM_SS.csv`

All four file types must be present for a date to be processed.

## Future Enhancements

The script includes placeholders for future enhancements:

- **File Moving**: `--move-files` option (not implemented yet)
- **File Archiving**: `--archive` option (not implemented yet)
