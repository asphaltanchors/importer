import pandas as pd
from pathlib import Path
from typing import Generator
import logging
import shutil
from datetime import datetime
import fcntl
import os
from .processors.base import BaseProcessor
from .db.session import SessionManager

logger = logging.getLogger(__name__)

class CSVImporter:
    def __init__(self, config):
        self.config = config
        self.session_manager = SessionManager(config.database_url)
        
    def setup_directories(self, base_dir: Path) -> None:
        """Create required subdirectories if they don't exist."""
        (base_dir / "processed").mkdir(exist_ok=True)
        (base_dir / "failed").mkdir(exist_ok=True)
        (base_dir / "logs").mkdir(exist_ok=True)
        
    def process_directory(self, directory: Path) -> None:
        """Process all CSV files in directory."""
        self.setup_directories(directory)
        
        for csv_file in directory.glob('*.csv'):
            # Skip files in subdirectories
            if csv_file.parent != directory:
                continue
                
            try:
                # Try to acquire lock
                lock_file = csv_file.with_suffix('.lock')
                with open(lock_file, 'w') as f:
                    try:
                        fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except IOError:
                        logger.info(f"Skipping {csv_file} - already being processed")
                        continue
                        
                    try:
                        self.process_file(csv_file)
                    except Exception as e:
                        logger.error(f"Error processing {csv_file}: {e}")
                    finally:
                        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
                        
                if lock_file.exists():
                    lock_file.unlink()
                    
            except Exception as e:
                logger.error(f"Error handling {csv_file}: {e}")
                
    def process_file(self, file_path: Path) -> None:
        """Log information about CSV file that would be processed."""
        file_size = file_path.stat().st_size
        file_modified = datetime.fromtimestamp(file_path.stat().st_mtime)
        
        logger.info(f"Found CSV file: {file_path}")
        logger.info(f"  Size: {file_size / 1024:.2f} KB")
        logger.info(f"  Last modified: {file_modified}")
        
        # Preview the CSV structure
        try:
            # Read just the header and first row to show structure
            df = pd.read_csv(file_path, nrows=1)
            logger.info(f"  Number of columns: {len(df.columns)}")
        except Exception as e:
            logger.error(f"Error reading CSV structure: {e}")
            
    def _read_chunks(self, file_path: Path) -> Generator[pd.DataFrame, None, None]:
        """Placeholder for future chunk processing."""
        yield pd.DataFrame()
            
    def _process_chunk(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """Placeholder for future chunk processing."""
        return chunk
        
    def _save_to_database(self, data: pd.DataFrame) -> None:
        """Placeholder for future database saving."""
        pass
                
    def _move_to_processed(self, file_path: Path) -> None:
        """Move successfully processed file to processed directory."""
        date_dir = file_path.parent / "processed" / datetime.now().strftime("%Y-%m-%d")
        date_dir.mkdir(parents=True, exist_ok=True)
        
        new_path = date_dir / file_path.name
        shutil.move(str(file_path), str(new_path))
        logger.info(f"Moved {file_path} to {new_path}")
        
    def _move_to_failed(self, file_path: Path, error_message: str) -> None:
        """Move failed file to failed directory with error log."""
        date_dir = file_path.parent / "failed" / datetime.now().strftime("%Y-%m-%d")
        date_dir.mkdir(parents=True, exist_ok=True)
        
        # Move the failed file
        new_path = date_dir / file_path.name
        shutil.move(str(file_path), str(new_path))
        
        # Create error log
        error_log = new_path.with_suffix('.error.log')
        with open(error_log, 'w') as f:
            f.write(f"Error processing {file_path.name}:\n{error_message}")
            
        logger.info(f"Moved failed file {file_path} to {new_path}")
