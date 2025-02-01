import pandas as pd
from pathlib import Path
from typing import Generator
import logging
from .processors.base import BaseProcessor
from .db.session import SessionManager

logger = logging.getLogger(__name__)

class CSVImporter:
    def __init__(self, config):
        self.config = config
        self.session_manager = SessionManager(config.database_url)
        
    def process_directory(self, directory: Path) -> None:
        """Process all CSV files in directory."""
        for csv_file in directory.glob('*.csv'):
            try:
                self.process_file(csv_file)
            except Exception as e:
                logger.error(f"Error processing {csv_file}: {e}")
                
    def process_file(self, file_path: Path) -> None:
        """Process a single CSV file."""
        logger.info(f"Processing {file_path}")
        
        # Process in chunks to handle large files
        for chunk in self._read_chunks(file_path):
            processed_data = self._process_chunk(chunk)
            self._save_to_database(processed_data)
            
    def _read_chunks(self, file_path: Path) -> Generator[pd.DataFrame, None, None]:
        """Read CSV file in chunks."""
        chunk_size = self.config.chunk_size
        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            yield chunk
            
    def _process_chunk(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """Process a chunk of data."""
        processor = BaseProcessor.get_processor(self.config.processor_type)
        return processor.process(chunk)
        
    def _save_to_database(self, data: pd.DataFrame) -> None:
        """Save processed data to database."""
        with self.session_manager.session() as session:
            try:
                # Convert DataFrame to database models and save
                models = self._convert_to_models(data)
                session.add_all(models)
                session.commit()
            except Exception as e:
                logger.error(f"Database error: {e}")
                session.rollback()
                raise
