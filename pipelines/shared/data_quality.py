# ABOUTME: Shared data quality monitoring and validation functions
# ABOUTME: Provides consistent data quality checks across all pipeline sources

import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from .database import get_db_connection, execute_query, get_table_row_count, table_exists

class DataQualityChecker:
    """Data quality monitoring and validation for pipeline sources"""
    
    def __init__(self, source_name: str, logger: Optional[logging.Logger] = None):
        self.source_name = source_name
        self.logger = logger or logging.getLogger(f"data_quality.{source_name}")
        self.quality_issues = []
    
    def check_row_counts(self, expected_tables: List[str], schema: str = "raw") -> Dict[str, int]:
        """
        Check row counts for expected tables
        
        Args:
            expected_tables: List of table names to check
            schema: Database schema name
            
        Returns:
            Dictionary of table names to row counts
        """
        self.logger.info(f"Checking row counts for {len(expected_tables)} tables in {schema} schema")
        
        row_counts = {}
        
        for table in expected_tables:
            full_table_name = f"{schema}.{table}"
            
            if not table_exists(schema, table):
                self.quality_issues.append(f"Table missing: {full_table_name}")
                row_counts[table] = 0
                continue
            
            try:
                count = get_table_row_count(schema, table)
                row_counts[table] = count
                
                if count == 0:
                    self.quality_issues.append(f"Empty table: {full_table_name}")
                    
            except Exception as e:
                self.quality_issues.append(f"Error checking {full_table_name}: {str(e)}")
                row_counts[table] = -1
        
        return row_counts
    
    def check_data_freshness(self, table: str, date_column: str, 
                           max_age_hours: int = 24, schema: str = "raw") -> bool:
        """
        Check if data in table is fresh (within max_age_hours)
        
        Args:
            table: Table name
            date_column: Column name containing timestamps
            max_age_hours: Maximum age in hours
            schema: Database schema name
            
        Returns:
            True if data is fresh, False otherwise
        """
        self.logger.info(f"Checking data freshness for {schema}.{table}")
        
        if not table_exists(schema, table):
            self.quality_issues.append(f"Cannot check freshness - table missing: {schema}.{table}")
            return False
        
        query = f"""
        SELECT MAX({date_column}) as latest_date
        FROM {schema}.{table}
        WHERE {date_column} IS NOT NULL
        """
        
        try:
            result = execute_query(query)
            if not result or not result[0]['latest_date']:
                self.quality_issues.append(f"No dates found in {schema}.{table}.{date_column}")
                return False
            
            latest_date = result[0]['latest_date']
            
            # Handle different date formats
            if isinstance(latest_date, str):
                try:
                    latest_date = datetime.fromisoformat(latest_date.replace('Z', '+00:00'))
                except ValueError:
                    self.quality_issues.append(f"Invalid date format in {schema}.{table}.{date_column}")
                    return False
            
            # Check freshness
            age = datetime.utcnow() - latest_date.replace(tzinfo=None)
            is_fresh = age.total_seconds() / 3600 <= max_age_hours
            
            if not is_fresh:
                self.quality_issues.append(
                    f"Stale data in {schema}.{table}: {age.days} days, {age.seconds//3600} hours old"
                )
            
            return is_fresh
            
        except Exception as e:
            self.quality_issues.append(f"Error checking freshness for {schema}.{table}: {str(e)}")
            return False
    
    def check_column_completeness(self, table: str, required_columns: List[str], 
                                schema: str = "raw", min_completeness: float = 0.95) -> Dict[str, float]:
        """
        Check completeness of required columns (percentage of non-null values)
        
        Args:
            table: Table name
            required_columns: List of column names to check
            schema: Database schema name
            min_completeness: Minimum acceptable completeness ratio (0.0 to 1.0)
            
        Returns:
            Dictionary of column names to completeness ratios
        """
        self.logger.info(f"Checking column completeness for {schema}.{table}")
        
        if not table_exists(schema, table):
            self.quality_issues.append(f"Cannot check completeness - table missing: {schema}.{table}")
            return {}
        
        completeness = {}
        
        for column in required_columns:
            query = f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT({column}) as non_null_rows
            FROM {schema}.{table}
            """
            
            try:
                result = execute_query(query)
                if result:
                    total = result[0]['total_rows']
                    non_null = result[0]['non_null_rows']
                    
                    if total > 0:
                        ratio = non_null / total
                        completeness[column] = ratio
                        
                        if ratio < min_completeness:
                            self.quality_issues.append(
                                f"Low completeness in {schema}.{table}.{column}: "
                                f"{ratio:.2%} (minimum: {min_completeness:.2%})"
                            )
                    else:
                        completeness[column] = 0.0
                        self.quality_issues.append(f"Empty table: {schema}.{table}")
                        
            except Exception as e:
                self.quality_issues.append(
                    f"Error checking completeness for {schema}.{table}.{column}: {str(e)}"
                )
                completeness[column] = 0.0
        
        return completeness
    
    def check_duplicate_keys(self, table: str, key_columns: List[str], 
                           schema: str = "raw") -> int:
        """
        Check for duplicate records based on key columns
        
        Args:
            table: Table name
            key_columns: List of columns that should be unique together
            schema: Database schema name
            
        Returns:
            Number of duplicate records found
        """
        self.logger.info(f"Checking for duplicates in {schema}.{table}")
        
        if not table_exists(schema, table):
            self.quality_issues.append(f"Cannot check duplicates - table missing: {schema}.{table}")
            return 0
        
        key_list = ", ".join(key_columns)
        query = f"""
        SELECT COUNT(*) as duplicate_count
        FROM (
            SELECT {key_list}, COUNT(*) as cnt
            FROM {schema}.{table}
            WHERE {" AND ".join([f"{col} IS NOT NULL" for col in key_columns])}
            GROUP BY {key_list}
            HAVING COUNT(*) > 1
        ) duplicates
        """
        
        try:
            result = execute_query(query)
            if result:
                duplicate_count = result[0]['duplicate_count']
                
                if duplicate_count > 0:
                    self.quality_issues.append(
                        f"Found {duplicate_count} duplicate key combinations in {schema}.{table} "
                        f"for columns: {key_list}"
                    )
                
                return duplicate_count
            
        except Exception as e:
            self.quality_issues.append(
                f"Error checking duplicates for {schema}.{table}: {str(e)}"
            )
        
        return 0
    
    def generate_quality_report(self) -> Dict[str, Any]:
        """
        Generate comprehensive data quality report
        
        Returns:
            Dictionary containing quality assessment results
        """
        report = {
            "source": self.source_name,
            "timestamp": datetime.utcnow().isoformat(),
            "issues_found": len(self.quality_issues),
            "issues": self.quality_issues,
            "status": "PASS" if len(self.quality_issues) == 0 else "FAIL"
        }
        
        return report
    
    def log_quality_summary(self):
        """Log summary of data quality checks"""
        if self.quality_issues:
            self.logger.warning(f"Data quality issues found: {len(self.quality_issues)}")
            for issue in self.quality_issues:
                self.logger.warning(f"  - {issue}")
        else:
            self.logger.info("All data quality checks passed")

def run_basic_quality_checks(source_name: str, tables: List[str], 
                           schema: str = "raw") -> Dict[str, Any]:
    """
    Run basic data quality checks for a source
    
    Args:
        source_name: Name of the data source
        tables: List of table names to check
        schema: Database schema name
        
    Returns:
        Quality report dictionary
    """
    checker = DataQualityChecker(source_name)
    
    # Check row counts
    row_counts = checker.check_row_counts(tables, schema)
    
    # Check for empty tables
    for table, count in row_counts.items():
        if count == 0:
            checker.quality_issues.append(f"Empty table: {schema}.{table}")
    
    # Generate report
    report = checker.generate_quality_report()
    report["row_counts"] = row_counts
    
    return report