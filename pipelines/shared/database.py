# ABOUTME: Shared database connection utilities for all pipeline sources
# ABOUTME: Provides consistent database access patterns and connection management

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from typing import Optional

# Load environment once at module level
load_dotenv()

def get_database_url() -> str:
    """Get database URL from environment variables"""
    try:
        return os.environ["DATABASE_URL"]
    except KeyError:
        raise ValueError("DATABASE_URL environment variable is not set")

def get_db_connection(cursor_factory=RealDictCursor):
    """
    Get database connection with consistent configuration
    
    Args:
        cursor_factory: Cursor factory class (default: RealDictCursor for dict-like results)
    
    Returns:
        psycopg2 connection object
    """
    database_url = get_database_url()
    return psycopg2.connect(database_url, cursor_factory=cursor_factory)

def execute_query(query: str, params: Optional[tuple] = None, fetch: bool = True):
    """
    Execute a single query with connection management
    
    Args:
        query: SQL query string
        params: Query parameters tuple
        fetch: Whether to fetch results (False for INSERT/UPDATE/DELETE)
    
    Returns:
        Query results if fetch=True, None otherwise
    """
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query, params)
        
        if fetch:
            results = cursor.fetchall()
            conn.close()
            return results
        else:
            conn.commit()
            conn.close()
            return None
            
    except Exception as e:
        conn.rollback()
        conn.close()
        raise e

def get_table_row_count(schema: str, table: str) -> int:
    """Get row count for a specific table"""
    query = f"SELECT COUNT(*) as count FROM {schema}.{table}"
    result = execute_query(query)
    return result[0]['count'] if result else 0

def table_exists(schema: str, table: str) -> bool:
    """Check if a table exists in the database"""
    query = """
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = %s AND table_name = %s
    )
    """
    result = execute_query(query, (schema, table))
    return result[0]['exists'] if result else False