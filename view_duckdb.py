#!/usr/bin/env python3
"""
DuckDB Database Viewer
Usage: python view_duckdb.py [database_file.duckdb]
"""

import sys
import duckdb
from pathlib import Path


def view_duckdb(db_path: str):
    """View contents of a DuckDB database."""
    
    if not Path(db_path).exists():
        print(f"Error: Database file '{db_path}' not found.")
        return
    
    try:
        # Connect to the database in read-only mode
        conn = duckdb.connect(db_path, read_only=True)
        
        print(f"\n{'='*60}")
        print(f"DuckDB Database: {db_path}")
        print(f"{'='*60}\n")
        
        # Get all schemas (excluding system schemas)
        schemas = conn.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name NOT IN ('information_schema', 'pg_catalog')
            ORDER BY schema_name DESC
        """).fetchall()
        
        # Get all tables across all schemas
        all_tables = conn.execute("""
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            ORDER BY table_schema DESC, table_name
        """).fetchall()
        
        if not all_tables:
            print("No tables found in the database.")
            conn.close()
            return
        
        print(f"Found {len(schemas)} schema(s) and {len(all_tables)} table(s):\n")
        
        # Group tables by schema
        current_schema = None
        for schema_name, table_name in all_tables:
            if current_schema != schema_name:
                current_schema = schema_name
                print(f"\n{'='*60}")
                print(f"Schema: {schema_name}")
                print(f"{'='*60}")
            print(f"\n{'─'*60}")
            print(f"Table: {table_name}")
            print(f"{'─'*60}")
            
            
            # Use fully qualified table name (schema.table)
            qualified_name = f"{schema_name}.{table_name}"
            
            # Get table schema
            schema_info = conn.execute(f"DESCRIBE {qualified_name}").fetchall()
            print("\nSchema:")
            print(f"{'Column':<30} {'Type':<20} {'Null':<10}")
            print(f"{'-'*30} {'-'*20} {'-'*10}")
            for col_name, col_type, null_allowed, *_ in schema_info:
                print(f"{col_name:<30} {col_type:<20} {null_allowed:<10}")
            
            # Get row count
            count = conn.execute(f"SELECT COUNT(*) FROM {qualified_name}").fetchone()[0]
            print(f"\nTotal rows: {count}")
            
            # Show sample data (first 5 rows)
            if count > 0:
                print("\nSample data (first 5 rows):")
                result = conn.execute(f"SELECT * FROM {qualified_name} LIMIT 5").fetchdf()
                print(result.to_string())

            
            print()
        
        conn.close()
        
        print(f"\n{'='*60}")
        print("To query interactively, use:")
        print(f"  python -c \"import duckdb; conn = duckdb.connect('{db_path}'); ...\"")
        print(f"{'='*60}\n")
        
    except Exception as e:
        print(f"Error reading database: {e}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        # Default to github_pipeline.duckdb if no argument provided
        db_file = "github_pipeline.duckdb"
        if not Path(db_file).exists():
            print("Usage: python view_duckdb.py <database_file.duckdb>")
            sys.exit(1)
    else:
        db_file = sys.argv[1]
    
    view_duckdb(db_file)
