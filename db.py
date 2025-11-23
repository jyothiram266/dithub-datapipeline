import duckdb
conn = duckdb.connect('github_pipeline.duckdb')

# Query issues
df = conn.execute("""
    SELECT title, state, created_at 
    FROM github_data_20251122103306.issues 
    LIMIT 10
""").fetchdf()
print(df)

conn.close()