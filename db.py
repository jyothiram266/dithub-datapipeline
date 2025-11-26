import duckdb
conn = duckdb.connect('github_pipeline.duckdb')

# Query issues
df = conn.execute("""
SELECT * FROM _dlt_pipeline_state;
""").fetchdf()
print(df)

conn.close()