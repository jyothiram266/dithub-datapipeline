import duckdb
import os

db_path = os.path.abspath("github_pipeline.duckdb")

con = duckdb.connect(db_path)

tables = con.execute("SHOW TABLES").fetchall()
print(tables)