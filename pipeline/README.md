This is an initial project that downloads csv and parquet data and then insert them into postgreSQL.

Key points:
- uv used to isolate the environments with different packages - fast when adding python packages, handy when initiating project framework and recording versions in pyproject.toml
- Read CSV file in pandas and insert thru SQLalchemy engine in chunks
- Read Parquet file in polars and insert thru 'adbc' engine 10X fasters and stable