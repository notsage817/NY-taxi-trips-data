"""@bruin

name: ingestion.trips
type: python

image: python:3.13

connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: VendorID
    type: String
    description: not null

@bruin"""

import json
import os
import io
import pandas as pd
import requests
from datetime import datetime, timezone
from typing import List, Dict, Tuple
from dateutil.relativedelta import relativedelta

BASEURL = "https://d37ci6vzp7h7m.cloudfront.net/trip-data"

def generate_months_toingest(start_date: str, end_date: str) -> List[Tuple[int, int]]:
    """
    Generate list of (year, month) tuples between start_date and end_date.
    
    Args:
        start_date: Date string in format 'YYYY-MM-DD'
        end_date: Date string in format 'YYYY-MM-DD'
    
    Returns:
        List of (year, month) tuples
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    months = []
    current = start
    while current <= end:
        months.append((current.year, current.month))
        current += relativedelta(months=1)
    
    return months

def build_parquet_url(taxi_type: str, year: int, month: int) -> str:
    """
    Build the full parquet file URL for a given taxi type, year, and month.
    
    Args:
        taxi_type: Type of taxi ('yellow', 'green', etc.)
        year: Year as integer
        month: Month as integer (1-12)
    
    Returns:
        Full URL to the parquet file
    """
    month_str = str(month).zfill(2)
    filename = f"{taxi_type}_tripdata_{year}-{month_str}.parquet"
    return f"{BASEURL}/{filename}"

def fetch_parquet(url: str) -> pd.DataFrame:
    response = requests.get(url)
    response.raise_for_status()
    
    return pd.read_parquet(io.BytesIO(response.content))

def materialize() -> pd.DataFrame:
    """
    TODO: Implement ingestion using Bruin runtime context.

    Required Bruin concepts to use here:
    - Built-in date window variables:
      - BRUIN_START_DATE / BRUIN_END_DATE (YYYY-MM-DD)
      - BRUIN_START_DATETIME / BRUIN_END_DATETIME (ISO datetime)
      Docs: https://getbruin.com/docs/bruin/assets/python#environment-variables
    - Pipeline variables:
      - Read JSON from BRUIN_VARS, e.g. `taxi_types`
      Docs: https://getbruin.com/docs/bruin/getting-started/pipeline-variables

    Design TODOs (keep logic minimal, focus on architecture):
    - generate list of months using generate_months_to_ingest() based on BRUIN_START_DATE/BRUIN_END_DATE.
    - For each month, build URLs for both taxi types using build_parquet_url().
    - Fetch data for combination of taxitype + year +month, parse into DataFrames, and concatenate.
    - Add a column like `extracted_at` for lineage/debugging (timestamp of extraction).
    - Prefer append-only in ingestion; handle duplicates in staging.
    """
    # Get date range from Bruin runtime context
    start_date = os.getenv("BRUIN_START_DATE", datetime.now().strftime("%Y-%m-01"))
    end_date = os.getenv("BRUIN_END_DATE", datetime.now().strftime("%Y-%m-%d"))
    
    # Get taxi types from pipeline variables
    bruin_vars = os.getenv("BRUIN_VARS", "{}")
    vars_dict = json.loads(bruin_vars)
    taxi_types = vars_dict.get("taxi_types", ["yellow", "green"])
    
    # Generate months to ingest
    months = generate_months_toingest(start_date, end_date)
    
    # Fetch and concatenate data
    dataframes = []
    extraction_time = datetime.now(timezone.utc).isoformat()
    
    for taxi_type in taxi_types:
        for year, month in months:
            try:
                url = build_parquet_url(taxi_type, year, month)
                df = fetch_parquet(url)
                df["extracted_at"] = extraction_time
                df['service_type'] = taxi_type
                dataframes.append(df)
            except Exception as e:
                print(f"Warning: Failed to fetch {taxi_type} data for {year}-{month}: {e}")
                continue
    
    if not dataframes:
        return pd.DataFrame()
    
    final_dataframe = pd.concat(dataframes, ignore_index=True)
    return final_dataframe


