"""Pre-create a Delta table with the benchmark schema (ts TIMESTAMP).

Usage: python3 create_table.py <table_uri>

Environment variables:
  AZURE_STORAGE_ACCOUNT_NAME  – ADLS Gen2 storage account
  AZURE_STORAGE_ACCOUNT_KEY   – ADLS Gen2 storage key
"""

import os
import sys
import time

import pyarrow as pa
from deltalake import DeltaTable

table_uri = sys.argv[1]

storage_options = {
    "account_name": os.environ["AZURE_STORAGE_ACCOUNT_NAME"],
    "account_key": os.environ["AZURE_STORAGE_ACCOUNT_KEY"],
}

for attempt in range(10):
    try:
        dt = DeltaTable(table_uri, storage_options=storage_options)
        print(f"Delta table already exists at {table_uri} (version {dt.version()})")
        break
    except Exception:
        try:
            schema = pa.schema([("ts", pa.timestamp("us"))])
            dt = DeltaTable.create(
                table_uri=table_uri,
                schema=schema,
                storage_options=storage_options,
            )
            print(f"Created Delta table at {table_uri} (version {dt.version()})")
            break
        except Exception as e:
            print(f"Attempt {attempt + 1}: table create race ({e}), retrying...")
            time.sleep(1)
else:
    print("ERROR: Could not create or open Delta table after 10 attempts")
    sys.exit(1)
