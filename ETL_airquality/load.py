# load.py
import os
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client
from time import sleep

# --- Configuration ---
load_dotenv()
BASE_DIR = Path(__file__).resolve().parents[0]
STAGED_DIR = BASE_DIR / "data" / "staged"

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("❌ Missing credentials. Please set SUPABASE_URL and SUPABASE_KEY in .env")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLE_NAME = "air_quality_readings"

# SQL Schema compatible with OpenAQ data
CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS public.{TABLE_NAME} (
    id BIGSERIAL PRIMARY KEY,
    city TEXT,
    location TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    parameter TEXT,
    value DOUBLE PRECISION,
    unit TEXT,
    last_updated TIMESTAMP WITH TIME ZONE,
    source_file TEXT,
    uploaded_at TIMESTAMP DEFAULT NOW()
);
"""

def create_table_if_not_exists():
    """
    Attempts to create the table using RPC, or prints SQL if RPC is restricted.
    """
    try:
        print("🔧 Checking Supabase table schema...")
        supabase.rpc("execute_sql", {"query": CREATE_TABLE_SQL}).execute()
        print("✅ Table check/creation successful.")
    except Exception as e:
        print(f"⚠️  RPC Error: {e}")
        print("ℹ️  If the table doesn't exist, run this SQL in your Supabase Dashboard:")
        print(CREATE_TABLE_SQL)

def load_staged_data(batch_size=100):
    # 1. Find latest staged file
    files = list(STAGED_DIR.glob("aqi_staged_*.csv"))
    if not files:
        print("❌ No staged data found. Run transform.py first.")
        return

    latest_file = max(files, key=os.path.getctime)
    print(f"📖 Loading data from: {latest_file.name}")

    # 2. Read CSV
    df = pd.read_csv(latest_file)
    
    # Handle NaN/NaT for JSON compatibility (Pandas NaN -> Python None)
    df = df.where(pd.notnull(df), None)

    # 3. Batch Insert
    records = df.to_dict(orient="records")
    total = len(records)
    print(f"📦 Uploading {total} rows to '{TABLE_NAME}'...")

    for i in range(0, total, batch_size):
        batch = records[i:i + batch_size]
        try:
            supabase.table(TABLE_NAME).insert(batch).execute()
            print(f"   ✅ Inserted rows {i+1} to {min(i+batch_size, total)}")
        except Exception as e:
            print(f"   ⚠️  Error on batch {i}: {e}")
            sleep(2) # Basic retry wait

    print("🎉 Load Complete!")

if __name__ == "__main__":
    create_table_if_not_exists()
    load_staged_data()