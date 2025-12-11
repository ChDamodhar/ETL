# ===========================
# load.py
# ===========================
# Purpose: Load transformed Telco Churn dataset into Supabase

import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv
import time

# ------------------------------------------------------
# Supabase Client
# ------------------------------------------------------
def get_supabase_client():
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")

    if not url or not key:
        raise ValueError("❌ Missing SUPABASE_URL or SUPABASE_KEY")

    return create_client(url, key)


# ------------------------------------------------------
# Load to Supabase in Safe Batches
# ------------------------------------------------------
def load_to_supabase(staged_path: str, table_name="telco_churn"):
    supabase = get_supabase_client()

    # Absolute path handling
    if not os.path.isabs(staged_path):
        staged_path = os.path.abspath(os.path.join(os.path.dirname(__file__), staged_path))

    print(f"📁 Loading data from: {staged_path}")

    if not os.path.exists(staged_path):
        print("❌ CSV file not found! Run transform.py first.")
        return

    df = pd.read_csv(staged_path)

    # Convert NaN → None to avoid JSON errors
    df = df.where(pd.notnull(df), None)

    batch_size = 200
    total_rows = len(df)

    print(f"📊 Total rows: {total_rows}. Loading in batches of {batch_size}...\n")

    for start in range(0, total_rows, batch_size):
        end = min(start + batch_size, total_rows)
        batch = df.iloc[start:end].to_dict(orient="records")

        # Retry logic for failed batches
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = supabase.table(table_name).insert(batch).execute()

                if hasattr(response, "error") and response.error:
                    raise Exception(response.error)

                print(f"✅ Inserted rows {start+1} → {end}")
                break  # SUCCESS — exit retry loop

            except Exception as e:
                print(f"⚠️ Error inserting batch rows {start+1}-{end}: {e}")
                print(f"🔁 Retrying ({attempt+1}/{max_retries})...")
                time.sleep(1)

        else:
            print(f"❌ FAILED permanently for rows {start+1}-{end}. Skipping batch.\n")

    print("\n🎯 Done! Telco churn data loaded into Supabase.")


# ------------------------------------------------------
# Run as standalone script
# ------------------------------------------------------
if __name__ == "__main__":
    staged_csv_path = os.path.join("..", "data", "staged", "telco_Customer_transformed.csv")

    load_to_supabase(staged_csv_path)
