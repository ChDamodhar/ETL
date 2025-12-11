# transform.py
import json
import pandas as pd
from pathlib import Path
import os
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parents[0]
RAW_DIR = BASE_DIR / "data" / "raw"
STAGED_DIR = BASE_DIR / "data" / "staged"
STAGED_DIR.mkdir(parents=True, exist_ok=True)

def get_latest_files():
    """Get the latest file for each city."""
    latest_files = {}
    for file_path in RAW_DIR.glob("*_raw_*.json"):
        # Extract city name (assuming filename is City_raw_Timestamp.json)
        city = file_path.name.split("_raw_")[0]
        
        # Keep only the newest file for each city
        if city not in latest_files or os.path.getctime(file_path) > os.path.getctime(latest_files[city]):
            latest_files[city] = file_path
    return latest_files.values()

def parse_open_meteo_json(file_path):
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
    except Exception as e:
        print(f"⚠️  Error reading {file_path}: {e}")
        return []

    # Open-Meteo Structure: { "city_name": "Delhi", "current": { "pm2_5": 45.0, "time": "..." } }
    city = data.get("city_name", "Unknown")
    current = data.get("current", {})
    timestamp = current.get("time")
    
    records = []
    
    # Map API keys to readable parameter names
    # Key = API key, Value = Unit (Open-Meteo units are usually µg/m³)
    pollutants = {
        "pm2_5": "µg/m³",
        "pm10": "µg/m³",
        "nitrogen_dioxide": "µg/m³",
        "ozone": "µg/m³",
        "sulphur_dioxide": "µg/m³"
    }

    for key, unit in pollutants.items():
        if key in current and current[key] is not None:
            records.append({
                "city": city,
                "location": f"{city} Center", # Generic location for city-level data
                "latitude": data.get("latitude"),
                "longitude": data.get("longitude"),
                "parameter": key.replace("_", ""), # clean name (pm2_5 -> pm25)
                "value": current[key],
                "unit": unit,
                "last_updated": timestamp,
                "source_file": file_path.name
            })
            
    return records

def run_transformation():
    print("🔄 Starting Transformation (Open-Meteo Format)...")
    all_records = []
    
    files = get_latest_files()
    for file_path in files:
        print(f"   📖 Processing {file_path.name}...")
        records = parse_open_meteo_json(file_path)
        all_records.extend(records)

    if not all_records:
        print("❌ No records found.")
        return

    df = pd.DataFrame(all_records)
    
    # Save
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = STAGED_DIR / f"aqi_staged_{timestamp}.csv"
    df.to_csv(output_path, index=False)
    
    print(f"✅ Transformation Complete. Saved {len(df)} rows to:")
    print(f"   {output_path}")

if __name__ == "__main__":
    run_transformation()