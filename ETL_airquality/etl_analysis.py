# etl_analysis.py
import os
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client

# --- Configuration ---
load_dotenv()
BASE_DIR = Path(__file__).resolve().parents[0]
PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
TABLE_NAME = "air_quality_readings"

def fetch_data():
    """Fetches all rows from Supabase."""
    print("🔍 Fetching data from Supabase for analysis...")
    
    # Supabase limits rows by default, picking 1000 for this demo
    res = supabase.table(TABLE_NAME).select("*").limit(2000).execute()
    
    data = res.data
    if not data:
        print("❌ No data found in database.")
        return pd.DataFrame()
        
    return pd.DataFrame(data)

def analyze_and_plot(df):
    if df.empty:
        return

    print("ℹ️  Data Loaded. Rows:", len(df))
    
    # Convert types
    df['value'] = pd.to_numeric(df['value'])
    df['last_updated'] = pd.to_datetime(df['last_updated'])

    # --- Insight 1: Pollutant Summary ---
    # Group by Pollutant Type (pm25, pm10, no2, etc.) to see average levels
    pollutant_summary = df.groupby('parameter')['value'].mean().reset_index()
    print("\n📊 Average Values by Pollutant:")
    print(pollutant_summary)

    # --- Insight 2: City Comparison (Focus on PM 2.5) ---
    # PM 2.5 is the most common health metric. 
    pm25_df = df[df['parameter'] == 'pm25']
    
    if not pm25_df.empty:
        city_ranking = pm25_df.groupby('city')['value'].mean().sort_values(ascending=False)
        
        print("\n🏙️  Most Polluted Cities (Avg PM 2.5):")
        print(city_ranking)
        
        # Save Ranking
        csv_path = PROCESSED_DIR / "city_aqi_ranking.csv"
        city_ranking.to_csv(csv_path)
        print(f"✅ Saved city ranking to {csv_path}")

        # --- Plotting ---
        plt.figure(figsize=(10, 6))
        colors = ['#e74c3c' if x > 60 else '#f1c40f' if x > 30 else '#2ecc71' for x in city_ranking.values]
        
        city_ranking.plot(kind='bar', color=colors, edgecolor='black')
        
        plt.title('Average PM 2.5 Levels by City (Higher is Worse)')
        plt.xlabel('City')
        plt.ylabel('PM 2.5 (µg/m³)')
        plt.axhline(y=60, color='red', linestyle='--', label='Unhealthy Threshold (60)')
        plt.legend()
        plt.tight_layout()
        
        plot_path = PROCESSED_DIR / "city_pollution_chart.png"
        plt.savefig(plot_path)
        print(f"✅ Saved chart to {plot_path}")
    else:
        print("⚠️  No PM 2.5 data available for plotting.")

    # --- Insight 3: Location Breakdown ---
    # Find the single most polluted location (sensor) in the dataset
    worst_location = df.loc[df['value'].idxmax()]
    print("\n⚠️  Highest Single Reading Recorded:")
    print(f"   City: {worst_location['city']}")
    print(f"   Location: {worst_location['location']}")
    print(f"   Pollutant: {worst_location['parameter']}")
    print(f"   Value: {worst_location['value']} {worst_location['unit']}")

if __name__ == "__main__":
    df = fetch_data()
    analyze_and_plot(df)