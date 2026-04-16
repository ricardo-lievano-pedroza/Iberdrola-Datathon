import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import os
import urllib3
import ssl

# Suppress insecure request warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import json

def process_gas_stations(raw_path="data/raw/gas_stations/gas_stations.json", output_path="data/processed/gas_stations.parquet"):
    """Reads the raw JSON gas station data and converts it to a GeoParquet file."""
    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"Raw gas stations file not found: {raw_path}")

    print(f"Reading raw data from {raw_path}...")
    with open(raw_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    if "ListaEESSPrecio" not in data:
        raise ValueError("Unexpected JSON structure: 'ListaEESSPrecio' not found.")
    
    # Create DataFrame
    df = pd.DataFrame(data["ListaEESSPrecio"])
    
    # Relevant columns to keep (filtering as requested by user)
    columns_to_keep = {
        "IDEESS": "id",
        "Rótulo": "name",
        "Dirección": "address",
        "Municipio": "municipality",
        "Provincia": "province",
        "Latitud": "latitude",
        "Longitud (WGS84)": "longitude"
    }
    
    # Filter and rename
    df = df[list(columns_to_keep.keys())].rename(columns=columns_to_keep)
    
    print("Cleaning coordinates...")
    # Clean coordinates: Replace Spanish decimal comma with period and convert to float
    for col in ["latitude", "longitude"]:
        df[col] = df[col].astype(str).str.replace(",", ".").astype(float)
    
    print("Converting to GeoPandas...")
    # Create Point geometry
    geometry = [Point(xy) for xy in zip(df["longitude"], df["latitude"])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
    
    # Ensure processed directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    print(f"Saving to {output_path}...")
    gdf.to_parquet(output_path)
    print(f"Success! Processed {len(gdf)} gas stations.")

def main(raw_path="data/raw/gas_stations/gas_stations.json", output_path="data/processed/gas_stations.parquet"):
    process_gas_stations(raw_path, output_path)

if __name__ == "__main__":
    main()
