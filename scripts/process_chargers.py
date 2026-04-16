import os
import requests
import xml.etree.ElementTree as ET
import polars as pl

def download_xml(url, output_path):
    """Downloads the XML file from the specified URL."""
    print(f"Downloading from {url}...")
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with requests.get(url, stream=True, timeout=120, headers=headers) as r:
        r.raise_for_status()
        with open(output_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
    print(f"Saved raw XML to {output_path}")

def parse_xml(xml_path):
    """Parses the DGT Datex2 XML file and extracts relevant columns using Polars."""
    if not os.path.exists(xml_path):
        raise FileNotFoundError(f"XML file not found: {xml_path}")
        
    print(f"Parsing {xml_path}...")
    tree = ET.parse(xml_path)
    root = tree.getroot()

    ns = {
        'com': 'http://datex2.eu/schema/3/common',
        'fac': 'http://datex2.eu/schema/3/facilities',
        'loc': 'http://datex2.eu/schema/3/locationReferencing',
        'egi': 'http://datex2.eu/schema/3/energyInfrastructure'
    }

    data = []
    # Find all sites in Spain
    sites = root.findall('.//egi:energyInfrastructureSite', ns)
    
    for site in sites:
        site_id = site.get('id')
        
        # Site Name (extracted from fac:name/com:values/com:value)
        site_name_elem = site.find('./fac:name/com:values/com:value', ns)
        site_name = site_name_elem.text if site_name_elem is not None else "Unknown"
        
        # Get coordinates for the site
        location = site.find('.//loc:coordinatesForDisplay', ns)
        lat = location.find('loc:latitude', ns).text if location is not None else None
        lon = location.find('loc:longitude', ns).text if location is not None else None
        
        # Find all connectors within this site
        connectors = site.findall('.//egi:connector', ns)
        
        for conn in connectors:
            # Helper function to extract text safely from a tag
            def get_val(parent, tag):
                child = parent.find(tag, ns)
                return child.text if child is not None else None

            data.append({
                'site_name': site_name,
                'site_id': site_id,
                'latitude': float(lat) if lat else None,
                'longitude': float(lon) if lon else None,
                'connector_type': get_val(conn, 'egi:connectorType'),
                'charging_mode': get_val(conn, 'egi:chargingMode'),
                'connector_format': get_val(conn, 'egi:connectorFormat'),
                'max_power': get_val(conn, 'egi:maxPowerAtSocket'),
                'voltage': get_val(conn, 'egi:voltage'),
                'max_current': get_val(conn, 'egi:maximumCurrent')
            })
            
    # Create Polars DataFrame
    df = pl.from_dicts(data)
    
    # Clean numeric columns
    numeric_cols = ['max_power', 'voltage', 'max_current']
    for col in numeric_cols:
        if col in df.columns:
            # Cast to float, setting invalid strings to null
            df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))
        
    return df

def main(
    raw_xml_path="data/raw/chargers/chargers.xml",
    parquet_output_path="data/processed/charging_points.parquet"
):
    # Ensure processed directory exists
    os.makedirs(os.path.dirname(parquet_output_path), exist_ok=True)
    
    try:
        # 1. Parse and generate DataFrame
        df = parse_xml(raw_xml_path)
        
        # 2. Save as Parquet
        df.write_parquet(parquet_output_path)
        
        print(f"\n--- Processing Success ---")
        print(f"Total sites: {len(df['site_id'].unique())}")
        print(f"Total connectors: {len(df)}")
        print(f"Processed file: {parquet_output_path}")
        
    except Exception as e:
        print(f"\nFailed to process chargers data: {str(e)}")
        raise e # Re-raise to let orchestrator handle it

if __name__ == "__main__":
    main()
