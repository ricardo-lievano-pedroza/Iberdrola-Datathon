import polars as pl
import os
import glob
import re
import sys

def main(
    input_dir="data/raw/traffic/informacion_tramo",
    output_path="data/processed/road_routes_traffic.parquet"
):
    """
    Consolidates traffic data from multiple daily CSV files into a single Parquet file.
    Each input file expected columns: 'tramo', 'total', 'corto'.
    Output: 'tramo' column + daily metrics (total, corto, not_short, pct_not_short) + global max summaries.
    """
    print("\n=== Merging Traffic Data Datasets ===")
    
    # Find all CSV files in the input directory
    # Expected pattern: YYYYMMDD_info_tramo.csv
    csv_files = glob.glob(os.path.join(input_dir, "*_info_tramo.csv"))
    
    if not csv_files:
        print(f"Error: No CSV files found in {input_dir}")
        return

    print(f"Found {len(csv_files)} files to process.")
    
    # Sort files to ensure stable merging order
    csv_files.sort()
    
    dfs = []
    
    for file_path in csv_files:
        filename = os.path.basename(file_path)
        # Extract date string
        match = re.search(r"(\d{8})_info_tramo\.csv", filename)
        if not match:
            print(f"Skipping {filename}: Does not match YYYYMMDD_info_tramo.csv pattern.")
            continue
            
        date_str = match.group(1)
        total_col_name = f"total_{date_str}"
        
        print(f" - Reading {filename} (Date: {date_str})")
        
        try:
            # Using scan_csv for efficiency (LazyFrame)
            # tramo is cast to string to ensure consistent join keys
            df = pl.scan_csv(file_path, separator=';') \
                .select([
                    pl.col("tramo").cast(pl.Utf8),
                    pl.col("total").alias(total_col_name),
                    pl.col("corto").alias(f"corto_{date_str}")
                ]) \
                .with_columns([
                    (pl.col(total_col_name) - pl.col(f"corto_{date_str}")).alias(f"not_short_{date_str}")
                ]) \
                .with_columns([
                    (pl.col(f"not_short_{date_str}") / pl.col(total_col_name)).fill_nan(0).alias(f"pct_not_short_{date_str}")
                ])
            dfs.append(df)
        except Exception as e:
            print(f"   Error reading {filename}: {e}")

    if not dfs:
        print("Error: No valid dataframes to merge.")
        return

    print("Merging all datasets on 'tramo'...")
    
    # Initialize with the first dataframe
    merged = dfs[0]
    
    # Iteratively outer join the rest
    # outer_coalesce=True ensures that 'tramo' from both sides is combined into one column
    # instead of having tramo_left, tramo_right...
    for i in range(1, len(dfs)):
        merged = merged.join(dfs[i], on="tramo", how="full", coalesce=True)

    print("Executing merge and filling missing values with 0...")
    try:
        # Collect the lazy computation
        df_final = merged.collect()
        
        # Fill nulls with 0 for all traffic-related columns
        traffic_related_prefixes = ["total_", "corto_", "not_short_", "pct_not_short_"]
        traffic_related_cols = [c for c in df_final.columns if any(c.startswith(p) for p in traffic_related_prefixes)]
        
        df_final = df_final.with_columns([
            pl.col(c).fill_null(0) for c in traffic_related_cols
        ])
        
        # Calculate Global Summary Columns (Max Demand)
        # We use max_horizontal across all dates to account for seasonality
        total_cols = [c for c in df_final.columns if c.startswith("total_")]
        not_short_cols = [c for c in df_final.columns if c.startswith("not_short_")]
        
        df_final = df_final.with_columns([
            pl.max_horizontal(total_cols).alias("total_max"),
            pl.max_horizontal(not_short_cols).alias("not_short_max")
        ]).with_columns([
            (pl.col("not_short_max") / pl.col("total_max")).fill_nan(0).alias("pct_not_short_max")
        ])
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Save to Parquet
        df_final.write_parquet(output_path)
        
        print(f"SUCCESS: Merged data saved to {output_path}")
        print(f"Total unique segments: {len(df_final)}")
        print(f"Columns: {', '.join(df_final.columns)}")
        
    except Exception as e:
        print(f"Error during merge execution: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
