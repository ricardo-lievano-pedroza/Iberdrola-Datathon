import polars as pl
import geopandas as gpd
import os
import time

def main(
    raw_path="data/raw/gas_stations.parquet",
    road_points_path="data/processed/road_backbone_points.parquet",
    backbone_roads_path="data/processed/backbone_roads.parquet",
    output_path="data/processed/road_backbone_gas_stations_proximity.parquet"
):
    """
    Calculates proximity of road backbone points to gas stations.
    
    Args:
        raw_path: Path to the input gas stations parquet.
        road_points_path: Path to the road backbone points parquet.
        backbone_roads_path: Path to the backbone road geometries (LineStrings) for filtering.
        output_path: Path to save the final results (backbone points + gas station info).
    """
    start_time = time.time()
    
    # 1. Load gas stations
    print(f"Loading gas stations from {raw_path}...")
    gdf_stations = gpd.read_parquet(raw_path)
    
    print(f"Loaded {len(gdf_stations)} gas stations. Projecting to metric CRS...")
    
    # Project to metric CRS (EPSG:3042)
    gdf_stations = gdf_stations.to_crs(epsg=3042)
    
    # 2. Filter gas stations to only those near backbone roads
    print(f"Loading backbone road geometries from {backbone_roads_path}...")
    gdf_backbone_roads = gpd.read_parquet(backbone_roads_path)
    if gdf_backbone_roads.crs is None:
        gdf_backbone_roads.set_crs(3042, inplace=True)
    
    print("Filtering gas stations to those within 1000m of a backbone road...")
    initial_stations = len(gdf_stations)
    gdf_stations = gpd.sjoin_nearest(
        gdf_stations,
        gdf_backbone_roads[['backbone_id', 'geometry']],
        how="inner",
        max_distance=1000,
        distance_col="dist_to_backbone_tmp"
    )
    gdf_stations = gdf_stations.drop_duplicates(subset=['id']).drop(columns=['dist_to_backbone_tmp', 'backbone_id', 'index_right'], errors='ignore')
    print(f"Kept {len(gdf_stations)} out of {initial_stations} gas stations near backbone roads.")
    
    # 3. Load Backbone Points
    print(f"Loading backbone points from {road_points_path}...")
    gdf_points = gpd.read_parquet(road_points_path)
    if gdf_points.crs is None:
        gdf_points.set_crs(3042, inplace=True)
    
    # 4. Join nearest gas station to each backbone point
    print("Calculating nearest gas station for each backbone point...")
    # We rename columns to avoid confusion after join
    gdf_stations_for_join = gdf_stations[['id', 'name', 'geometry']].rename(
        columns={'id': 'nearest_gas_station_id', 'name': 'nearest_gas_station_name'}
    )
    
    gdf_result = gpd.sjoin_nearest(
        gdf_points,
        gdf_stations_for_join,
        how="left",
        distance_col="distance_to_gas_station_m"
    )
    
    # Drop duplicates if a point is equidistant to multiple stations
    gdf_result = gdf_result.drop_duplicates(subset=['point_id'])
    if 'index_right' in gdf_result.columns:
        gdf_result = gdf_result.drop(columns=['index_right'])
    
    # 5. Save results
    print(f"Saving results to {output_path}...")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    gdf_result.to_parquet(output_path)
    
    end_time = time.time()
    print(f"Success! Final dataset has {len(gdf_result)} backbone points with gas station proximity.")
    print(f"Total time: {end_time - start_time:.2f}s.")

if __name__ == "__main__":
    main()
