import polars as pl
import geopandas as gpd
import os
import time

def main(
    charging_points_path="data/processed/charging_points.parquet",
    road_points_path="data/processed/road_backbone_points.parquet",
    backbone_roads_path="data/processed/backbone_roads.parquet",
    output_path="data/processed/road_backbone_charging_proximity.parquet"
):
    """
    Calculates proximity of road backbone points to high-power EV charging sites.
    
    Args:
        charging_points_path: Path to the input charging points parquet.
        road_points_path: Path to the road backbone points parquet.
        backbone_roads_path: Path to the backbone road geometries (LineStrings) for filtering.
        output_path: Path to save the final results (backbone points + charging info).
    """
    start_time = time.time()
    
    # 1. Load and aggregate charging points
    print(f"Loading charging points from {charging_points_path}...")
    df_full = pl.read_parquet(charging_points_path)
    
    # Filter for high power (max_power >= 100kW)
    df_full = df_full.filter(pl.col('max_power') >= 100000)
    
    # Group by site_id to handle sites with multiple chargers
    df_sites = df_full.group_by("site_id").agg([
        pl.col("site_name").first(),
        pl.col("latitude").first(),
        pl.col("longitude").first(),
        pl.col("connector_type").unique(),
        pl.col("connector_type").count().alias('n_chargers'),
        pl.col("max_power").max().alias("max_power_at_site"),
        pl.col("charging_mode").unique()
    ]).filter(
        (pl.col("latitude").is_not_null()) & (pl.col("longitude").is_not_null())
    )
    
    print(f"Filtered to {df_sites.height} high-power sites. Converting to GeoPandas...")
    
    # Convert to GeoPandas
    df_sites_pd = df_sites.to_pandas()
    gdf_sites = gpd.GeoDataFrame(
        df_sites_pd,
        geometry=gpd.points_from_xy(df_sites_pd.longitude, df_sites_pd.latitude),
        crs="EPSG:4326"
    )
    
    # Project to metric CRS (EPSG:3042)
    gdf_sites = gdf_sites.to_crs(epsg=3042)
    
    # 2. Filter sites to only those near backbone roads
    print(f"Loading backbone road geometries from {backbone_roads_path}...")
    gdf_backbone_roads = gpd.read_parquet(backbone_roads_path)
    if gdf_backbone_roads.crs is None:
        gdf_backbone_roads.set_crs(3042, inplace=True)
    
    print("Filtering sites to those within 1000m of a backbone road...")
    initial_sites = len(gdf_sites)
    gdf_sites = gpd.sjoin_nearest(
        gdf_sites,
        gdf_backbone_roads[['backbone_id', 'geometry']],
        how="inner",
        max_distance=1000,
        distance_col="dist_to_backbone_tmp"
    )
    gdf_sites = gdf_sites.drop_duplicates(subset=['site_id']).drop(columns=['dist_to_backbone_tmp', 'backbone_id', 'index_right'], errors='ignore')
    print(f"Kept {len(gdf_sites)} out of {initial_sites} sites near backbone roads.")
    
    # 3. Load Backbone Points
    print(f"Loading backbone points from {road_points_path}...")
    gdf_points = gpd.read_parquet(road_points_path)
    if gdf_points.crs is None:
        gdf_points.set_crs(3042, inplace=True)
    
    # 4. Join nearest charging station to each backbone point
    print("Calculating nearest charging station for each backbone point...")
    # We rename columns to avoid confusion after join
    gdf_sites_for_join = gdf_sites[['site_id', 'site_name', 'geometry']].rename(
        columns={'site_id': 'nearest_charging_site_id', 'site_name': 'nearest_charging_site_name'}
    )
    
    gdf_result = gpd.sjoin_nearest(
        gdf_points,
        gdf_sites_for_join,
        how="left",
        distance_col="distance_to_charging_station_m"
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
    print(f"Success! Final dataset has {len(gdf_result)} backbone points with charging proximity.")
    print(f"Total time: {end_time - start_time:.2f}s.")

if __name__ == "__main__":
    main()
